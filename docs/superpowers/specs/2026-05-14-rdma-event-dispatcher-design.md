# RDMA EventDispatcher — Design

**Date:** 2026-05-14
**Author:** Wine93
**Scope:** `src/cache/common/infiniband/event.{h,cc}`

## Goal

Replace the current half-ported nginx-style `EventDispatcher` with a working,
brpc-style multi-dispatcher implementation that lets RDMA completion-queue
events be processed concurrently across multiple OS threads.

## Motivation

The current code in `src/cache/common/infiniband/event.{h,cpp}` has three
problems:

1. **Doesn't compile.** `ProcessEvents()` references `c->rev`, `c->wev`,
   `revents`, which don't exist on the `Event` struct — leftovers from
   porting nginx's per-connection read/write event split.
2. **Doesn't run.** `Start()`, `Shutdown()`, `EventWorker()` are empty stubs;
   `epoll_fd_` is never initialized and no worker thread is ever spawned.
3. **Single-threaded.** Even once fixed, one epoll thread serializes all
   completion-queue events across every RDMA connection in the process.

We want concurrent event processing modeled on brpc's `EventDispatcher` pool.

## Design

### Architecture

A pool of `EventDispatcher` instances, each owning one `epoll_fd` and one
dedicated OS thread that loops on `epoll_wait` and invokes handlers **inline**.
A thin pool facade hashes the fd to pick a dispatcher, so call sites stay
unchanged.

```
                     +--- EventDispatcherPool (singleton) ---+
AddEvent(fd, type)  ►| pick = fmix64(fd) % N                 |
DelEvent(fd)        ►|   │                                   |
                     |   ├─► EventDispatcher[0]  thread+epfd |
                     |   ├─► EventDispatcher[1]  thread+epfd |
                     |   └─► ...                             |
                     +---------------------------------------+
```

### Types

```cpp
struct Event {
  int fd;
  void* data;
  EventHandler handler;   // std::function<void(Event*)>
};

enum class EventType : uint8_t { kReadEvent = 0, kWriteEvent = 1 };
```

`Event` is unchanged from the current header (minus the nginx `rev`/`wev`
direction split, which was never present in the struct itself).

### EventDispatcher (per-thread)

```cpp
class EventDispatcher {
 public:
  Status Start();
  Status Shutdown();
  Status AddEvent(const Event& event, EventType type);
  Status DelEvent(int fd);

 private:
  void EventWorker();           // thread body

  int epoll_fd_{-1};
  int wakeup_fd_{-1};           // eventfd for clean shutdown
  std::atomic<bool> running_{false};
  std::thread worker_thread_;

  static constexpr int kMaxEvents = 1024;
};
```

#### Lifecycle

- `Start()`
  - `epoll_fd_ = epoll_create1(EPOLL_CLOEXEC)`
  - `wakeup_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)` and register it with
    `EPOLLIN` so a write to it wakes `epoll_wait`
  - `running_ = true`; spawn `worker_thread_(&EventDispatcher::EventWorker, this)`
- `Shutdown()`
  - `running_ = false`
  - `write(wakeup_fd_, &one, 8)` to unblock the worker
  - `worker_thread_.join()`
  - `close(wakeup_fd_)`, `close(epoll_fd_)`

#### Worker body

The wakeup eventfd is registered with `ee.data.ptr = nullptr` so it can be
distinguished from real `Event*` entries (the `epoll_data` is a union — we
can't reliably mix `.fd` and `.ptr` readers).

```cpp
void EventDispatcher::EventWorker() {
  epoll_event events[kMaxEvents];
  while (running_.load(std::memory_order_acquire)) {
    int n = epoll_wait(epoll_fd_, events, kMaxEvents, /*timeout=*/-1);
    if (n < 0) {
      if (errno == EINTR) continue;
      PLOG(ERROR) << "epoll_wait failed";
      break;
    }
    for (int i = 0; i < n; ++i) {
      auto* ev = static_cast<Event*>(events[i].data.ptr);
      if (ev == nullptr) {                 // wakeup_fd_ — drain & continue
        uint64_t v;
        (void)read(wakeup_fd_, &v, sizeof(v));
        continue;
      }
      if (ev->handler) ev->handler(ev);    // inline, must not block
    }
  }
}
```

#### Handler contract

Handlers run **inline** on the dispatcher thread. They MUST be non-blocking:
the expected pattern is a short `ibv_poll_cq` drain followed by either direct
fast handling or offload to a bthread queue (matching the existing comments in
`client.cc` / `server.cpp`). A blocking handler stalls every other fd sharded
to the same dispatcher.

#### AddEvent / DelEvent

```cpp
Status AddEvent(const Event& event, EventType type) {
  epoll_event ee{};
  ee.data.ptr = /* owned Event*, see ownership note below */;
  ee.events = EPOLLET | (type == EventType::kReadEvent ? EPOLLIN : EPOLLOUT);
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event.fd, &ee) != 0) {
    PLOG(ERROR) << "epoll_ctl ADD failed";
    return Status::Internal("add event failed");
  }
  return Status::OK();
}

Status DelEvent(int fd) {
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) != 0) {
    PLOG(ERROR) << "epoll_ctl DEL failed";
    return Status::Internal("del event failed");
  }
  return Status::OK();
}
```

Both are thread-safe because `epoll_ctl` is. `EPOLLET` is preserved to match
the rearm-and-redrain CQ pattern already implemented in
`Connection::PollCompletionQueue` (`connection.cc:130`).

### EventDispatcherPool (facade)

```cpp
class EventDispatcherPool {
 public:
  static EventDispatcherPool& GetInstance();

  Status Start();      // starts all N dispatchers
  Status Shutdown();   // shuts down all N dispatchers

  Status AddEvent(const Event& event, EventType type);  // picks by fd hash
  Status DelEvent(int fd);                              // picks by fd hash

 private:
  EventDispatcher& Pick(int fd);  // returns dispatchers_[fmix64(fd) % N]
  std::vector<std::unique_ptr<EventDispatcher>> dispatchers_;
};
```

Existing call sites are migrated to `EventDispatcherPool::GetInstance().AddEvent(...)`.
Two mechanical renames: `client.cc:294`, `server.cpp:136`. `EventDispatcher`
itself does **not** expose `GetInstance()` — it's only constructed by the
pool.

### Configuration

```cpp
DEFINE_int32(rdma_event_dispatcher_num, 1,
             "Number of RDMA event dispatcher threads");
```

Replaces the existing `rdma_event_workers` flag. Default 1 (matches user
request). Pool size is fixed at `Start()` time.

### Ownership

The `Event` object passed to `AddEvent` is stored in `epoll_event::data.ptr`,
so it must outlive its registration. The caller (typically `Connection`) owns
the `Event` and is responsible for calling `DelEvent(fd)` **before** the
`Event` is destroyed. This is enforced by convention (matches brpc's
SocketId-based ownership); no internal map is needed.

`AddEvent` takes `const Event&` today, but the address of a stack temporary
won't survive — call sites must pass a member-stored `Event` (typical pattern:
`Event` lives on the `Connection` / session object).

## Bugs fixed

| # | Current bug | Fix |
|---|-------------|-----|
| 1 | `c->rev` / `c->wev` / `revents` don't exist | Removed; handler is invoked once per epoll event |
| 2 | `epoll_fd_` never initialized | Created in `Start()` |
| 3 | Worker thread never spawned | Spawned in `Start()` |
| 4 | `static epoll_event event_list_[kMaxEvents]` declared but never defined | Moved to a local in `EventWorker()` |
| 5 | `Instance()` vs `GetInstance()` API mismatch | Pool exposes `GetInstance()` to match call sites |
| 6 | Filename `event.cpp` inconsistent with repo (`event.cc`) | Renamed |
| 7 | No `DelEvent` — fds leaked in epoll on connection close | Added |
| 8 | `Shutdown` is a no-op | Implemented with eventfd wakeup |

## Out of scope

- `ModEvent` — not needed for RDMA CQ fds (read-only, never toggle direction)
- bthread fan-out per event — handlers are contracted to drain CQ quickly and
  offload to bthreads themselves, matching the existing
  `client.cc` / `server.cpp` comments
- Least-loaded or round-robin sharding — fd-hash chosen
- Hot-resize of the dispatcher pool — pool size fixed at `Start()`

## Testing

- Unit tests under `test/unit/cache/common/infiniband/event_test.cc`
  (follows `test/unit/cache/README.md` conventions; check that file before
  writing tests):
  - Start/Shutdown is clean (no thread leaks)
  - AddEvent + writing to a pipe wakes the handler
  - DelEvent unregisters cleanly (handler does not fire after Del)
  - With N=4 dispatchers, fds with different hashes land on different
    dispatchers (verified via per-handler thread-id capture)
  - Shutdown returns promptly even with no pending events (eventfd works)

## Risks

- **Blocking handler ⇒ HoL blocking.** A handler that blocks stalls all other
  fds on the same dispatcher. Mitigated by the documented contract; not
  detected automatically.
- **Use-after-free if DelEvent is skipped.** If a `Connection` is destroyed
  without `DelEvent`, the dispatcher will deref a dangling pointer on the next
  CQ event. Mitigated by convention (`Connection` dtor calls `DelEvent`).
