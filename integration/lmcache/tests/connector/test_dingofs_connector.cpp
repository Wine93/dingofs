// SPDX-License-Identifier: Apache-2.0
//
// C++ tests for DingoFSConnector (ConnectorBase implementation).
// Tests exercise the public submit_batch_* / drain_completions interface.

#include "dingofs_connector.h"

#include <sys/select.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using dingofs::connector::DingoFSConnector;
using lmcache::connector::Completion;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string MakeTempDir() {
  char tmpl[] = "/tmp/dingofs_conn_test_XXXXXX";
  char* dir = mkdtemp(tmpl);
  assert(dir != nullptr);
  return std::string(dir);
}

static void RemoveDir(const std::string& path) {
  std::filesystem::remove_all(path);
}

// RAII guard for temp directory + connector
struct TestGuard {
  std::string dir;
  std::shared_ptr<DingoFSConnector> conn;

  explicit TestGuard() : dir(MakeTempDir()) {
    conn = std::make_shared<DingoFSConnector>(dir);
  }

  ~TestGuard() {
    conn->close();
    conn.reset();
    RemoveDir(dir);
  }

  TestGuard(const TestGuard&) = delete;
  TestGuard& operator=(const TestGuard&) = delete;
};

// Drain completions with eventfd-based wait
static std::vector<Completion> DrainWithWait(
    DingoFSConnector* conn, int timeout_ms = 5000) {
  int efd = conn->event_fd();
  assert(efd >= 0);

  std::vector<Completion> all;
  fd_set fds;
  struct timeval tv;
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;

  while (true) {
    FD_ZERO(&fds);
    FD_SET(efd, &fds);

    int ret = select(efd + 1, &fds, nullptr, nullptr, &tv);
    if (ret < 0) {
      perror("select() failed");
      assert(false);
    }
    if (ret == 0) break;  // timeout

    auto completions = conn->drain_completions();
    for (auto& c : completions) {
      all.push_back(std::move(c));
    }
    if (!all.empty()) break;
  }

  return all;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

static void test_create_destroy() {
  printf("  test_create_destroy...");
  std::string dir = MakeTempDir();

  {
    auto conn = std::make_shared<DingoFSConnector>(dir);
    assert(conn->event_fd() >= 0);
    conn->close();
  }

  RemoveDir(dir);
  printf(" OK\n");
}

static void test_single_set_get() {
  printf("  test_single_set_get...");
  TestGuard g;

  const size_t data_size = 4096;
  std::vector<uint8_t> write_buf(data_size);
  for (size_t i = 0; i < data_size; ++i) {
    write_buf[i] = static_cast<uint8_t>(i & 0xFF);
  }

  // SET
  std::vector<std::string> keys = {"test_key_1"};
  std::vector<void*> wbufs = {write_buf.data()};
  std::vector<size_t> lens = {data_size};

  uint64_t fid = g.conn->submit_batch_set(keys, wbufs, lens, data_size);
  assert(fid != 0);

  auto completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].future_id == fid);
  assert(completions[0].ok);

  // GET
  std::vector<uint8_t> read_buf(data_size, 0);
  std::vector<void*> rbufs = {read_buf.data()};

  fid = g.conn->submit_batch_get(keys, rbufs, lens, data_size);
  assert(fid != 0);

  completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);

  assert(memcmp(write_buf.data(), read_buf.data(), data_size) == 0);
  printf(" OK\n");
}

static void test_exists() {
  printf("  test_exists...");
  TestGuard g;

  const size_t data_size = 1024;
  std::vector<uint8_t> buf(data_size, 42);

  // Write a key
  std::vector<std::string> keys = {"exists_key"};
  std::vector<void*> wbufs = {buf.data()};
  std::vector<size_t> lens = {data_size};

  uint64_t fid = g.conn->submit_batch_set(keys, wbufs, lens, data_size);
  auto completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);

  // Check EXISTS for written key
  fid = g.conn->submit_batch_exists(keys);
  completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);
  assert(completions[0].result_bytes.size() == 1);
  assert(completions[0].result_bytes[0] == 1);

  // Check EXISTS for non-existing key
  std::vector<std::string> missing = {"no_such_key"};
  fid = g.conn->submit_batch_exists(missing);
  completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);
  assert(completions[0].result_bytes.size() == 1);
  assert(completions[0].result_bytes[0] == 0);

  printf(" OK\n");
}

static void test_large_data() {
  printf("  test_large_data...");
  TestGuard g;

  // 1 MB chunk (triggers sharding: 1 MB / 4 MB max = 1 shard)
  const size_t data_size = 1024 * 1024;
  std::vector<uint8_t> write_buf(data_size);
  for (size_t i = 0; i < data_size; ++i) {
    write_buf[i] = static_cast<uint8_t>((i * 17) & 0xFF);
  }

  std::vector<std::string> keys = {"large_chunk"};
  std::vector<void*> wbufs = {write_buf.data()};
  std::vector<size_t> lens = {data_size};

  // SET
  uint64_t fid = g.conn->submit_batch_set(keys, wbufs, lens, data_size);
  auto completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);

  // GET
  std::vector<uint8_t> read_buf(data_size, 0);
  std::vector<void*> rbufs = {read_buf.data()};
  fid = g.conn->submit_batch_get(keys, rbufs, lens, data_size);
  completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);

  assert(memcmp(write_buf.data(), read_buf.data(), data_size) == 0);
  printf(" OK\n");
}

static void test_overwrite() {
  printf("  test_overwrite...");
  TestGuard g;

  const size_t data_size = 2048;
  std::vector<std::string> keys = {"ow_key"};
  std::vector<size_t> lens = {data_size};

  // First write: all 0xAA
  std::vector<uint8_t> buf_aa(data_size, 0xAA);
  std::vector<void*> wbufs_aa = {buf_aa.data()};
  g.conn->submit_batch_set(keys, wbufs_aa, lens, data_size);
  auto completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);

  // Second write: all 0xBB
  std::vector<uint8_t> buf_bb(data_size, 0xBB);
  std::vector<void*> wbufs_bb = {buf_bb.data()};
  g.conn->submit_batch_set(keys, wbufs_bb, lens, data_size);
  completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);

  // Read back: should get 0xBB
  std::vector<uint8_t> read_buf(data_size, 0);
  std::vector<void*> rbufs = {read_buf.data()};
  g.conn->submit_batch_get(keys, rbufs, lens, data_size);
  completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);
  assert(memcmp(buf_bb.data(), read_buf.data(), data_size) == 0);

  printf(" OK\n");
}

static void test_get_nonexistent() {
  printf("  test_get_nonexistent...");
  TestGuard g;

  const size_t data_size = 1024;
  std::vector<uint8_t> buf(data_size, 0);

  std::vector<std::string> keys = {"nonexistent_key"};
  std::vector<void*> rbufs = {buf.data()};
  std::vector<size_t> lens = {data_size};

  uint64_t fid = g.conn->submit_batch_get(keys, rbufs, lens, data_size);
  assert(fid != 0);

  auto completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(!completions[0].ok);  // Should fail
  assert(!completions[0].error.empty());

  printf(" OK\n");
}

static void test_exists_cache_hit() {
  printf("  test_exists_cache_hit...");
  TestGuard g;

  const size_t data_size = 512;
  std::vector<uint8_t> buf(data_size, 0x55);

  std::vector<std::string> keys = {"cache_hit_key"};
  std::vector<void*> wbufs = {buf.data()};
  std::vector<size_t> lens = {data_size};

  // Write key
  g.conn->submit_batch_set(keys, wbufs, lens, data_size);
  auto completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);

  // First exists: populates ExistsCache via set
  g.conn->submit_batch_exists(keys);
  completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);
  assert(completions[0].result_bytes[0] == 1);

  // Second exists: should hit ExistsCache fast path
  g.conn->submit_batch_exists(keys);
  completions = DrainWithWait(g.conn.get());
  assert(completions.size() == 1);
  assert(completions[0].ok);
  assert(completions[0].result_bytes[0] == 1);

  printf(" OK\n");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main() {
  printf("Running DingoFS Connector C++ tests:\n");

  test_create_destroy();
  test_single_set_get();
  test_exists();
  test_large_data();
  test_overwrite();
  test_get_nonexistent();
  test_exists_cache_hit();

  printf("\nAll tests passed!\n");
  return 0;
}
