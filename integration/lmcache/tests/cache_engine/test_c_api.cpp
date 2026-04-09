// SPDX-License-Identifier: Apache-2.0
//
// C/C++ tests for the DingoFS CacheEngine C API.
// Uses simple assert-based testing (no external framework dependency).

#include "cache_engine_capi.h"

#include <sys/eventfd.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

// Helper: create a unique temp directory
static std::string make_temp_dir() {
  char tmpl[] = "/tmp/dingofs_test_XXXXXX";
  char* dir = mkdtemp(tmpl);
  assert(dir != nullptr);
  return std::string(dir);
}

// Helper: recursively remove a directory
static void remove_dir(const std::string& path) {
  std::filesystem::remove_all(path);
}

// Helper: build a JSON config string for cache_engine_create
static std::string make_config(const std::string& cache_dir) {
  return "{\"cache_dir\": \"" + cache_dir + "\"}";
}

// RAII guard for temp directory + engine cleanup
struct TestGuard {
  std::string dir;
  cache_engine_t* conn;

  explicit TestGuard() : dir(make_temp_dir()) {
    std::string cfg = make_config(dir);
    conn = cache_engine_create(cfg.c_str());
    assert(conn != nullptr);
  }

  ~TestGuard() {
    if (conn) {
      cache_engine_destroy(conn);
    }
    remove_dir(dir);
  }

  TestGuard(const TestGuard&) = delete;
  TestGuard& operator=(const TestGuard&) = delete;
};

// Helper: drain completions with retry (wait for eventfd)
static std::vector<cache_completion_t> drain_with_wait(
    cache_engine_t* conn, int timeout_ms = 5000) {
  int efd = cache_engine_event_fd(conn);
  assert(efd >= 0);

  std::vector<cache_completion_t> all_completions;
  cache_completion_t buf[64];

  // Poll eventfd with timeout
  fd_set fds;
  struct timeval tv;
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;

  while (true) {
    FD_ZERO(&fds);
    FD_SET(efd, &fds);

    int ret = select(efd + 1, &fds, nullptr, nullptr, &tv);
    if (ret < 0) {
      perror("select() failed in drain_with_wait");
      assert(false && "select() error");
    }
    if (ret == 0) break;  // timeout

    int n = cache_engine_drain_completions(conn, buf, 64);
    for (int i = 0; i < n; ++i) {
      all_completions.push_back(buf[i]);
    }

    if (n > 0) break;  // Got completions
  }

  return all_completions;
}

// ========================================================================
// Test: create and destroy
// ========================================================================
static void test_create_destroy() {
  printf("  test_create_destroy...");
  std::string dir = make_temp_dir();

  std::string cfg = make_config(dir);
  cache_engine_t* conn = cache_engine_create(cfg.c_str());
  assert(conn != nullptr);

  int efd = cache_engine_event_fd(conn);
  assert(efd >= 0);

  cache_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ========================================================================
// Test: invalid config returns NULL
// ========================================================================
static void test_invalid_config() {
  printf("  test_invalid_config...");

  // Empty string is not valid JSON
  cache_engine_t* conn = cache_engine_create("");
  assert(conn == nullptr);
  const char* err = cache_engine_last_error();
  assert(err != nullptr);
  assert(strlen(err) > 0);

  printf(" OK\n");
}

// ========================================================================
// Test: single SET + GET round-trip
// ========================================================================
static void test_single_set_get() {
  printf("  test_single_set_get...");
  TestGuard g;

  // Prepare data
  const size_t data_size = 4096;
  std::vector<uint8_t> write_buf(data_size);
  for (size_t i = 0; i < data_size; ++i) {
    write_buf[i] = static_cast<uint8_t>(i & 0xFF);
  }

  // SET
  const char* key = "test_key_1";
  const char* keys[] = {key};
  const void* wbufs[] = {write_buf.data()};
  size_t lens[] = {data_size};

  uint64_t fid = cache_engine_submit_batch_set(
      g.conn, keys, wbufs, lens, 1, data_size);
  assert(fid != 0);

  auto completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].future_id == fid);
  assert(completions[0].ok == 1);
  assert(completions[0].error == nullptr);

  // GET
  std::vector<uint8_t> read_buf(data_size, 0);
  void* rbufs[] = {read_buf.data()};

  fid = cache_engine_submit_batch_get(
      g.conn, keys, rbufs, lens, 1, data_size);
  assert(fid != 0);

  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].future_id == fid);
  assert(completions[0].ok == 1);
  assert(completions[0].error == nullptr);

  // Verify data
  assert(memcmp(write_buf.data(), read_buf.data(), data_size) == 0);

  printf(" OK\n");
}

// ========================================================================
// Test: EXISTS
// ========================================================================
static void test_exists() {
  printf("  test_exists...");
  TestGuard g;

  // Write a key
  const size_t data_size = 1024;
  std::vector<uint8_t> buf(data_size, 42);

  const char* key = "exists_key";
  const char* keys[] = {key};
  const void* wbufs[] = {buf.data()};
  size_t lens[] = {data_size};

  uint64_t fid = cache_engine_submit_batch_set(
      g.conn, keys, wbufs, lens, 1, data_size);
  assert(fid != 0);
  auto completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Check EXISTS for existing key
  fid = cache_engine_submit_batch_exists(g.conn, keys, 1);
  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].result_bytes != nullptr);
  assert(completions[0].result_len == 1);
  assert(completions[0].result_bytes[0] == 1);

  // Check EXISTS for non-existing key
  const char* missing_key = "no_such_key";
  const char* missing_keys[] = {missing_key};

  fid = cache_engine_submit_batch_exists(g.conn, missing_keys, 1);
  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].result_bytes != nullptr);
  assert(completions[0].result_len == 1);
  assert(completions[0].result_bytes[0] == 0);

  printf(" OK\n");
}

// ========================================================================
// Test: batch operations
// ========================================================================
static void test_batch_operations() {
  printf("  test_batch_operations...");
  TestGuard g;

  const size_t num_keys = 10;
  const size_t data_size = 2048;

  // Prepare keys and data
  std::vector<std::string> key_strs(num_keys);
  std::vector<const char*> key_ptrs(num_keys);
  std::vector<std::vector<uint8_t>> write_bufs(num_keys);
  std::vector<const void*> wbuf_ptrs(num_keys);
  std::vector<size_t> lens(num_keys, data_size);

  for (size_t i = 0; i < num_keys; ++i) {
    key_strs[i] = "batch_key_" + std::to_string(i);
    key_ptrs[i] = key_strs[i].c_str();
    write_bufs[i].resize(data_size);
    for (size_t j = 0; j < data_size; ++j) {
      write_bufs[i][j] = static_cast<uint8_t>((i * 7 + j) & 0xFF);
    }
    wbuf_ptrs[i] = write_bufs[i].data();
  }

  // Batch SET
  uint64_t fid = cache_engine_submit_batch_set(
      g.conn, key_ptrs.data(), wbuf_ptrs.data(), lens.data(),
      num_keys, data_size);
  assert(fid != 0);

  auto completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Batch EXISTS
  fid = cache_engine_submit_batch_exists(
      g.conn, key_ptrs.data(), num_keys);
  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].result_len == num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    assert(completions[0].result_bytes[i] == 1);
  }

  // Batch GET
  std::vector<std::vector<uint8_t>> read_bufs(num_keys);
  std::vector<void*> rbuf_ptrs(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    read_bufs[i].resize(data_size, 0);
    rbuf_ptrs[i] = read_bufs[i].data();
  }

  fid = cache_engine_submit_batch_get(
      g.conn, key_ptrs.data(), rbuf_ptrs.data(), lens.data(),
      num_keys, data_size);
  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Verify all data
  for (size_t i = 0; i < num_keys; ++i) {
    assert(
        memcmp(write_bufs[i].data(), read_bufs[i].data(), data_size)
        == 0);
  }

  printf(" OK\n");
}

// ========================================================================
// Test: concurrent operations (multiple batches in-flight)
// ========================================================================
static void test_concurrent_batches() {
  printf("  test_concurrent_batches...");
  TestGuard g;

  const size_t data_size = 4096;
  const int num_batches = 5;

  // Submit multiple SET batches without draining
  std::vector<uint64_t> fids;
  std::vector<std::vector<uint8_t>> all_bufs;
  std::vector<std::string> all_keys;

  for (int b = 0; b < num_batches; ++b) {
    std::string key = "concurrent_key_" + std::to_string(b);
    all_keys.push_back(key);

    std::vector<uint8_t> buf(data_size);
    for (size_t i = 0; i < data_size; ++i) {
      buf[i] = static_cast<uint8_t>((b * 13 + i) & 0xFF);
    }
    all_bufs.push_back(std::move(buf));

    const char* keys[] = {all_keys.back().c_str()};
    const void* wbufs[] = {all_bufs.back().data()};
    size_t lens[] = {data_size};

    uint64_t fid = cache_engine_submit_batch_set(
        g.conn, keys, wbufs, lens, 1, data_size);
    assert(fid != 0);
    fids.push_back(fid);
  }

  // Drain all completions
  int total_completed = 0;
  int retries = 0;
  while (total_completed < num_batches && retries < 50) {
    auto completions = drain_with_wait(g.conn, 500);
    for (const auto& c : completions) {
      assert(c.ok == 1);
      total_completed++;
    }
    retries++;
  }
  assert(total_completed == num_batches);

  // Verify data correctness after concurrent writes
  for (int b = 0; b < num_batches; ++b) {
    std::vector<uint8_t> read_buf(data_size, 0);
    const char* keys[] = {all_keys[b].c_str()};
    void* rbufs[] = {read_buf.data()};
    size_t lens[] = {data_size};

    uint64_t fid = cache_engine_submit_batch_get(
        g.conn, keys, rbufs, lens, 1, data_size);
    assert(fid != 0);

    auto completions = drain_with_wait(g.conn);
    assert(completions.size() == 1);
    assert(completions[0].ok == 1);
    assert(
        memcmp(all_bufs[b].data(), read_buf.data(), data_size)
        == 0);
  }

  printf(" OK\n");
}

// ========================================================================
// Test: close idempotency
// ========================================================================
static void test_close_idempotent() {
  printf("  test_close_idempotent...");
  std::string dir = make_temp_dir();

  std::string cfg = make_config(dir);
  cache_engine_t* conn = cache_engine_create(cfg.c_str());
  assert(conn != nullptr);

  // Close multiple times
  cache_engine_close(conn);
  cache_engine_close(conn);
  cache_engine_close(conn);

  cache_engine_destroy(conn);
  remove_dir(dir);
  printf(" OK\n");
}

// ========================================================================
// Test: error handling
// ========================================================================
static void test_error_handling() {
  printf("  test_error_handling...");

  // NULL engine arguments
  uint64_t fid = cache_engine_submit_batch_get(
      nullptr, nullptr, nullptr, nullptr, 0, 0);
  assert(fid == 0);
  const char* err = cache_engine_last_error();
  assert(err != nullptr);
  assert(strlen(err) > 0);

  // GET on non-existent key should fail
  TestGuard g;

  const size_t data_size = 1024;
  std::vector<uint8_t> buf(data_size, 0);
  const char* key = "nonexistent_key";
  const char* keys[] = {key};
  void* rbufs[] = {buf.data()};
  size_t lens[] = {data_size};

  fid = cache_engine_submit_batch_get(
      g.conn, keys, rbufs, lens, 1, data_size);
  assert(fid != 0);

  auto completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 0);  // Should fail
  assert(completions[0].error != nullptr);
  assert(strlen(completions[0].error) > 0);

  printf(" OK\n");
}

// ========================================================================
// Test: large data (simulating KV cache chunks)
// ========================================================================
static void test_large_data() {
  printf("  test_large_data...");
  TestGuard g;

  // 1 MB chunk (typical KV cache size)
  const size_t data_size = 1024 * 1024;
  std::vector<uint8_t> write_buf(data_size);
  for (size_t i = 0; i < data_size; ++i) {
    write_buf[i] = static_cast<uint8_t>((i * 17) & 0xFF);
  }

  const char* key = "large_chunk";
  const char* keys[] = {key};
  const void* wbufs[] = {write_buf.data()};
  size_t lens[] = {data_size};

  // SET
  uint64_t fid = cache_engine_submit_batch_set(
      g.conn, keys, wbufs, lens, 1, data_size);
  assert(fid != 0);
  auto completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].error == nullptr);

  // GET
  std::vector<uint8_t> read_buf(data_size, 0);
  void* rbufs[] = {read_buf.data()};

  fid = cache_engine_submit_batch_get(
      g.conn, keys, rbufs, lens, 1, data_size);
  assert(fid != 0);
  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].error == nullptr);

  assert(memcmp(write_buf.data(), read_buf.data(), data_size) == 0);

  printf(" OK\n");
}

// ========================================================================
// Test: overwriting an existing key returns the latest value
// ========================================================================
static void test_data_overwrite() {
  printf("  test_data_overwrite...");
  TestGuard g;

  const size_t data_size = 2048;
  const char* key = "ow_key";
  const char* keys[] = {key};
  size_t lens[] = {data_size};

  // First write: all 0xAA
  std::vector<uint8_t> buf_aa(data_size, 0xAA);
  const void* wbufs_aa[] = {buf_aa.data()};

  uint64_t fid = cache_engine_submit_batch_set(
      g.conn, keys, wbufs_aa, lens, 1, data_size);
  assert(fid != 0);
  auto completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Second write: all 0xBB
  std::vector<uint8_t> buf_bb(data_size, 0xBB);
  const void* wbufs_bb[] = {buf_bb.data()};

  fid = cache_engine_submit_batch_set(
      g.conn, keys, wbufs_bb, lens, 1, data_size);
  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Read back and verify we get 0xBB
  std::vector<uint8_t> read_buf(data_size, 0);
  void* rbufs[] = {read_buf.data()};

  fid = cache_engine_submit_batch_get(
      g.conn, keys, rbufs, lens, 1, data_size);
  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  assert(memcmp(buf_bb.data(), read_buf.data(), data_size) == 0);

  printf(" OK\n");
}

// ========================================================================
// Test: key containing slashes (tests separator replacement)
// ========================================================================
static void test_key_with_slashes() {
  printf("  test_key_with_slashes...");
  TestGuard g;

  const size_t data_size = 1024;
  std::vector<uint8_t> write_buf(data_size, 0xCD);

  const char* key = "model/layer_0/chunk_42";
  const char* keys[] = {key};
  const void* wbufs[] = {write_buf.data()};
  size_t lens[] = {data_size};

  // SET
  uint64_t fid = cache_engine_submit_batch_set(
      g.conn, keys, wbufs, lens, 1, data_size);
  assert(fid != 0);
  auto completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // GET
  std::vector<uint8_t> read_buf(data_size, 0);
  void* rbufs[] = {read_buf.data()};

  fid = cache_engine_submit_batch_get(
      g.conn, keys, rbufs, lens, 1, data_size);
  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  assert(memcmp(write_buf.data(), read_buf.data(), data_size) == 0);

  printf(" OK\n");
}

// ========================================================================
// Test: batch_exists with a mix of existing and non-existing keys
// ========================================================================
static void test_batch_exists_mixed() {
  printf("  test_batch_exists_mixed...");
  TestGuard g;

  const size_t data_size = 512;

  // Write only even-indexed keys: exist_0, exist_2, exist_4
  const size_t num_write = 3;
  std::string write_key_strs[] = {
      "exist_0", "exist_2", "exist_4"};
  const char* write_key_ptrs[] = {write_key_strs[0].c_str(),
                                  write_key_strs[1].c_str(),
                                  write_key_strs[2].c_str()};
  std::vector<uint8_t> buf(data_size, 0x11);
  const void* wbufs[] = {buf.data(), buf.data(), buf.data()};
  size_t lens[] = {data_size, data_size, data_size};

  uint64_t fid = cache_engine_submit_batch_set(
      g.conn, write_key_ptrs, wbufs, lens, num_write, data_size);
  assert(fid != 0);
  auto completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);

  // Check all 5 keys: exist_0 through exist_4
  const size_t num_check = 5;
  std::string check_key_strs[] = {"exist_0", "exist_1", "exist_2",
                                  "exist_3", "exist_4"};
  const char* check_key_ptrs[] = {
      check_key_strs[0].c_str(), check_key_strs[1].c_str(),
      check_key_strs[2].c_str(), check_key_strs[3].c_str(),
      check_key_strs[4].c_str()};

  fid = cache_engine_submit_batch_exists(
      g.conn, check_key_ptrs, num_check);
  completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].result_bytes != nullptr);
  assert(completions[0].result_len == num_check);
  assert(completions[0].result_bytes[0] == 1);  // exist_0: present
  assert(completions[0].result_bytes[1] == 0);  // exist_1: absent
  assert(completions[0].result_bytes[2] == 1);  // exist_2: present
  assert(completions[0].result_bytes[3] == 0);  // exist_3: absent
  assert(completions[0].result_bytes[4] == 1);  // exist_4: present

  printf(" OK\n");
}

// ========================================================================
// Test: multiple drain calls do not return duplicate completions
// ========================================================================
static void test_multiple_drain_calls() {
  printf("  test_multiple_drain_calls...");
  TestGuard g;

  const size_t data_size = 1024;
  std::vector<uint8_t> buf(data_size, 0x77);

  const char* key = "drain_key";
  const char* keys[] = {key};
  const void* wbufs[] = {buf.data()};
  size_t lens[] = {data_size};

  // Submit one SET
  uint64_t fid = cache_engine_submit_batch_set(
      g.conn, keys, wbufs, lens, 1, data_size);
  assert(fid != 0);

  // First drain: should get 1 completion
  auto completions = drain_with_wait(g.conn);
  assert(completions.size() == 1);
  assert(completions[0].ok == 1);
  assert(completions[0].future_id == fid);

  // Second drain immediately: should get 0 (no new completions)
  cache_completion_t extra[16];
  int n = cache_engine_drain_completions(g.conn, extra, 16);
  assert(n == 0);

  printf(" OK\n");
}

// ========================================================================
// Main
// ========================================================================
int main() {
  printf("Running DingoFS CacheEngine C API tests:\n");

  test_create_destroy();
  test_invalid_config();
  test_single_set_get();
  test_exists();
  test_batch_operations();
  test_concurrent_batches();
  test_close_idempotent();
  test_error_handling();
  test_large_data();
  test_data_overwrite();
  test_key_with_slashes();
  test_batch_exists_mixed();
  test_multiple_drain_calls();

  printf("\nAll tests passed!\n");
  return 0;
}
