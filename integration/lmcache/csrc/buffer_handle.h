// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef DINGOFS_INTEGRATION_LMCACHE_BUFFER_HANDLE_H_
#define DINGOFS_INTEGRATION_LMCACHE_BUFFER_HANDLE_H_

#include <pybind11/pybind11.h>

namespace dingofs {
namespace integration {
namespace lmcache {

namespace py = pybind11;

// BufferHandle pins a Python object so its raw memory survives an async I/O.
//
// Construction must happen with the GIL held (we steal a strong ref via
// release()). Destruction runs on whatever thread releases the last
// shared_ptr (typically a bthread, no GIL); we reacquire the GIL and drop
// the Python ref. Embedding via shared_ptr lets us pass the handle through
// std::function<void(void*)>, which IOBuffer::AppendUserData requires.
class BufferHandle {
 public:
  explicit BufferHandle(py::object obj) noexcept
      : owner_(obj.release().ptr()) {}

  ~BufferHandle() {
    if (owner_ != nullptr) {
      py::gil_scoped_acquire acquire;
      Py_DECREF(owner_);
    }
  }

  BufferHandle(const BufferHandle&) = delete;
  BufferHandle& operator=(const BufferHandle&) = delete;
  BufferHandle(BufferHandle&&) = delete;
  BufferHandle& operator=(BufferHandle&&) = delete;

 private:
  PyObject* owner_;
};

}  // namespace lmcache
}  // namespace integration
}  // namespace dingofs

#endif  // DINGOFS_INTEGRATION_LMCACHE_BUFFER_HANDLE_H_
