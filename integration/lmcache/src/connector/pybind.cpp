// SPDX-License-Identifier: Apache-2.0

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "connector_pybind_utils.h"
#include "dingofs_connector.h"

namespace py = pybind11;

PYBIND11_MODULE(_native, m) {
  m.doc() = "DingoFS native connector for LMCache";

  py::class_<dingofs::connector::DingoFSConnector,
             std::shared_ptr<dingofs::connector::DingoFSConnector>>(
      m, "DingoFSConnector")
      .def(py::init<const std::string&, uint32_t, uint64_t, uint32_t,
                     size_t, int>(),
           py::arg("cache_dir"),
           py::arg("fs_id") = 1,
           py::arg("ino") = 1,
           py::arg("cache_size_mb") = 102400,
           py::arg("exists_cache_capacity") = 100000,
           py::arg("num_workers") = 4)
      LMCACHE_BIND_CONNECTOR_METHODS(
          dingofs::connector::DingoFSConnector);
}
