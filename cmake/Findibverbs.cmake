# - Find libibverbs
#
# ibverbs_INCLUDE_DIR - Where to find infiniband/verbs.h
# ibverbs_LIBRARIES   - List of libraries when using ibverbs.
# ibverbs_FOUND       - True if ibverbs found.

find_path(ibverbs_INCLUDE_DIR
  NAMES infiniband/verbs.h)
find_library(ibverbs_LIBRARIES
  NAMES libibverbs.so.1 libibverbs.so)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ibverbs
  DEFAULT_MSG ibverbs_LIBRARIES ibverbs_INCLUDE_DIR)

mark_as_advanced(
  ibverbs_INCLUDE_DIR
  ibverbs_LIBRARIES)

if(ibverbs_FOUND AND NOT TARGET ibverbs::ibverbs)
  add_library(ibverbs::ibverbs UNKNOWN IMPORTED)
  set_target_properties(ibverbs::ibverbs PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${ibverbs_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${ibverbs_LIBRARIES}")
endif()
