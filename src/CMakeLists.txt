
include_directories( ${XROOTD_INCLUDES} )
add_library (XrdFileCache MODULE IO.cc Factory.cc Cache.cc Prefetch.cc)

target_link_libraries(XrdFileCache ${XROOTD_UTILS} ${XROOTD_SERVER})

