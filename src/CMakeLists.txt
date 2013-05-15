
include_directories( ${XROOTD_INCLUDES} ${XROOTD_INCLUDES_PRIVATE} )
add_library (XrdFileCache MODULE IO.cc Factory.cc Cache.cc Prefetch.cc Context.cc File.cc)
add_library (XrdFileCacheAllowAlways MODULE AllowDecision.cc)

target_link_libraries(XrdFileCache ${XROOTD_UTILS} ${XROOTD_SERVER} ${XROOTD_CLIENT})

install(
  TARGETS XrdFileCache
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )

install(
  TARGETS XrdFileCacheAllowAlways
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )

install(
  FILES Decision.hh
  DESTINATION include )

