
include_directories( ${XROOTD_INCLUDES} )
add_executable( xrdfragcp xrdfragcp.cxx )
target_link_libraries( xrdfragcp ${XROOTD_UTILS} ${XROOTD_CLIENT} )

