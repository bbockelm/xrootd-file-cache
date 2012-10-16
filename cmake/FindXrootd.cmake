
FIND_PATH(XROOTD_INCLUDES XrdVersion.hh
  HINTS
  ${XROOTD_DIR}
  $ENV{XROOTD_DIR}
  /usr
  /opt/xrootd/
  PATH_SUFFIXES include/xrootd
  PATHS /opt/xrootd
)

FIND_LIBRARY(XROOTD_UTILS XrdUtils
  HINTS
  ${XROOTD_DIR}
  $ENV{XROOTD_DIR}
  /usr
  /opt/xrootd/
  PATH_SUFFIXES lib
)

FIND_LIBRARY(XROOTD_SERVER XrdServer
  HINTS
  ${XROOTD_DIR}
  $ENV{XROOTD_DIR}
  /usr
  /opt/xrootd/
  PATH_SUFFIXES lib
)

FIND_LIBRARY(XROOTD_CLIENT XrdClient
  HINTS
  ${XROOTD_DIR}
  $ENV{XROOTD_DIR}
  /usr
  /opt/xrootd
  PATH_SUFFIXES lib
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Xrootd DEFAULT_MSG XROOTD_UTILS XROOTD_SERVER XROOTD_CLIENT XROOTD_INCLUDES)

