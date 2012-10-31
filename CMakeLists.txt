
cmake_minimum_required( VERSION 2.6 )
project( xrootd-file-cache )

set( CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake )

find_package( Xrootd REQUIRED )

if( CMAKE_COMPILER_IS_GNUCXX )
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror" )
endif()

if( CMAKE_COMPILER_IS_GNUCC )
  set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror" )
endif()

INCLUDE (CheckCXXSourceCompiles)
SET(CMAKE_REQUIRED_INCLUDES ${XROOTD_INCLUDES})
check_cxx_source_compiles("
#include <XrdOuc/XrdOucCache.hh>
int main() {
    XrdSfsReadV readV;
    return 0;
}
" HAVE_READV)

if (HAVE_READV)
  add_definitions(-DHAVE_READV)
endif()

include_directories ("${PROJECT_SOURCE_DIR}")
add_subdirectory( src )
add_subdirectory( test/xrdfragcp )

