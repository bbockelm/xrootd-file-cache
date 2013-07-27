#ifndef  __XRDFILECACHE_CONTEXT_HH
#define  __XRDFILECACHE_CONTEXT_HH

#include <fstream>
#include <iostream>

class XrdOucCacheIO;


#define aMsg(level, format, ...)\
   if (Dbg > level) XrdFileCache::strprintf(level, format, ##__VA_ARGS__)

#define aMsgIO(level, io, format, ...) \
  if (Dbg > level) XrdFileCache::strprintfIO(level, io, format, ##__VA_ARGS__)


namespace  XrdFileCache
{ 
enum LogLevel {
   kDump,
   kDebug,
   kInfo,
   kWarning,
   kError
};

extern LogLevel Dbg;
extern std::fstream Rec;

const char* levelName(LogLevel);
void strprintf(LogLevel level, const char* fmt, ...);
void strprintfIO(LogLevel level,  XrdOucCacheIO* io, const char* fmt, ...);
}

#endif
