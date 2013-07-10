#include "Context.hh"
#include <stdarg.h>
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucCache.hh"
#include "XrdOss/XrdOss.hh"
#if !defined(HAVE_VERSIONS)
#include "XrdOss/XrdOssApi.hh"
#endif
#include "XrdSys/XrdSysPlugin.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdVersion.hh"
#include "XrdPosix/XrdPosixXrootd.hh"

#include "Factory.hh"

namespace XrdFileCache
{
LogLevel Dbg;
std::fstream Rec;

const char* const s_levelNames[] = { "Dump ", "Debug","Info ", "Warn ", "Err  " };

const char* levelName(LogLevel iLevel) {
   return s_levelNames[iLevel];
}


void strprintf(LogLevel level, const char* fmt, ...)
{
   int size = 512;

   std::string str;

   va_list ap;
   while (true)
   {
      str.resize(size);
      va_start(ap, fmt);
      int n = vsnprintf((char *)str.c_str(), size, fmt, ap);
      va_end(ap);
      if (n > -1 && n < size)
      {
         Factory::GetInstance().GetSysError().Emsg(levelName(level), str.c_str());
         return;
      }

      if (n > -1)
         size = n + 1;
      else
         size *= 2;
   }
}


void strprintfIO(LogLevel level, XrdOucCacheIO* io, const char* fmt, ...)
{
   int size = 512;

   std::string str;

   va_list ap;
   while (true)
   {
      str.resize(size);
      va_start(ap, fmt);
      int n = vsnprintf((char *)str.c_str(), size, fmt, ap);
      va_end(ap);
      if (n > -1 && n < size)
      {
         std::string path = io->Path();

         size_t kloc = path.rfind("?");
         size_t split_loc = path.rfind("//");
         if (split_loc != path.npos && kloc != path.npos) {

            Factory::GetInstance().GetSysError().Emsg(levelName(level), str.c_str(), path.substr(split_loc+1,kloc-split_loc-1).c_str());
         }
         else
            Factory::GetInstance().GetSysError().Emsg(levelName(level), str.c_str(), io->Path());
         return;
      }

      if (n > -1)
         size = n + 1;
      else
         size *= 2;
   }
}

}
