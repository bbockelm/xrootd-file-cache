#ifndef __XRDFILECACHE_LOGGER_HH__
#define __XRDFILECACHE_LOGGER_HH__

#include "XrdSys/XrdSysError.hh"

namespace XrdFileCache {
class Logger : public XrdSysError
{
public:
   Logger(XrdSysLogger *lp, const char *ErrPrefix="sys");

   void Emsg2(const char *esfx, const char *text1, 
              const char *text2=0,
              const char *text3=0);

   XrdSysLogger* m_extra;

};

}

#endif
