#include "Logger.hh"
#include "XrdSys/XrdSysLogger.hh"

#include <sstream>
#include <stdio.h>

using namespace XrdFileCache;

Logger::Logger(XrdSysLogger *lp, const char *ErrPrefix)
   :XrdSysError (lp, ErrPrefix), m_extra(0)
{
    //    logger(lp);
    SetPrefix(ErrPrefix); 
}



void Logger::Emsg2(const char *esfx, const char *text1, 
                   const char *text2,
                   const char *text3)
{
   Emsg(esfx, text1, text2, text3);
   if (m_extra)
   {
      printf("test \n");
      XrdSysLogger* old = logger(m_extra);
      Emsg(esfx, text1, text2, text3);
      logger(old);
   }
}
