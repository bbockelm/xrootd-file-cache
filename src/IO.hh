#ifndef __XRDOUCCACHEDATA_HH__
#define __XRDOUCCACHEDATA_HH__
/******************************************************************************/
/*                                                                            */
/* (c) 2012 University of Nebraska-Lincoln                                    */
/*     by Brian Bockelman                                                     */
/*                                                                            */
/******************************************************************************/

/*
 * The XrdFileCacheIO object is used as a proxy for the original source
 */

#include <XrdOuc/XrdOucCache.hh>
#include "XrdSys/XrdSysPthread.hh"

#include "XrdFileCacheFwd.hh"

class XrdSysError;
class XrdOssDF;
class XfcStats;

namespace XrdFileCache
{

class Stats : public XrdOucCacheStats
{
public:
   long long    BytesGetPrefetch;
   long long    BytesGetDisk;
   int          HitsPrefetch;
   int          HitsDisk;

   inline void AddStat(Stats &Src)
   {      
      XrdOucCacheStats::Add(Src);

      sMutex1.Lock();
      BytesGetPrefetch += Src.BytesGetPrefetch;
      BytesGetDisk     += Src.BytesGetDisk;

      HitsPrefetch += Src.HitsPrefetch;
      HitsDisk     += Src.HitsDisk;

      sMutex1.UnLock();
   }

   Stats() : BytesGetPrefetch(0),BytesGetDisk(0),HitsPrefetch(0), HitsDisk(0){}
private:
   XrdSysMutex sMutex1;

};

class IO : public XrdOucCacheIO
{

   friend class Cache;

public:

   XrdOucCacheIO *
   Base() {return &m_io; }

   virtual XrdOucCacheIO *Detach();

   long long
   FSize() {return m_io.FSize(); }

   const char *
   Path() {return m_io.Path(); }

   int Read (char  *Buffer, long long Offset, int Length);

#if defined(HAVE_READV)
   virtual int  ReadV (const XrdOucIOVec *readV, int n);

#endif

   int
   Sync() {return 0; }

   int
   Trunc(long long Offset) { errno = ENOTSUP; return -1; }

   int
   Write(char *Buffer, long long Offset, int Length) { errno = ENOTSUP; return -1; }

protected:
<<<<<<< HEAD
    IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache &cache);

private:
   ~IO();
    bool getFilePathFromURL(const char* url, std::string& res);

    XrdOucCacheIO & m_io;
    XrdOucCacheStats & m_statsGlobal;
    XfcStats* m_stats;
    Cache & m_cache;
    XrdOssDF* m_diskDF;
    PrefetchPtr m_prefetch;
=======
   IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache &cache, XrdSysError &);

private:
   ~IO();
   bool getFilePathFromURL(const char* url, std::string& res);

   XrdOucCacheIO & m_io;
   XrdOucCacheStats & m_statsGlobal;
   Stats   m_stats;
   Cache & m_cache;
   XrdOssDF* m_diskDF;
   PrefetchPtr m_prefetch;
   XrdSysError m_log;
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
};

}
#endif
