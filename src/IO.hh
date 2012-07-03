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

namespace XrdFileCache {

class IO : public XrdOucCacheIO
{

friend class Cache;

public:

    XrdOucCacheIO *Base() {return &m_io;}

    virtual XrdOucCacheIO *Detach();

    long long FSize() {return m_io.FSize();}

    const char *Path() {return m_io.Path();}

    int Read (char  *Buffer, long long  Offset, int  Length);

    int Sync() {return 0;}

    int Trunc(long long Offset) { errno = ENOTSUP; return -1; }

    int Write(char *Buffer, long long Offset, int Length) { errno = ENOTSUP; return -1; }

protected:
    IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache &cache, PrefetchPtr pread);

private:

    ~IO() {}
    int Read (XrdOucCacheStats &Now, char *Buffer, long long Offs, int Length);

    XrdOucCacheIO & m_io;
    XrdOucCacheStats & m_stats;
    PrefetchPtr m_prefetch;
    Cache & m_cache;

};

}
#endif
