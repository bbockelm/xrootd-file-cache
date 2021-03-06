
#ifndef __XRDFILECACHE_CACHE_HH__
#define __XRDFILECACHE_CACHE_HH__
/******************************************************************************/
/*                                                                            */
/* (c) 2012 University of Nebraksa-Lincoln                                    */
/*     by Brian Bockelman                                                     */
/*                                                                            */
/******************************************************************************/

#include <XrdSys/XrdSysPthread.hh>
#include <XrdOuc/XrdOucCache.hh>

#include "XrdFileCacheFwd.hh"

class XrdOssDF;

namespace XrdFileCache {

class Cache : public XrdOucCache
{

friend class IO;
friend class Factory;

public:

    XrdOucCacheIO *Attach(XrdOucCacheIO *, int Options=0);

    int isAttached();

    virtual XrdOucCache* Create(XrdOucCache::Parms&, XrdOucCacheIO::aprParms*) {return NULL;}
   static bool getFilePathFromURL(const char* url, std::string& res);

protected:

    Cache(XrdOucCacheStats&, XrdSysError&);

    void checkDiskCache(XrdOucCacheIO*);
    XrdOssDF* getCachedFile() { return m_cached_file; }
    bool readFromDisk() const { return m_read_from_disk; }

private:

    void Detach(XrdOucCacheIO *);

    static Cache *m_cache;
    static XrdSysMutex m_cache_mutex;

    XrdSysMutex m_io_mutex;
    unsigned int m_attached;

    XrdSysError & m_log;
    XrdOucCacheStats & m_stats;

   XrdOssDF* m_cached_file;
   bool      m_read_from_disk;


};

}

#endif
