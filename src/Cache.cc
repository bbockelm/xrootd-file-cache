#include "XrdSys/XrdSysPthread.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"

#include <fcntl.h>
#include <sstream>


#include "IO.hh"
#include "Cache.hh"
#include "Factory.hh"
#include "Prefetch.hh"
#include "Context.hh"

using namespace XrdFileCache;


Cache *Cache::m_cache = NULL;
XrdSysMutex Cache::m_cache_mutex;

Cache::Cache(XrdOucCacheStats & stats)
    : m_attached(0),
      m_stats(stats)
{}

XrdOucCacheIO *
Cache::Attach(XrdOucCacheIO *io, int Options)
{
    XrdSysMutexHelper lock(&m_io_mutex);
    m_attached++;

    aMsgIO(kInfo, io, "Cache::Attach()");
    if (io)
    {
       return new IO(*io, m_stats, *this);
    }
    else
    {
       aMsgIO(kDebug, io, "Cache::Attache(), XrdOucCacheIO == NULL");
    }
    
    m_attached--;
    return io;
}

int
Cache::isAttached()
{
    XrdSysMutexHelper lock(&m_io_mutex);
    return m_attached;
}

void
Cache::Detach(XrdOucCacheIO* io)
{
    aMsgIO(kInfo, io, "Cache::Detach()");
    XrdSysMutexHelper lock(&m_io_mutex);
    m_attached--;

    aMsgIO(kDebug, io, "Cache::Detach(), deleting IO object. Attach count = %d", m_attached);


    delete io;
}


