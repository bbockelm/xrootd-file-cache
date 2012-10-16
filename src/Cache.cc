
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOss/XrdOssApi.hh"

#include "IO.hh"
#include "Cache.hh"
#include "Factory.hh"
#include "Prefetch.hh"

using namespace XrdFileCache;

void *PrefetchRunner(void * prefetch_void)
{
    Prefetch *prefetch = static_cast<Prefetch *>(prefetch_void);
    if (prefetch)
        prefetch->Run();
    return NULL;
}

Cache *Cache::m_cache = NULL;
XrdSysMutex Cache::m_cache_mutex;

Cache::Cache(XrdOucCacheStats & stats, XrdSysError & log)
    : m_attached(0),
      m_log(log),
      m_stats(stats)
{
}

XrdOucCacheIO *
Cache::Attach(XrdOucCacheIO *io, int Options)
{
    XrdSysMutexHelper lock(&m_io_mutex);
    m_attached ++;
    if (io)
    {
        m_log.Emsg("Attach", "Creating new IO object for file ", io->Path());
        PrefetchPtr prefetch = Factory::GetInstance().GetPrefetch(*io);

        // TODO: Check to see if we should prefetch this!
        pthread_t tid;
        XrdSysThread::Run(&tid, PrefetchRunner, (void *)(prefetch.get()), 0, "XrdFileCache Prefetcher");

        return new IO(*io, m_stats, *this, prefetch, m_log);
    }
    else
    {
        m_attached --;
        return NULL;
    }
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
    XrdSysMutexHelper lock(&m_io_mutex);
    m_attached--;
    delete io;
}

