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
      m_stats(stats),
      m_cached_file(0),
      m_read_from_disk(false)
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

        m_cached_file = Factory::GetInstance().GetOss()->newFile(Factory::GetInstance().GetUsername().c_str());
        checkDiskCache(io);

        PrefetchPtr prefetch;
        if (!m_read_from_disk)
        {
           prefetch = Factory::GetInstance().GetPrefetch(*io);
           pthread_t tid;
           XrdSysThread::Run(&tid, PrefetchRunner, (void *)(prefetch.get()), 0, "XrdFileCache Prefetcher");
        }


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

    std::stringstream ss; ss << m_attached << " " << io->Path();
    m_log.Emsg("Detach", "io object ", ss.str().c_str() );

    delete io;
}

bool
Cache::getFilePathFromURL(const char* url, std::string &result)
{
   std::string path = url;
   size_t split_loc = path.rfind("//");

   if (split_loc == path.npos)
      return false;

   size_t kloc = path.rfind("?");
   result = path.substr(split_loc+1,kloc-split_loc-1);

   if (kloc == path.npos)
      return false;

   return true;
}

void
Cache::checkDiskCache(XrdOucCacheIO* io)
{
   XrdOucEnv myEnv;

   std::string fname;
   getFilePathFromURL(io->Path(), fname);
   fname = Factory::GetInstance().GetTempDirectory() + fname;

   int res =  m_cached_file->Open(fname.c_str(), O_RDONLY, 0600, myEnv);
   if (res >= 0)
      m_read_from_disk = true;
}
