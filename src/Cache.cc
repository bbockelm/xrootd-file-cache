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

      
        bool havePrefetch = Factory::GetInstance().HavePrefetchForIO(*io);

        // check file is in already on disk
        // AMT todo:: resolve ovnership of XrdOssDF
        XrdOssDF* preExistDF = Factory::GetInstance().GetOss()->newFile(Factory::GetInstance().GetUsername().c_str());
        if (!havePrefetch)
        {
            XrdOucEnv myEnv;
            std::string fname;
            getFilePathFromURL(io->Path(), fname);
            fname = Factory::GetInstance().GetTempDirectory() + fname;
            int res = preExistDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv);
            if (res >= 0)
                m_log.Emsg("Attach", "File already cached on disk.");
        }

        // use prefetch if file is not yet on disk
        PrefetchPtr prefetch;
        if (preExistDF->getFD() <= 0)
        {
            prefetch = Factory::GetInstance().GetPrefetch(*io);
	    // AMT:: shouldn't we check if thread is already running
            pthread_t tid;
            XrdSysThread::Run(&tid, PrefetchRunner, (void *)(prefetch.get()), 0, "XrdFileCache Prefetcher");
        }

        return new IO(*io, m_stats, *this,preExistDF, prefetch,  m_log);
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
   // AMT:: where is this used ??
    XrdSysMutexHelper lock(&m_io_mutex);
    return m_attached;
}

void
Cache::Detach(XrdOucCacheIO* io)
{ 
    XrdSysMutexHelper lock(&m_io_mutex);
    m_attached--;

    std::stringstream ss; ss << m_attached << " " << io->Path();
    m_log.Emsg("Detach", "deleting io object ", ss.str().c_str() );

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
  
}
