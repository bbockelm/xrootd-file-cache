#include "XrdSys/XrdSysPthread.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"

#include <fcntl.h>
#include <sstream>


#include "IO.hh"
#include "Cache.hh"
#include "Factory.hh"
#include "Prefetch.hh"
#include "File.hh"
#include "Context.hh"

using namespace XrdFileCache;

void *
PrefetchRunner(void * prefetch_void)
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
{}

XrdOucCacheIO *
Cache::Attach(XrdOucCacheIO *io, int Options)
{
    XrdSysMutexHelper lock(&m_io_mutex);
    m_attached++;

    m_log.Emsg("Attach", "Creating new IO object for file ", io->Path());
    Rec << time(NULL)  << " Attach " << io->Path() << std::endl;
    if (io)
    {

        FilePtr f =   Factory::GetInstance().GetXfcFile(*io);
        // AMT: should thread be  spawned on each Attach() ??
        pthread_t tid;
        Rec << time(NULL) << " Prefetch " << io->Path() << std::endl;
        XrdSysThread::Run(&tid, PrefetchRunner, (void *)(f->GetPrefetch()), 0, "XrdFileCache Prefetcher");
        return new IO(*io, m_stats, *this, f,  m_log);
    }
    else
    {
        m_attached--;
        return NULL;
    }
}

int
Cache::isAttached()
{
    // AMT:: is this used anywere, can si it also in xrootd coide itself??
    XrdSysMutexHelper lock(&m_io_mutex);
    return m_attached;
}

void
Cache::Detach(XrdOucCacheIO* io)
{
    Rec << time(NULL)  << " Detach " << io->Path() << std::endl;

    // AMT:: don't know why ~IO should be called from this class
    //        why not directly in IO::Detach() ???
    XrdSysMutexHelper lock(&m_io_mutex);
    m_attached--;

    std::stringstream ss; ss << m_attached << " " << io->Path();
    if (Dbg) m_log.Emsg("Detach", "deleting io object ", ss.str().c_str() );

    delete io;
}


// AMT the function is a UTIL, does not have to be in this class
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
