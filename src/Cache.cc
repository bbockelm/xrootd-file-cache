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
        return new IO(*io, m_stats, *this, m_log);
    }
    else
    {
        m_log.Emsg("Attach", "No caching !!! ", io->Path());
    }
    
    m_attached--;
    return io;
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


