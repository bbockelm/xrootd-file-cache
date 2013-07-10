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

<<<<<<< HEAD
    aMsgIO(kDebug, io, "Cache::Attache()");
=======
    m_log.Emsg("Attach", "Creating new IO object for file ", io->Path());
    // Rec << time(NULL)  << " Attach " << io->Path() << std::endl;
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
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
<<<<<<< HEAD
    XrdSysMutexHelper lock(&m_io_mutex);
    m_attached--;

    aMsgIO(kDebug, io, "Cache::Detach(), deleting IO object. Attach count = %d", m_attached);

=======

    // AMT:: don't know why ~IO should be called from this class
    //        why not directly in IO::Detach() ???
    XrdSysMutexHelper lock(&m_io_mutex);
    m_attached--;

    fflush(Rec);
    std::stringstream ss; ss << m_attached << " " << io->Path();
    if (Dbg) m_log.Emsg("Detach", "deleting io object ", ss.str().c_str() );
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2

    delete io;
}


