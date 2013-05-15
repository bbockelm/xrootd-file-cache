#include "IO.hh"
#include "Cache.hh"
#include "Prefetch.hh"
#include "Context.hh"
#include "File.hh"

#include <sstream>
#include <stdio.h>

#include "XrdClient/XrdClientConst.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSfs/XrdSfsInterface.hh"


using namespace XrdFileCache;

IO::IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache & cache, FilePtr file, XrdSysError &log)
    : m_io(io),
      m_stats(stats),
      m_file(file),
      m_cache(cache),
      m_log(log)
{}

XrdOucCacheIO *
IO::Detach()
{
   if (Dbg > 1) m_log.Emsg("IO", "Detach ", m_io.Path());
   fflush(stdout);
    XrdOucCacheIO * io = &m_io;
    if (m_file.get())
    {
        // AMT maybe don't have to do this here but automatically in destructor
        //  is valid io still needed for destruction? if not than nothing has to be done here 
        m_file.reset();
    }
    m_cache.Detach(this); // This will delete us!
    return io;
}

/*
 * Read from the cache; prefer to read from the Prefetch object, if possible.
 */
int IO::Read (char *buff, long long off, int size)
{
    std::stringstream ss; ss << "Read " << off << "@" << size;
    m_log.Emsg("IO", ss.str().c_str());
    ssize_t bytes_read = 0;
    ssize_t retval = 0;


    if (m_file.get())
    {
        retval = m_file->Read(m_stats, buff, off, size);
        printf("XfcFile read return val [%d] ...... \n", retval);
    } 


    if (retval > 0)
    {
        bytes_read += retval;
        buff += retval;
        size -= retval;
    }


    if ((size > 0))
    {
        retval = m_io.Read(buff, off, size);
        printf("XfcFile read ORIG return val [%d] ...... \n", retval);
        if (retval > 0) bytes_read += retval;
    }
    return (retval < 0) ? retval : bytes_read;

}

/*
 * Perform a readv from the cache
 */
#if defined(HAVE_READV)

int IO::ReadV (const XrdOucIOVec *readV, int n)
{

   printf("AMT NOT YET IMPLEMNETED !!!!\n");
   return 0;
}
#endif

