#include "IO.hh"
#include "Cache.hh"
#include "Prefetch.hh"

#include <sstream>
#include <stdio.h>

#include "XrdClient/XrdClientConst.hh"
#include "XrdSys/XrdSysError.hh"

using namespace XrdFileCache;

IO::IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache & cache, PrefetchPtr pread, XrdSysError &log)
    : m_io(io),
      m_stats(stats),
      m_prefetch(pread),
      m_cache(cache),
      m_log(log)
{}

XrdOucCacheIO *
IO::Detach()
{
    XrdOucCacheIO * io = &m_io;
    if (m_prefetch.get())
    {
        m_prefetch->CloseCleanly();
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

    if (m_cache.readFromDisk())
    {
       m_log.Emsg("IO", ">>> read from disk");
       retval = m_cache.getCachedFile()->Read(buff, off, size);
    }
    else if (m_prefetch)
    {
       if (m_prefetch->hasCompletedSuccessfully())
       {
          m_log.Emsg("IO", ">>> open file from disk and read from it");
          m_cache.checkDiskCache(&m_io);
          retval = m_cache.getCachedFile()->Read(buff, off, size);
       }
       else
       {
          m_log.Emsg("IO", ">>> read from Prefetch");
          retval = m_prefetch->Read(buff, off, size);     
       }
    }

    if (retval > 0)
    {
       bytes_read += retval;
       buff += retval;
       size -= retval;
    }


    if ((size > 0) && ((retval = m_io.Read(buff, off, size)) > 0))
    {
            bytes_read += retval;
    }
    return (retval < 0) ? retval : bytes_read;
}

/*
 * Perform a readv from the cache
 */
#if defined(HAVE_READV)
ssize_t IO::ReadV (const XrdSfsReadV *readV, size_t n)
{
    ssize_t bytes_read = 0;
    size_t missing = 0;
    XrdSfsReadV missingReadV[READV_MAXCHUNKS];
    for (size_t i=0; i<n; i++)
    {
        XrdSfsXferSize size = readV[i].size;
        char * buff = readV[i].data;
        XrdSfsFileOffset off = readV[i].offset;\
        std::stringstream ss; ss << "ReadV " << off << "@" << size;
        m_log.Emsg("IO", ss.str().c_str());
        if (m_prefetch)
        { 
       	  m_log.Emsg("IO","Trying to read from prefetch.");
           ssize_t retval = m_prefetch->Read(buff, off, size);
           if ((retval > 0) && (retval == size))
           {
               // TODO: could handle partial reads here
               bytes_read += size;
               continue;
           }
        }
        missingReadV[missing].size = size;
        missingReadV[missing].data = buff;
        missingReadV[missing].offset = off;
        missing ++;
        if (missing >= READV_MAXCHUNKS)
        {   // Something went wrong in construction of this request;
            // Should be limited in higher layers to a max of 512 chunks.
            return -1;
        }
    }
    if (missing)
    { 
        m_log.Emsg("IO","ReadV missing.");
        ssize_t retval = m_io.ReadV(missingReadV, missing);
        if (retval >= 0)
        {
            return retval + bytes_read;
        }
        else
        {
            return retval;
        }
    }
    return bytes_read;
}
#endif

