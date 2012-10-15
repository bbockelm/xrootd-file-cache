
#include "IO.hh"
#include "Cache.hh"
#include "Prefetch.hh"

#include "XrdClient/XrdClientConst.hh"

using namespace XrdFileCache;

IO::IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache & cache, PrefetchPtr pread)
    : m_io(io),
      m_stats(stats),
      m_prefetch(pread),
      m_cache(cache)
{}

XrdOucCacheIO *
IO::Detach()
{
    XrdOucCacheIO * io = &m_io;
    m_cache.Detach(this); // This will delete us!
    return io;
}

/*
 * Read from the cache; prefer to read from the Prefetch object, if possible.
 */
int IO::Read (char *buff, long long off, int size)
{
    ssize_t bytes_read = 0;
    ssize_t retval = 0;
    if (m_prefetch)
    {
        retval = m_prefetch->Read(buff, off, size);
        if (retval > 0)
        {
            bytes_read += retval;
            buff += retval;
            size -= retval;
        }
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
    for (int i=0; i<n; i++)
    {
        XrdSfsXferSize size = readV[i].size;
        char * buff = readV[i].data;
        XrdSfsFileOffset off = readV[i].offset;
        if (m_prefetch)
        {
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

