
#include "IO.hh"
#include "Cache.hh"
#include "Prefetch.hh"

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

