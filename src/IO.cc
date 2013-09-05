#include "IO.hh"
#include "Cache.hh"
#include "Prefetch.hh"
#include "Context.hh"
#include "Factory.hh"

#include <stdio.h>
#include <fcntl.h>
#include <utime.h>

#include "XrdClient/XrdClientConst.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
 
#include <XrdSys/XrdSysPthread.hh>

using namespace XrdFileCache;

class XfcStats : public XrdOucCacheStats
{
public:
    long long    BytesCachedPrefetch;
    long long    BytesPrefetch;
    long long    BytesDisk;
    int          HitsPrefetch;
    int          HitsDisk;

    inline void AddStat(XfcStats &Src)
    {
        XrdOucCacheStats::Add(Src);

        sMutex1.Lock();
        BytesCachedPrefetch += Src.BytesCachedPrefetch;
        BytesPrefetch       += Src.BytesPrefetch;
        BytesDisk           += Src.BytesDisk;

        HitsPrefetch += Src.HitsPrefetch;
        HitsDisk     += Src.HitsDisk;

        sMutex1.UnLock();
    }

    XfcStats() :
        BytesCachedPrefetch(0), 
        BytesPrefetch(0),
        BytesDisk(0),
        HitsPrefetch(0), 
        HitsDisk(0){}


    void Dump()
    {
        aMsg(kError, "StatDump bCP = %lld, bP = %lld, bD =  %lld\n", BytesCachedPrefetch, BytesPrefetch, BytesDisk);
    }

private:
    XrdSysMutex sMutex1;

};

void *
PrefetchRunner(void * prefetch_void)
{
    Prefetch *prefetch = static_cast<Prefetch *>(prefetch_void);
    if (prefetch)
        prefetch->Run();
    return NULL;
}
//______________________________________________________________________________


IO::IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache & cache)
    : m_io(io),
      m_statsGlobal(stats),
      m_stats(0),
      m_cache(cache)
{
    m_stats = new XfcStats();

    std::string fname;
    getFilePathFromURL(io.Path(), fname);
    fname = Factory::GetInstance().GetTempDirectory() + fname;
    m_prefetch = Factory::GetInstance().GetPrefetch(io, fname);
    pthread_t tid;
    XrdSysThread::Run(&tid, PrefetchRunner, (void *)(m_prefetch.get()), 0, "XrdFileCache Prefetcher");
}

IO::~IO() 
{
   delete m_stats;
}

XrdOucCacheIO *
IO::Detach()
{
    m_statsGlobal.Add(*m_stats);
    aMsgIO(kInfo, &m_io, "IO::Detach() bPrefCh[%lld] bPref[%lld] bDisk[%lld] bError[%lld]", 
           m_stats->BytesCachedPrefetch,  m_stats->BytesPrefetch,m_stats->BytesDisk, m_stats->BytesPass);

    aMsgIO(kInfo, &m_io, "IO::Detach() hitPref[%d] hitDisk[%d] hitError[%d]",
           m_stats->HitsPrefetch,  m_stats->HitsDisk, m_stats->Miss);



    XrdOucCacheIO * io = &m_io;
    // AMT maybe don't have to do this here but automatically in destructor
    //  is valid io still needed for destruction? if not than nothing has to be done here
    m_prefetch->CloseCleanly();
    m_prefetch.reset();

    // This will delete us!
    m_cache.Detach(this); 
    return io;
}

/*
 * Read from the cache; prefer to read from the Prefetch object, if possible.
 */
int
IO::Read (char *buff, long long off, int size)
{
    aMsgIO(kDebug, &m_io, "IO::Read() %lld@%d", off, size);
    // return m_io.Read(buff, off, size);

    ssize_t bytes_read = 0;
    ssize_t retval = 0;
    XfcStats stat_tmp;
    
    int nbp;
    if ( m_prefetch->GetStatForRng(off, size, nbp))
    {
        XrdSysCondVar cond(0);
        m_prefetch->AddTaskForRng(off, size, &cond);
        {
            XrdSysCondVarHelper xx(cond);
            cond.Wait();
            aMsgIO(kDump, &m_io, "IO::Read() use prefetch, cond.Wait() finsihed.");
        }
    }
    else
    {
        aMsgIO(kDump, &m_io, "IO::Read() use Prefetch -- blocks already downlaoded.");
    }

    retval = m_prefetch->Read(buff, off, size);

    if (retval > 0) {
        stat_tmp.HitsPrefetch = 1;
        stat_tmp.BytesCachedPrefetch = nbp * Prefetch::s_buffer_size;
        if (stat_tmp.BytesCachedPrefetch > size) 
            stat_tmp.BytesCachedPrefetch = size;
        stat_tmp.BytesPrefetch = retval;
    }


    // now handle statistics and return value

    if (retval > 0)
    { 
        stat_tmp.BytesRead = retval;
        stat_tmp.Hits = 1;
        aMsgIO(kDebug, &m_io, "IO::Read() read hit %d", retval);

        bytes_read += retval;
        buff += retval;
        size -= retval;
    }


    if ((size > 0))
    {
        aMsgIO(kDebug, &m_io, "IO::Read() missed %d bytes", size);
        stat_tmp.BytesPass = size;
        stat_tmp.Miss = 1;
        if (retval > 0) bytes_read += retval;
    }

    if (retval <= 0)
    {
        aMsgIO(kError, &m_io, "IO::Read(), origin bytes read %d", retval);
    }

    m_stats->AddStat(stat_tmp);
    return (retval < 0) ? retval : bytes_read;
}

/*
 * Perform a readv from the cache
 */
#if defined(HAVE_READV)
int
IO::ReadV (const XrdOucIOVec *readV, int n)
{
   ssize_t bytes_read = 0;
   size_t missing = 0;
   XrdOucIOVec missingReadV[READV_MAXCHUNKS];
   for (size_t i=0; i<n; i++)
   {
      XrdSfsXferSize size = readV[i].size;
      char * buff = readV[i].data;
      XrdSfsFileOffset off = readV[i].offset;
      if (m_prefetch.get())
      {
         ssize_t retval = Read(buff, off, size);
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
      missing++;
      if (missing >= READV_MAXCHUNKS)
      { // Something went wrong in construction of this request;
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
   return 0;
}
#endif


bool
IO::getFilePathFromURL(const char* url, std::string &result)
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
