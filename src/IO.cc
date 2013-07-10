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

 

using namespace XrdFileCache;

class XfcStats : public XrdOucCacheStats
{
public:
   long long    BytesGetPrefetch;
   long long    BytesGetDisk;
   int          HitsPrefetch;
   int          HitsDisk;

   inline void AddStat(XfcStats &Src)
   {
      XrdOucCacheStats::Add(Src);

      sMutex1.Lock();
      BytesGetPrefetch += Src.BytesGetPrefetch;
      BytesGetDisk     += Src.BytesGetDisk;

      HitsPrefetch += Src.HitsPrefetch;
      HitsDisk     += Src.HitsDisk;

      sMutex1.UnLock();
   }

   XfcStats() : BytesGetPrefetch(0),BytesGetDisk(0),HitsPrefetch(0), HitsDisk(0){}
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

IO::IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache & cache)
    : m_io(io),
      m_statsGlobal(stats),
      m_stats(0),
      m_cache(cache),
      m_diskDF(0)
{
    m_stats = new XfcStats();

    // test is existance of *.cinfo file which assert the file has
    // been completely downloaded
    XrdOucEnv myEnv;
    std::string fname;
    getFilePathFromURL(io.Path(), fname);
    fname = Factory::GetInstance().GetTempDirectory() + fname;
    int test_open = -1;
    std::string chkname = fname + ".cinfo";
    {
        XrdOucEnv myEnv;
        XrdOssDF* testFD  = Factory::GetInstance().GetOss()->newFile(Factory::GetInstance().GetUsername().c_str());
        test_open = testFD->Open(chkname.c_str(), O_RDONLY, 0600, myEnv);
    }

    if (test_open < 0 )
    {
        m_prefetch =   Factory::GetInstance().GetPrefetch(io, fname);
        pthread_t tid;
        XrdSysThread::Run(&tid, PrefetchRunner, (void *)(m_prefetch.get()), 0, "XrdFileCache Prefetcher");
    }
    else {
        m_diskDF = Factory::GetInstance().GetOss()->newFile(Factory::GetInstance().GetUsername().c_str());
        if ( m_diskDF && m_diskDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv))
        {
            aMsgIO(kInfo, &m_io, "IO::Attach(), read from disk.");
            m_diskDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv);
        }
    }


    // update mtime on both files
    utime(chkname.c_str(), NULL);
    utime(fname.c_str(), NULL);
}

IO::~IO() 
{
   m_statsGlobal.Add(*m_stats);
   delete m_stats;
}

XrdOucCacheIO *
IO::Detach()
{
    long long bytesWrite = m_prefetch.get() ? m_prefetch->GetOffset() : -1;
    char Hbuf[512];
    char Bbuf[512];
    sprintf(Hbuf, "NumHits[%d] NumHitsPrefetch[%d] NumHitsDisk[%d] NumMissed[%d] ",
            m_stats->Hits,
            m_stats->HitsPrefetch,
            m_stats->HitsDisk,
            m_stats->Miss
            );

    sprintf(Bbuf, "bytes = BytesGet[%lld] BytesGetPrefetch[%lld] BytesGetDisk[%lld] BytesPass[%lld] BytesWrite[%lld]",
            m_stats->BytesGet,
            m_stats->BytesGetPrefetch,
            m_stats->BytesGetDisk,
            m_stats->BytesPass,
            bytesWrite
            );


    aMsgIO(kInfo, &m_io, "IO::Detach() %s %s", &Hbuf[0], &Bbuf[0]);
    XrdOucCacheIO * io = &m_io;
    if (m_prefetch.get())
    {
       // AMT maybe don't have to do this here but automatically in destructor
       //  is valid io still needed for destruction? if not than nothing has to be done here
       m_prefetch.reset();
    }
    else
    {
       m_diskDF->Close();
    }

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
    aMsgIO(kDebug, &m_io, "IO::Read() aMsgIO(kDebug, &m_io, %lld@%d", off, size);
    ssize_t bytes_read = 0;
    ssize_t retval = 0;
    XfcStats stat_tmp;
    if (m_prefetch)
    {
        aMsgIO(kDebug, &m_io, "IO::Read() use Prefetch");
        retval = m_prefetch->Read(buff, off, size);
        if (retval > 0) {
            stat_tmp.HitsPrefetch = 1;
            stat_tmp.BytesGetPrefetch = retval;
        }
    }
    else
    {
        aMsgIO(kDebug, &m_io, "IO::Read() use disk");
        retval = m_diskDF->Read(buff, off, size);
        if (retval > 0) {
            stat_tmp.HitsDisk = 1;
            stat_tmp.BytesGetDisk = retval;
        }
    }


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
        aMsgIO(kDebug, &m_io, "IO::Read(). Trying to read %d from origin", size);

        retval = m_io.Read(buff, off, size);

        // statistic
        stat_tmp.BytesPass = retval;
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
