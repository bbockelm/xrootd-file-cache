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

<<<<<<< HEAD
IO::IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache & cache)
    : m_io(io),
      m_statsGlobal(stats),
      m_stats(0),
      m_cache(cache),
      m_diskDF(0)
=======
namespace
{
    time_t eNow;
int Time(char *tbuff)
{
    const int minblen = 24;
    eNow = time(0);
    struct tm tNow;
    int i;

// Format the header
//
   tbuff[minblen-1] = '\0'; // tbuff must be at least 24 bytes long
   localtime_r((const time_t *) &eNow, &tNow);
   i =    snprintf(tbuff, minblen, "%02d%02d%02d %02d:%02d:%02d %03ld ",
                  tNow.tm_year-100, tNow.tm_mon+1, tNow.tm_mday,
                  tNow.tm_hour,     tNow.tm_min,   tNow.tm_sec,
                  XrdSysThread::Num());
   return (i >= minblen ? minblen-1 : i);
}
}


IO::IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache & cache,  XrdSysError &log)
    : m_io(io),
      m_statsGlobal(stats),
      m_cache(cache),
      m_diskDF(0),
      m_log(0, "IO_")
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
{
    m_stats = new XfcStats();

<<<<<<< HEAD
    // test is existance of *.cinfo file which assert the file has
    // been completely downloaded
=======
    m_log.logger(log.logger());
    m_log.SetPrefix(io.Path());

    // check file is completed downloaing
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
    XrdOucEnv myEnv;
    std::string fname;
    getFilePathFromURL(io.Path(), fname);
    fname = Factory::GetInstance().GetTempDirectory() + fname;
<<<<<<< HEAD
    int test_open = -1;
    std::string chkname = fname + ".cinfo";
=======
    int test_open = -1;  
    std::string chkname = fname + ".cinfo";
    // test is existance of cinfo file
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
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
<<<<<<< HEAD
        m_diskDF = Factory::GetInstance().GetOss()->newFile(Factory::GetInstance().GetUsername().c_str());
        if ( m_diskDF && m_diskDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv))
        {
            aMsgIO(kInfo, &m_io, "IO::Attach(), read from disk.");
            m_diskDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv);
        }
=======
      m_diskDF = Factory::GetInstance().GetOss()->newFile(Factory::GetInstance().GetUsername().c_str());
      if ( m_diskDF && m_diskDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv))
	{
	  m_diskDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv);
          if (Dbg) m_log.Emsg("Attach, ", fname.c_str(), "read from disk");
	}

      // update mtime on both files
      utime(chkname.c_str(), NULL);
      utime(fname.c_str(), NULL);
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
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


IO::~IO()
{
   m_statsGlobal.Add(m_stats);
}


XrdOucCacheIO *
IO::Detach()
{
<<<<<<< HEAD
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
=======
    if (Dbg > 1) m_log.Emsg("IO", "Detach ", m_io.Path());
    fflush(stdout);

    long long bytesWrite = m_prefetch.get() ? m_prefetch->GetOffset() : -1;
 
    char tbuf[64];
    Time(&tbuf[0]);
    char Hbuf[512];
    char Bbuf[512];
    sprintf(Hbuf, "NumHits[%d] NumHitsPrefetch[%d] NumHitsDisk[%d] NumMissed[%d] ",
            m_stats.Hits,
            m_stats.HitsPrefetch,
            m_stats.HitsDisk,
            m_stats.Miss
            );

    sprintf(Bbuf, "bytes = BytesGet[%lld] BytesGetPrefetch[%lld] BytesGetDisk[%lld] BytesPass[%lld] BytesWrite[%lld]",
            m_stats.BytesGet, 
            m_stats.BytesGetPrefetch, 
            m_stats.BytesGetDisk, 
            m_stats.BytesPass, 
            bytesWrite
            );

    m_log.Emsg("IO Detach", &Hbuf[0], &Bbuf[0]);

    fprintf(Rec, "%s %s %s %s\n", &tbuf[0], &Hbuf[0], &Bbuf[0], m_io.Path());
    fflush(Rec);

>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
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
<<<<<<< HEAD

    // This will delete us!
    m_cache.Detach(this); 
=======
    else
      {
	m_diskDF->Close();
      }
    m_cache.Detach(this); // This will delete us!
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
    return io;
}

/*
 * Read from the cache; prefer to read from the Prefetch object, if possible.
 */
int
IO::Read (char *buff, long long off, int size)
{
<<<<<<< HEAD
    aMsgIO(kDebug, &m_io, "IO::Read() aMsgIO(kDebug, &m_io, %lld@%d", off, size);
    ssize_t bytes_read = 0;
    ssize_t retval = 0;
    XfcStats stat_tmp;
=======
    std::stringstream ss; ss << "Read " << off << "@" << size;
    if (Dbg > 0) m_log.Emsg("IO", ss.str().c_str());
    ssize_t bytes_read = 0;
    ssize_t retval = 0;

    Stats stat;

>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
    if (m_prefetch)
    {
        aMsgIO(kDebug, &m_io, "IO::Read() use Prefetch");
        retval = m_prefetch->Read(buff, off, size);
        if (retval > 0) {
<<<<<<< HEAD
            stat_tmp.HitsPrefetch = 1;
            stat_tmp.BytesGetPrefetch = retval;
=======
           stat.HitsPrefetch = 1;
           stat.BytesGetPrefetch = retval;
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
        }
    }
    else
    {
        aMsgIO(kDebug, &m_io, "IO::Read() use disk");
        retval = m_diskDF->Read(buff, off, size);
<<<<<<< HEAD
        if (retval > 0) {
            stat_tmp.HitsDisk = 1;
            stat_tmp.BytesGetDisk = retval;
        }
=======

        if (retval != size) {
            m_log.Emsg("IO, ", "Read from Disk error.");
        }
        if (retval > 0) {
           stat.HitsDisk = 1;
           stat.BytesGetDisk = retval;
        }

>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
    }


    if (retval > 0)
<<<<<<< HEAD
    { 
        stat_tmp.BytesRead = retval;
        stat_tmp.Hits = 1;
        aMsgIO(kDebug, &m_io, "IO::Read() read hit %d", retval);

=======
    {
        // statistic
        stat.BytesRead = retval;
        stat.Hits = 1;
        ss.clear();
        ss << retval;
        m_log.Emsg("IO, ", "Read Hit! ", ss.str().c_str());
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
        bytes_read += retval;
        buff += retval;
        size -= retval;
    }


    if ((size > 0))
    {
        aMsgIO(kDebug, &m_io, "IO::Read(). Trying to read %d from origin", size);

        retval = m_io.Read(buff, off, size);

        // statistic
<<<<<<< HEAD
        stat_tmp.BytesPass = retval;
        stat_tmp.Miss = 1;

=======
        stat.BytesPass = retval;
        stat.Miss = 1;

        ss.clear(); ss << " Read from origin server  " << size;
        if (Dbg > 1) m_log.Emsg("IO", ss.str().c_str());
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2
        if (retval > 0) bytes_read += retval;
    }
    if (retval <= 0)
    {
        aMsgIO(kError, &m_io, "IO::Read(), origin bytes read %d", retval);
    }
<<<<<<< HEAD
=======

    m_stats.AddStat(stat);

    return (retval < 0) ? retval : bytes_read;
>>>>>>> bb8576b05d95c2cdb146836a89a728859bbab5a2

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
