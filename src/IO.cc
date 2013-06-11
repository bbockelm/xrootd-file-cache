#include "IO.hh"
#include "Cache.hh"
#include "Prefetch.hh"
#include "Context.hh"

#include "Factory.hh"

#include <sstream>
#include <stdio.h>
#include <fcntl.h>

#include "XrdClient/XrdClientConst.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"


using namespace XrdFileCache;

void *
PrefetchRunner(void * prefetch_void)
{
    Prefetch *prefetch = static_cast<Prefetch *>(prefetch_void);
    if (prefetch)
        prefetch->Run();
    return NULL;
}

IO::IO(XrdOucCacheIO &io, XrdOucCacheStats &stats, Cache & cache,  XrdSysError &log)
    : m_io(io),
      m_stats(stats),
      m_cache(cache),
      m_diskDF(0),
      m_log(log)
{

    // check file is completed downloaing
    XrdOucEnv myEnv;
    std::string fname;
    getFilePathFromURL(io.Path(), fname);
    fname = Factory::GetInstance().GetTempDirectory() + fname;
    int test_open = -1;  
    // test is existance of cinfo file
    {
        std::string chkname = fname + ".cinfo";
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
	  if (Dbg) m_log.Emsg("Attach, ", "read from disk");
	  Rec << time(NULL) << " disk " << fname << std::endl;
	  m_diskDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv);
	}
    }
}

XrdOucCacheIO *
IO::Detach()
{
    if (Dbg > 1) m_log.Emsg("IO", "Detach ", m_io.Path());
    fflush(stdout);
    XrdOucCacheIO * io = &m_io;
    if (m_prefetch.get())
    {
        // AMT maybe don't have to do this here but automatically in destructor
        //  is valid io still needed for destruction? if not than nothing has to be done here
        m_prefetch.reset();
    }
    m_cache.Detach(this); // This will delete us!
    return io;
}

/*
 * Read from the cache; prefer to read from the Prefetch object, if possible.
 */
int
IO::Read (char *buff, long long off, int size)
{
    std::stringstream ss; ss << "Read " << off << "@" << size;
    if (Dbg) m_log.Emsg("IO", ss.str().c_str());
    ssize_t bytes_read = 0;
    ssize_t retval = 0;

    if (m_prefetch)
    {
        if (Dbg > 1) m_log.Emsg("IO, ", "Read from Prefetch.");
        retval = m_prefetch->Read(buff, off, size);
    }
    else
    {
        if (Dbg > 1) m_log.Emsg("File, ", "Read from disk.");
        retval = m_diskDF->Read(buff, off, size);
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
        // printf(" read ORIG return val [%d] ...... \n", retval);

        if (Dbg > 1) m_log.Emsg("IO", "Read from origin server.");
        if (retval > 0) bytes_read += retval;
    }
    if (retval <= 0)
    {
        ss.clear(); ss << "[" << retval << "]";
        m_log.Emsg("IO", "Read error, bytes read ", ss.str().c_str());
    }
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
