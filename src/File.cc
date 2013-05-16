
#include <stdio.h>
#include <sstream>
#include <fcntl.h>

#include "File.hh"
#include "IO.hh"
#include "Prefetch.hh"
#include "Factory.hh"
#include "Cache.hh"
#include "Context.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSfs/XrdSfsInterface.hh"
using namespace XrdFileCache;

File::File(XrdSysError &log, XrdOss& outputFS, XrdOucCacheIO & inputFile):
    m_diskDF(0),
    m_prefetch(0),
    m_log(log)
{
    m_log.Emsg("construction xcf File ", inputFile.Path());

    // look for file on disk first
    XrdOucEnv myEnv;
    std::string fname;
    Cache::getFilePathFromURL(inputFile.Path(), fname);
    fname = Factory::GetInstance().GetTempDirectory() + fname;
    int test = -1;  // test is existance of cinfo file
    {
        std::string chkname = fname + ".cinfo";
        XrdOucEnv myEnv;
        XrdOssDF* testfd = Factory::GetInstance().GetOss()->newFile(Factory::GetInstance().GetUsername().c_str());
        test = testfd->Open(chkname.c_str(), O_RDONLY, 0600, myEnv);
    }
    if (test >= 0 )
    {
        m_diskDF = Factory::GetInstance().GetOss()->newFile(Factory::GetInstance().GetUsername().c_str());
        if ( m_diskDF  && m_diskDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv))
        {
	   if (Dbg) m_log.Emsg("File, ", "read from disk");
            m_diskDF->Open(fname.c_str(), O_RDONLY, 0600, myEnv);
        }
    }

    // create prefetch 
    if (m_diskDF  == 0 || m_diskDF && m_diskDF->getFD() <= 0)
    {
       if (Dbg) m_log.Emsg("xcfFile ", "Create Prefetch");
        m_prefetch = new XrdFileCache::Prefetch(log, outputFS, inputFile);
    }
}

File::~File()
{
    if (m_prefetch)
    {
        m_prefetch->CloseCleanly();
    }

    delete m_prefetch;

    // AMT:: how diskDF is closed ???

}


int File::Read (XrdOucCacheStats &/*Now*/, char *buff, long long off, int size)
{
    return 0;
    if (m_prefetch)
    {
    m_log.Emsg("File, ", "Read from Prefetch.");
        return  m_prefetch->Read(buff, off, size);
    }
    else
    {
    m_log.Emsg("File, ", "Read from disk.");
        return m_diskDF->Read(buff, off, size);
    }
}
