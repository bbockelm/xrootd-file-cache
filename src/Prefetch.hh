#ifndef __XRDFILECACHE_PREFETCH_HH__
#define __XRDFILECACHE_PREFETCH_HH__
/*
 * A simple prefetch class.
 */
#include <string.h>
#include <string>
#include <XrdSys/XrdSysPthread.hh>
#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucCache.hh>

#include "XrdFileCacheFwd.hh"
class XrdClient;
namespace XrdFileCache
{

class Prefetch {

    friend class IO;

public:

   Prefetch(XrdOss& outputFS, XrdOucCacheIO & inputFile, std::string& path);
    ~Prefetch();

    void Run();
    void Join();

protected:

    ssize_t Read(char * buff, off_t offset, size_t size);
    void CloseCleanly();

private:

    inline off_t
    GetOffset() {return __sync_fetch_and_or(&m_offset, 0); }

    XrdOss & m_output_fs;

    XrdOssDF *m_output;

    XrdOucCacheIO & m_input;

    XrdClient* m_xrdClient;

    off_t m_offset;
    static const size_t m_buffer_size;
    bool m_started;
    bool m_finalized;
    bool m_stop;
    XrdSysCondVar m_cond;
    std::string m_temp_filename;

    bool Open();
    bool Close();
    bool Fail(bool cleanup);

};

}
#endif
