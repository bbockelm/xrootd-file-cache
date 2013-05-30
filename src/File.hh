#ifndef __XRDFILECACHE_FILE_HH__
#define __XRDFILECACHE_FILE_HH__

#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucCache.hh>
#include "XrdSys/XrdSysError.hh"

namespace XrdFileCache
{
class Prefetch;

class File
{
public:
    File(XrdSysError &log, XrdOss& outputFS, XrdOucCacheIO & inputFile);
    virtual ~File();

    Prefetch*
    GetPrefetch() { return m_prefetch; }
    int Read (XrdOucCacheStats &Now, char *Buffer, long long Offs, int Length);


private:
    XrdOssDF* m_diskDF;
    Prefetch* m_prefetch;
    XrdSysError m_log;
};

}
#endif
