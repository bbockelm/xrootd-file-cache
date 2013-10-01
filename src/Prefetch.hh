#ifndef __XRDFILECACHE_PREFETCH_HH__
#define __XRDFILECACHE_PREFETCH_HH__
/*
 * A simple prefetch class.
 */

#include <string>
#include <vector>
#include <queue>

#include <XrdSys/XrdSysPthread.hh>
#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucCache.hh>

#include "XrdFileCacheFwd.hh"
#include "CacheFileInfo.hh"

class XrdClient;
namespace XrdFileCache
{
class CacheStats;

class Prefetch {

    friend class IO;

    struct Task {
       int firstBlock;
       int lastBlock;
       int cntFetched;
       XrdSysCondVar* condVar;

       Task(int fb = 0, int lb = 0, XrdSysCondVar* iCondVar = 0):
          firstBlock(fb), lastBlock(lb), cntFetched(0),
          condVar(iCondVar) {}

        ~Task() {}

        void Dump();
    };

public:

    Prefetch(XrdOss& outputFS, XrdOucCacheIO & inputFile, std::string& path);
    ~Prefetch();
    void Run();
    void Join();

    void AddTaskForRng(long long offset, int size, XrdSysCondVar* cond);

    bool GetStatForRng(long long offset, int size, int& pulled, int& nblocks);

protected:

   ssize_t Read(char * buff, off_t offset, size_t size, CacheStats&);
    void CloseCleanly();

private:

// inline off_t GetOffset() {return __sync_fetch_and_or(&m_offset, 0); }
    bool GetNextTask(Task&);


    bool Open();
    bool Close();
    bool Fail(bool cleanup);
    void RecordDownloadInfo();
    int getBytesToRead(Task& task, int block) const;

    // file
    XrdOss  &m_output_fs;
    XrdOssDF *m_output;
    XrdOssDF *m_infoFile;
    CacheFileInfo m_cfi;
    XrdOucCacheIO & m_input;
    std::string     m_temp_filename;
    std::queue<Task> m_tasks_queue;

    bool m_started;
    bool m_failed;
    bool m_stop;

    int m_numMissBlock;
    int m_numHitBlock;

    XrdSysCondVar m_stateCond;
    XrdSysMutex   m_downloadStatusMutex;
    XrdSysMutex   m_quequeMutex;

};

}
#endif
