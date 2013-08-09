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
class XrdClient;
namespace XrdFileCache
{

class Prefetch {

    friend class IO;

    struct Task {
        long long m_offset;
        int m_size;
        XrdSysCondVar* m_condVar;

Task(long long iOff = 0, int iSize = s_buffer_size, XrdSysCondVar* iCondVar = 0):m_offset(iOff), m_size(iSize), m_condVar(iCondVar) {}
        ~Task() {}
        void Dump();
    };

public:

    Prefetch(XrdOss& outputFS, XrdOucCacheIO & inputFile, std::string& path);
    ~Prefetch();

    void Run();
    void Join();

    void AddTask(long long offset, int size, XrdSysCondVar* cond);

    bool GetStatForRng(long long offset, int size, int& pulled);

    static const size_t s_buffer_size;
protected:

    ssize_t Read(char * buff, off_t offset, size_t size);
    void CloseCleanly();

private:

// inline off_t GetOffset() {return __sync_fetch_and_or(&m_offset, 0); }
    bool GetNextTask(Task&);


    bool Open();
    bool Close();
    bool Fail(bool cleanup);


    // file
    XrdOss  &m_output_fs;
    XrdOssDF *m_output;
    XrdOucCacheIO & m_input;
    std::string     m_temp_filename;
    std::vector<bool> m_download_status;
    std::queue<Task> m_tasks_queue;

    bool m_started;
    bool m_finalized;
    bool m_stop;

    int m_numMissBlock;
    int m_numHitBlock;

    XrdSysCondVar m_stateCond;
    XrdSysMutex   m_downloadStatusMutex;
    XrdSysMutex   m_quequeMutex;

};

}
#endif
