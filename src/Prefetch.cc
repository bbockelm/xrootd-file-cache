
#include <vector>
#include <stdio.h>
#include <sstream>
#include <fcntl.h>

#include "Prefetch.hh"
#include "Factory.hh"
#include "Cache.hh"
#include "Context.hh"

#include <XrdCl/XrdClFile.hh>
#include <XrdCl/XrdClXRootDResponses.hh>
//#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOuc/XrdOucEnv.hh"

using namespace XrdFileCache;

const size_t Prefetch::s_buffer_size = 128*1024;

Prefetch::Prefetch(XrdOss &outputFS, XrdOucCacheIO &inputIO, std::string& disk_file_path)
    : m_output_fs(outputFS),
      m_output(NULL),
      m_input(inputIO),
      m_temp_filename(disk_file_path),
      m_started(false),
      m_finalized(false),
      m_stop(false),
      m_numMissBlock(0),
      m_numHitBlock(0),
      m_stateCond(0) // We will explicitly lock the condition before use.
{
    aMsgIO(kDebug, &m_input, "Prefetch::Prefetch()");
}

Prefetch::~Prefetch()
{ 
    aMsgIO(kDebug, &m_input, "Prefetch::~Prefetch() destroying Prefetch Object");
    aMsgIO(kInfo, &m_input, "Prefetch::~Prefetch() hit[%d] miss[%d]",  m_numHitBlock, m_numMissBlock);

    Join();

    aMsgIO(kDebug, &m_input, "Prefetch::~Prefetch close disk file");
    m_output->Close();
    delete m_output;
    m_output = NULL;


    aMsgIO(kDebug, &m_input, "Prefetch::~Prefetch close Info file");
    m_cfi.write(m_infoFile);
    if (Dbg <= kDump)  m_cfi.print();
    m_infoFile->Close();
    delete m_infoFile;
    m_infoFile = NULL;
}

void
Prefetch::CloseCleanly()
{
    XrdSysCondVarHelper monitor(m_stateCond);
    if (m_started && !m_finalized)
        m_stop = true;
}
//_________________________________________________________________________________________________
void
Prefetch::Run()
{
    if (!Open())
        return;

    aMsgIO(kDebug, &m_input, "Prefetch::Run()");

    std::vector<char> buff;
    buff.reserve(s_buffer_size);
    int retval = 0;

    Task task;
    while (GetNextTask(task))
    {
        task.Dump();
        aMsgIO(kDebug, &m_input, "Prefetch::Run() new task block[%d, %d], condVar [%c]", task.firstBlock, task.lastBlock, task.condVar ? 'x': 'o');
        for (int block = task.firstBlock; block <= task.lastBlock; block++)
        {
            bool already;
            m_downloadStatusMutex.Lock();
            already = m_cfi.testBit(block);
            m_downloadStatusMutex.UnLock();
            if (already) {
                // this happens in the case of queue
                aMsgIO(kDebug, &m_input, "Prefetch::Run() already done, continue ...");
                continue;
            } else {
                aMsgIO(kDebug, &m_input, "Prefetch::Run() download block [%d]", block);
            }
 
            long long offset = block * s_buffer_size;
            retval = m_input.Read(&buff[0], offset, s_buffer_size);
            aMsgIO(kDebug, &m_input, "Prefetch::Run() retval %d for block %d", block, retval);
             // write in disk file
            int buffer_remaining = retval;
            int buffer_offset = 0;

            while ((buffer_remaining > 0) && // There is more to be written
                   (((retval = m_output->Write(&buff[buffer_offset], offset + buffer_offset, buffer_remaining)) != -1) 
                    || (errno == EINTR)))  // Write occurs without an error
            {
                buffer_remaining -= retval;
                buffer_offset += retval;
            }
            if (retval < 0)
            {
                break;
            }

            m_downloadStatusMutex.Lock();
            m_cfi.setBit(block);
            if (task.condVar)
                m_numMissBlock++;
            else
                m_numHitBlock++;
            m_downloadStatusMutex.UnLock();

        }

        aMsgIO(kDebug, &m_input, "Prefetch::Run() task completed ");
        task.Dump();
        if (task.condVar)
        {
            aMsgIO(kDebug, &m_input, "Prefetch::Run() task *Signal* begin");
            XrdSysCondVarHelper(*task.condVar);
            task.condVar->Signal();
            aMsgIO(kDebug, &m_input, "Prefetch::Run() task *Signal* end");
        }


        // after completeing a task, check if IO wants to break
        if (m_stop)
        {
            aMsgIO(kDebug, &m_input, "Prefetch::Run() %s", "stopping for a clean cause");
            retval = -EINTR;
            break;
        }

    }
    aMsgIO(kDebug, &m_input, "Prefetch::Run() close !");
    Close();
}

void
Prefetch::Join()
{
    aMsgIO(kDebug, &m_input, "Prefetch::Join() going to lock");

    XrdSysCondVarHelper monitor(m_stateCond);
    if (m_finalized)
    {
        aMsgIO(kDebug, &m_input, "Prefetch::Join() already finalized");
        return;
    }
    else if (m_started)
    {
        aMsgIO(kDebug, &m_input, "Prefetch::Join() waiting");
        m_stateCond.Wait();
    }
    else
    {
        aMsgIO(kDebug, &m_input, "Prefetch::Join() not started - running it before Joining");
        monitor.UnLock();
        // Because we have unlocked the mutex, someone else may be
        // able to race us and Run - causing us to exit early.
        // Hence, we call Join again without the mutex held.
        Run();
        Join();
    }
}


bool
Prefetch::Open()
{
    XrdSysCondVarHelper monitor(m_stateCond);
    if (m_started)
    {
        return false;
    }
    m_started = true;
    // Finalize temporary turned on in case of exception.
    m_finalized = true;


    aMsgIO(kDebug, &m_input, "Prefetch::Open() open file for disk cache");


    // Create the data file itself.
    XrdOucEnv myEnv;
    m_output_fs.Create(Factory::GetInstance().GetUsername().c_str(), m_temp_filename.c_str(), 0600, myEnv, XRDOSS_mkpath);
    m_output = m_output_fs.newFile(Factory::GetInstance().GetUsername().c_str());
    if (!m_output || m_output->Open(m_temp_filename.c_str(), O_RDWR, 0600, myEnv) < 0)
    {
        aMsgIO(kError, &m_input, "Prefetch::Open() fail to get data-FD");
        return false;
    }

    // Create the info file
    std::string ifn = m_temp_filename + ".cinfo";
    m_output_fs.Create(Factory::GetInstance().GetUsername().c_str(), ifn.c_str(), 0600, myEnv, XRDOSS_mkpath);
    m_infoFile = m_output_fs.newFile(Factory::GetInstance().GetUsername().c_str());
    if (!m_infoFile || m_infoFile->Open(ifn.c_str(), O_RDWR, 0600, myEnv) < 0) 
    {
        aMsgIO(kError, &m_input, "Prefetch::Open() fail to get info-FD %s ", ifn.c_str());
        return false;
    }
    if ( m_cfi.read(m_infoFile) <= 0)
    {
        int ss = (m_input.FSize() -1)/s_buffer_size + 1;
        aMsgIO(kError, &m_input, "Creating new file info. Reserve space for %d blocks", ss);
        m_cfi.resizeBits(ss);
    }
    else
    {
        aMsgIO(kError, &m_input, "Info file already exists");
        if (Dbg <= kDump) m_cfi.print();
    }

    m_finalized = false;
    return true;
}

bool
Prefetch::Close()
{
    aMsgIO(kInfo, &m_input, "Prefetch::Close()");
    XrdSysCondVarHelper monitor(m_stateCond);
    if (!m_started)
    {
        return false;
    }

    if (m_output)
    {
        // AMT create a file with cinfo extension, to mark file has completed
        //
        if (m_started && !m_stop)
        {
            std::stringstream ss;
            aMsgIO(kInfo, &m_input, "Prefetch::Close() creating info file");
            ss << "touch " <<  m_temp_filename << ".cinfo";
            system(ss.str().c_str());
        }

    }

    m_stateCond.Broadcast();
    m_finalized = true;

    return false; // Fail until this is implemented.
}

bool
Prefetch::Fail(bool cleanup)
{
    // Prefetch did not competed download.
    // Remove cached file.

    XrdSysCondVarHelper monitor(m_stateCond);
    aMsgIO(kWarning, &m_input, "Prefetch::Fail() cleanup = %d, stated = %d, finalised = %d", cleanup, m_finalized, m_started);

    if (m_finalized)
        return false;
    if (!m_started)
        return false;

    if (m_output)
    {
        aMsgIO(kWarning, &m_input, "Prefetch::Fail() close output.");
        m_output->Close();
        delete m_output;
    }

    if (cleanup && !m_temp_filename.empty())
        m_output_fs.Unlink(m_temp_filename.c_str());

    m_stateCond.Broadcast();
    m_finalized = true;
    return true;
}

//______________________________________________________________________________
void
Prefetch::Task::Dump()
{
    aMsg(kDebug, "Task firstBlock = %d, lastBlock =  %d,  cond = %p", firstBlock, lastBlock, (void*)condVar);
}

bool
Prefetch::GetStatForRng(long long offset, int size, int& pulled)
{
    int first_block = offset / s_buffer_size;
    int last_block  =  (offset + size -1)/ s_buffer_size;
    int nblocks     = last_block - first_block + 1;

    int requireTask = false;

    m_downloadStatusMutex.Lock();
    if (m_cfi.isComplete()) 
    {
        pulled = nblocks;
        requireTask = false;
    }
    else {
        pulled = 0;
        for (int i = first_block; i <= last_block; ++i)
        {
            pulled += m_cfi.testBit(i);        
        }
        requireTask = (pulled < nblocks);
    }
    m_downloadStatusMutex.UnLock();

    aMsgIO(kInfo, &m_input, "Prefetch::GetStatForRng() bolcksPulled[%d] needed[%d]", pulled, nblocks);

    return requireTask;
}

void
Prefetch::AddTaskForRng(long long offset, int size, XrdSysCondVar* cond)
{
    aMsgIO(kDebug, &m_input, "Prefetch::AddTask %lld %d cond= %p", offset, size, (void*)cond);
    m_downloadStatusMutex.Lock();
    int first_block = offset / s_buffer_size;
    int last_block  = (offset + size -1)/ s_buffer_size;
    m_tasks_queue.push(Task(first_block, last_block, cond)); 
    m_downloadStatusMutex.UnLock();
}

bool
Prefetch::GetNextTask(Task& t )
{
    bool res = false;
    m_quequeMutex.Lock();
    if (m_tasks_queue.empty())
    {
        // give block-attoms which has not been downloaded from beginning to end
        m_downloadStatusMutex.Lock();
        for (int i = 0; i < m_cfi.getSizeInBits(); ++i)
        {
            if (m_cfi.testBit(i) == false) 
            {
                t.firstBlock = i;
                t.lastBlock = t.firstBlock + 1;
                t.condVar = 0;

                aMsgIO(kDebug, &m_input, "Prefetch::GetNextTask() read first undread block");
                res = true;
                break;
            }
        }

        m_downloadStatusMutex.UnLock();
    }
    else
    {
        aMsgIO(kDebug, &m_input, "Prefetch::GetNextTask() from queue");
        t = m_tasks_queue.front();
        m_tasks_queue.pop();
        res = true;
    }
    m_quequeMutex.UnLock();

    return res;
}


//______________________________________________________________________________



ssize_t
Prefetch::Read(char *buff, off_t offset, size_t size)
{
    aMsgIO(kDebug, &m_input, "Prefetch::Read()  started (1)!!!");

    ssize_t res =  m_output->Read(buff, offset, size);    

    return res;
}
