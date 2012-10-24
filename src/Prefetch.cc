
#include <vector>
#include <stdio.h>
#include <sstream>
#include <fcntl.h>

#include "Prefetch.hh"
#include "Factory.hh"
#include "Cache.hh"

#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOuc/XrdOucEnv.hh"

using namespace XrdFileCache;

const size_t Prefetch::m_buffer_size = 64*1024;

Prefetch::Prefetch(XrdSysError &log, XrdOss &outputFS, XrdOucCacheIO &inputIO)
    : m_output_fs(outputFS),
      m_output(NULL),
      m_input(inputIO),
      m_offset(0),
      m_started(false),
      m_finalized(false),
      m_stop(false),
      m_cond(0), // We will explicitly lock the condition before use.
      m_log(0, "Prefetch_"),
      m_temp_filename("")
{
    m_log.logger(log.logger());
}

void
Prefetch::CloseCleanly()
{
    XrdSysCondVarHelper monitor(m_cond);
    if (m_started && !m_finalized)
        m_stop = true;
}

void
Prefetch::Run()
{
    if (!Open())
        return;

    m_log.Emsg("Run", "Beginning prefetch of ", m_input.Path());
    std::vector<char> buff;
    buff.reserve(m_buffer_size);

    int retval = 0;
    while (0 != (retval = m_input.Read(&buff[0], m_offset, m_buffer_size)))
    {
        if ((retval < 0) && (retval != -EINTR))
        {
            break;
        }

        int buffer_remaining = retval;
        int buffer_offset = 0;

       while ((buffer_remaining > 0) &&  // There is more to be written
              (((retval = m_output->Write(&buff[buffer_offset], m_offset, buffer_remaining)) != -1) || (errno == EINTR))) { // Write occurs without an error
            buffer_remaining -= retval;
            buffer_offset += retval;
            __sync_fetch_and_add(&m_offset, retval);
        }
        if (retval < 0)
            break;
        if (m_offset % (10*1024*1024) == 0)
        {
            std::stringstream ss;
            ss << "Prefetched " << (m_offset/(1024*1024)) << " MB";
            m_log.Emsg("Fetching", ss.str().c_str());
        }
        // Note we don't lock read-access, as this will only ever go from 0 to 1
        // Reading during a partial write is OK in this case.
        if (m_stop)
        {
            m_log.Emsg("Read", "Stopping for a clean close");
            retval = -EINTR;
            break;
        }
    }

    if (retval < 0) {
        m_log.Emsg("Read", retval, "Failure prefetching file");
        Fail(retval != -EINTR);
    }

    Close();
}

void
Prefetch::Join()
{
    XrdSysCondVarHelper monitor(m_cond);
    if (m_finalized)
    {
        m_log.Emsg("Join", "Prefetch is already finalized");
        return;
    }
    else if (m_started)
    {
        m_log.Emsg("Join", "Waiting until prefetch finishes");
        m_cond.Wait();
        m_log.Emsg("Join", "Prefetch finished");
    }
    else
    {
        m_log.Emsg("Join", "Prefetch not started - running it before Joining");
        monitor.UnLock();
        // Because we have unlocked the mutex, someone else may be
        // able to race us and Run - causing us to exit early.
        // Hence, we call Join again without the mutex held.
        Run();
        Join();
    }
}

bool
Prefetch::GetTempFilename(std::string &result)
{ 
   /*
    std::string path = m_input.Path();
    size_t split_loc = path.rfind("//");

    if (split_loc == path.npos)
        return false;

    size_t kloc = path.rfind("?");


    if (kloc == path.npos)
        return false;
   */
    Cache::getFilePathFromURL(m_input.Path(), result);
    std::string &tmp_directory = Factory::GetInstance().GetTempDirectory();
    result = tmp_directory + result + ".tmp";

    return true;
}

bool
Prefetch::Open()
{
    XrdSysCondVarHelper monitor(m_cond);
    if (m_started) {
        return false;
    }
    m_started = true;
    // Finalize temporary turned on in case of exception.
    m_finalized = true;

    std::string temp_path;

    if (!GetTempFilename(temp_path))
    {
        m_log.Emsg("Open", "Failed to create temporary filename for ", m_input.Path());
        return false;
    }
    m_log.Emsg("Open", ("Opening temp file " + temp_path).c_str(), " to prefetch file ", m_input.Path());

    // Create the file itself.
    XrdOucEnv myEnv;
   
    m_output_fs.Create(Factory::GetInstance().GetUsername().c_str(), temp_path.c_str(), 0600, myEnv, XRDOSS_mkpath);
    m_output = m_output_fs.newFile(Factory::GetInstance().GetUsername().c_str());
    if (!m_output || m_output->Open(temp_path.c_str(), O_WRONLY, 0600, myEnv) < 0)
    {
        return false;
    }

    // If the file is pre-existing, pick up from where we left off.
    struct stat fileStat;
    if (m_output->Fstat(&fileStat) == 0)
    {
        m_offset = fileStat.st_size;
    }

    m_finalized = false;
    return true;
}

bool
Prefetch::Close()
{
    XrdSysCondVarHelper monitor(m_cond);
    if (!m_started) {
        return false;
    }

    if (m_output)
    {
        m_log.Emsg("Close", "Close m_output");
        m_output->Close();
        delete m_output;
        m_output = NULL;
    }

    // final file has same name , except of missing '.tmp' extension
    std::string finalName = m_temp_filename.substr(0, m_temp_filename.size()-4);
    m_output_fs.Rename(m_temp_filename.c_str(), finalName.c_str());

    m_cond.Broadcast();
    m_finalized = true;

    return false; // Fail until this is implemented.
}

bool
Prefetch::Fail(bool cleanup)
{
    XrdSysCondVarHelper monitor(m_cond);
    if (m_finalized)
        return false;
    if (!m_started)
        return false;
   
    if (m_output)
    {
       m_log.Emsg("Fail", "Close m_output");
        m_output->Close();
        delete m_output;
        m_output = NULL;
    }
   
    if (cleanup && !m_temp_filename.empty())
        m_output_fs.Unlink(m_temp_filename.c_str());

    m_cond.Broadcast();
    m_finalized = true;
    return true;
}

Prefetch::~Prefetch()
{
    m_log.Emsg("Destructor", "Destroying Prefetch Object");
    Join();
}

ssize_t
Prefetch::Read(char *buff, off_t offset, size_t size)
{
    XrdSysCondVarHelper monitor(m_cond);
    if (!m_started || m_finalized) {
        errno = EBADF;
        return -errno;
    }
    // TODO: if the file has been finalized, we could read it from its final location

    std::stringstream ss;
    ss << "offset = " << offset;



    off_t prefetch_offset = GetOffset();
    if (prefetch_offset < offset)
    {
        m_log.Emsg("Read", "Offset below requested offset. Nothing to read.", ss.str().c_str());
        return 0;
    }
    else if (prefetch_offset >= static_cast<off_t>(offset + size))
    {
       ss << ", size  = " << size;
       m_log.Emsg("Read", "read complete size", ss.str().c_str());
       return m_output->Read(buff, offset, size);
    }
    else
    {
       size_t to_read = offset + size - prefetch_offset;
       ss << ", to_read  = " << to_read;

       m_log.Emsg("Read", "read partial read ", ss.str().c_str());
       return m_output->Read(buff, offset, to_read);
    }
}



bool
Prefetch::hasCompletedSuccessfully() const
{
   // AMT : this is temporary simplification.
   //       should consolidate m_started, m_finalized, and m_stop first

   return m_finalized == true;
}
