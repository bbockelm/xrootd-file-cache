
#include <vector>
#include <stdio.h>
#include <sstream>
#include <fcntl.h>

#include "Prefetch.hh"
#include "Factory.hh"
#include "Cache.hh"
#include "Context.hh"

#include <XrdClient/XrdClient.hh>
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOuc/XrdOucEnv.hh"
using namespace XrdFileCache;

const size_t Prefetch::m_buffer_size = 512*1024;

Prefetch::Prefetch(XrdSysError &log, XrdOss &outputFS, XrdOucCacheIO &inputIO, std::string& disk_file_path)
    : m_output_fs(outputFS),
      m_output(NULL),
      m_input(inputIO),
      m_xrdClient(0),
      m_offset(0),
      m_started(false),
      m_finalized(false),
      m_stop(false),
      m_cond(0), // We will explicitly lock the condition before use.
      m_log(0, "Prefetch_"),
      m_temp_filename(disk_file_path)
{
    m_log.logger(log.logger());
    m_log.SetPrefix(m_input.Path());

    m_xrdClient = new XrdClient(m_input.Path());

    if ( !m_xrdClient->Open(0, kXR_async) || m_xrdClient->LastServerResp()->status != kXR_ok)
    {
        m_log.Emsg("Constructor", "Client error ", m_input.Path());
    }
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

    if (Dbg) m_log.Emsg("Run", "Beginning prefetch of ", m_input.Path());
    std::vector<char> buff;
    buff.reserve(m_buffer_size);

    int retval = 0;
    // AMT
    while (0 != (retval = m_xrdClient->Read(&buff[0], m_offset, m_buffer_size)))
    {
        if ((retval < 0) && (retval != -EINTR))
        {
            break;
        }

        int buffer_remaining = retval;
        int buffer_offset = 0;

        while ((buffer_remaining > 0) && // There is more to be written
               (((retval = m_output->Write(&buff[buffer_offset], m_offset, buffer_remaining)) != -1) || (errno == EINTR)))  // Write occurs without an error
        {
            buffer_remaining -= retval;
            buffer_offset += retval;
            __sync_fetch_and_add(&m_offset, retval);
        }
        if (retval < 0)
        {
            break;
        }

        if (m_offset % (10*1024*1024) == 0)
        {
            std::stringstream ss;
            ss << "Prefetched " << (m_offset/(1024*1024)) << " MB";
            if (Dbg > 1) m_log.Emsg("Fetching", ss.str().c_str());
        }
        // Note we don't lock read-access, as this will only ever go from 0 to 1
        // Reading during a partial write is OK in this case.
        if (m_stop)
        {
            if (Dbg) m_log.Emsg("Run", "Stopping for a clean close");
            retval = -EINTR;
            break;
        }
    }

    if (retval < 0)
    {
        if (Dbg) m_log.Emsg("Run", retval, "Failure prefetching file");
        m_stop = true;
        Fail(retval != -EINTR);
    }

    Close();
}

void
Prefetch::Join()
{
    if (Dbg) m_log.Emsg("Join", "Going to lock ...");

    XrdSysCondVarHelper monitor(m_cond);
    if (m_finalized)
    {
        if (Dbg) m_log.Emsg("Join", "Prefetch is already finalized");
        return;
    }
    else if (m_started)
    {
        if (Dbg) m_log.Emsg("Join", "Waiting until prefetch finishes");
        m_cond.Wait();
        if (Dbg) m_log.Emsg("Join", "Prefetch finished");
    }
    else
    {
        if (Dbg) m_log.Emsg("Join", "Prefetch not started - running it before Joining");
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
    XrdSysCondVarHelper monitor(m_cond);
    if (m_started)
    {
        return false;
    }
    m_started = true;
    // Finalize temporary turned on in case of exception.
    m_finalized = true;


    if (Dbg) m_log.Emsg("Open", ("Opening temp file " + m_temp_filename ).c_str(), " to prefetch file ", m_input.Path());

    // Create the file itself.
    XrdOucEnv myEnv;

    m_output_fs.Create(Factory::GetInstance().GetUsername().c_str(), m_temp_filename.c_str(), 0600, myEnv, XRDOSS_mkpath);
    m_output = m_output_fs.newFile(Factory::GetInstance().GetUsername().c_str());
    if (!m_output || m_output->Open(m_temp_filename.c_str(), O_RDWR, 0777, myEnv) < 0)
    {
        m_log.Emsg("Open", "Failed to create temporary file ", m_temp_filename.c_str());
        return false;
    }

    // If the file is pre-existing, pick up from where we left off.
    struct stat fileStat;
    if (m_output->Fstat(&fileStat) == 0)
    {
        m_offset = fileStat.st_size;
        std::stringstream ss; ss << m_offset;
        if(m_offset) { if (Dbg) m_log.Emsg("Open", "Pickup where we left off. Offset = ", ss.str().c_str()); }
    }

    m_finalized = false;
    return true;
}

bool
Prefetch::Close()
{
    XrdSysCondVarHelper monitor(m_cond);
    if (!m_started)
    {
        return false;
    }

    if (m_output)
    {
        if (Dbg) m_log.Emsg("Close", "Close m_output");
        m_output->Close();
        delete m_output;
        m_output = NULL;

        // AMT create a file with cinfo extension, to mark file has completed
        //
        if (m_started && !m_stop)
        {
            std::stringstream ss;
            if (Dbg) m_log.Emsg("Close create info file", ss.str().c_str());
            ss << "touch " <<  m_temp_filename << ".cinfo";
            system(ss.str().c_str());
        }

    }

    m_cond.Broadcast();
    m_finalized = true;

    return false; // Fail until this is implemented.
}

bool
Prefetch::Fail(bool cleanup)
{
    // Prefetch did not competed download.
    // Remove cached file.
           
    std::stringstream ss;
    ss << "Fail " << m_temp_filename << " cleanup = " << cleanup << " finalised = " << m_finalized <<std::endl;
    if (Dbg) m_log.Emsg("Fail ", ss.str().c_str());

    XrdSysCondVarHelper monitor(m_cond);
    if (m_finalized)
        return false;
    if (!m_started)
        return false;

    if (m_output)
    {
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
    if (Dbg) m_log.Emsg("Destructor", "Destroying Prefetch Object");
    Join();
}

ssize_t
Prefetch::Read(char *buff, off_t offset, size_t size)
{
    XrdSysCondVarHelper monitor(m_cond);
    if (!m_started || m_finalized)
    {
        errno = EBADF;
        return -errno;
    }
    // TODO: if the file has been finalized, we could read it from its final location

    std::stringstream ss;
    ss << "offset = " << offset;


    off_t prefetch_offset = GetOffset();
    ss << "prefetch_offset = " << prefetch_offset;

    if (prefetch_offset < offset)
    {
        if (Dbg > 1) m_log.Emsg("Read", "Offset below requested offset. Nothing to read.", ss.str().c_str());
        return 0;
    }
    else if (prefetch_offset >= static_cast<off_t>(offset + size))
    {
        ss << ", size  = " << size;
        int res =  m_output->Read(buff, offset, size);
	ss <<  ", Read res = " << res;
        if (Dbg > 1) m_log.Emsg("Read", "read complete size .... ", ss.str().c_str());
	return res;
    }
    else
    {
        size_t to_read = offset + size - prefetch_offset;
        ss << ", to_read  = " << to_read;

        if (Dbg > 1) m_log.Emsg("Read", "read partial read ", ss.str().c_str());
        return m_output->Read(buff, offset, to_read);
    }
}


/*
   bool
   Prefetch::hasCompletedSuccessfully() const
   {
   return m_finalized == true && m_stop == false;
   }
 */
