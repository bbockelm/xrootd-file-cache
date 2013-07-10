
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

const size_t Prefetch::m_buffer_size = 64*1024;

Prefetch::Prefetch(XrdOss &outputFS, XrdOucCacheIO &inputIO, std::string& disk_file_path)
    : m_output_fs(outputFS),
      m_output(NULL),
      m_input(inputIO),
      m_xrdClient(0),
      m_offset(0),
      m_started(false),
      m_finalized(false),
      m_stop(false),
      m_cond(0), // We will explicitly lock the condition before use.
      m_temp_filename(disk_file_path)
{
    m_xrdClient = new XrdClient(m_input.Path());

    if ( !m_xrdClient->Open(0, kXR_async) || m_xrdClient->LastServerResp()->status != kXR_ok)
    {
      aMsgIO(kDebug, &m_input, "Prefetch::Prefetch() Client error.");
    }
}

Prefetch::~Prefetch()
{ 
    aMsgIO(kDebug, &m_input, "Prefetch::~Prefetch() destroying Prefetch Object");
    Join();

    aMsgIO(kWarning, &m_input, "Prefetch::~Prefetch close disk file");
    m_output->Close();
    delete m_output;
    m_output = NULL;
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

   aMsgIO(kDebug, &m_input, "Prefetch::Run()");

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
         aMsgIO(kDump, &m_input, "Prefetch::Run() Prefetch %d MB", m_offset/(1024*1024));
      }
      // Note we don't lock read-access, as this will only ever go from 0 to 1
      // Reading during a partial write is OK in this case.
      if (m_stop)
      {
         aMsgIO(kInfo, &m_input, "Prefetch::Run() %s", "stopping for a clean cause");
         retval = -EINTR;
         break;
      }
   }

   if (retval < 0)
   {
      aMsgIO(kError, &m_input, "Prefetch::Run()  failure prefetching file, retval = %d", retval);
      m_stop = true;
      Fail(retval != -EINTR);
   }

   Close();
}

void
Prefetch::Join()
{
    aMsgIO(kDebug, &m_input, "Prefetch::Join() going to lock");

    XrdSysCondVarHelper monitor(m_cond);
    if (m_finalized)
    {
        aMsgIO(kDebug, &m_input, "Prefetch::Join() already finalized");
        return;
    }
    else if (m_started)
    {
        aMsgIO(kDebug, &m_input, "Prefetch::Join() waiting");
        m_cond.Wait();
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
    XrdSysCondVarHelper monitor(m_cond);
    if (m_started)
    {
        return false;
    }
    m_started = true;
    // Finalize temporary turned on in case of exception.
    m_finalized = true;


    aMsgIO(kDebug, &m_input, "Prefetch::Open() open disk file");


    // Create the file itself.
    XrdOucEnv myEnv;

    m_output_fs.Create(Factory::GetInstance().GetUsername().c_str(), m_temp_filename.c_str(), 0600, myEnv, XRDOSS_mkpath);
    m_output = m_output_fs.newFile(Factory::GetInstance().GetUsername().c_str());
    if (!m_output || m_output->Open(m_temp_filename.c_str(), O_RDWR, 0600, myEnv) < 0)
    {
        aMsgIO(kError, &m_input, "Prefetch::Open() fail to get FD");
        return false;
    }

    // If the file is pre-existing, pick up from where we left off.
    struct stat fileStat;
    if (m_output->Fstat(&fileStat) == 0)
    {
        m_offset = fileStat.st_size;
        std::stringstream ss; ss << m_offset;
        if(m_offset) { 
            aMsgIO(kDebug, &m_input, "Prefetch::Open() pickup where we left of %d",  m_offset);
        }
    }

    m_finalized = false;
    return true;
}

bool
Prefetch::Close()
{
    aMsgIO(kInfo, &m_input, "Prefetch::Close()");
    XrdSysCondVarHelper monitor(m_cond);
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

    m_cond.Broadcast();
    m_finalized = true;

    return false; // Fail until this is implemented.
}

bool
Prefetch::Fail(bool cleanup)
{
    // Prefetch did not competed download.
    // Remove cached file.

    XrdSysCondVarHelper monitor(m_cond);
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
        m_output = NULL;
    }

    if (cleanup && !m_temp_filename.empty())
        m_output_fs.Unlink(m_temp_filename.c_str());

    m_cond.Broadcast();
    m_finalized = true;
    return true;
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

    off_t prefetch_offset = GetOffset();

    if (prefetch_offset < offset)
    {
        aMsgIO(kDebug, &m_input, "Prefetch::Read() Offset %lld below requested offset %lld.Nothing to read Nothing to read\n", prefetch_offset);
        return 0;
    }
    else if (prefetch_offset >= static_cast<off_t>(offset + size))
    {
        aMsgIO(kDebug, &m_input, "Prefetch::Read() read complete size ");
        return m_output->Read(buff, offset, size);
    }
    else
    {
        size_t to_read = offset + size - prefetch_offset;
        aMsgIO(kDebug, &m_input, "Prefetch::Read() partial read first %lld bytes", to_read);
        return m_output->Read(buff, offset, to_read);
    }
}
