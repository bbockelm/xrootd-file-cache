#include <sstream>
#include <fcntl.h>
#include <stdio.h>

#include "XrdSys/XrdSysPthread.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOss/XrdOss.hh"
#if !defined(HAVE_VERSIONS)
#include "XrdOss/XrdOssApi.hh"
#endif
#include "XrdSys/XrdSysPlugin.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdVersion.hh"
#include "XrdPosix/XrdPosixXrootd.hh"

#include "Cache.hh"
#include "Factory.hh"
#include "Prefetch.hh"
#include "Decision.hh"
#include "File.hh"
#include "Context.hh"


using namespace XrdFileCache;

#define TS_Xeq(x,m)    if (!strcmp(x,var)) return m(Config);

// Copy/paste from XrdOss/XrdOssApi.cc.  Unfortunately, this function
// is not part of the stable API for extension writers, necessitating
// the copy/paste.
//

Factory * Factory::m_factory = NULL;
XrdSysMutex Factory::m_factory_mutex;

XrdOss *XrdOssGetSS(XrdSysLogger *Logger, const char *config_fn,
                    const char *OssLib, const char *OssParms)
{
   static XrdOssSys myOssSys;
   extern XrdSysError OssEroute;
   XrdSysPlugin *myLib;
   XrdOss *(*ep)(XrdOss *, XrdSysLogger *, const char *, const char *);

   XrdSysError err(Logger, "XrdOssGetSS");

// If no library has been specified, return the default object
//
#if defined(HAVE_VERSIONS)
   if (!OssLib) OssLib = "libXrdOfs.so"
#else
   if (!OssLib || !*OssLib) {err.Emsg("GetOSS", "Attempting to initiate default OSS object.");
                 if (myOssSys.Init(Logger, config_fn)) return 0;
                    else return (XrdOss *)&myOssSys;
                }
#endif

// Create a plugin object
//
   OssEroute.logger(Logger);
   OssEroute.Emsg("XrdOssGetSS", "Initializing OSS lib from ", OssLib);
#if defined(HAVE_VERSIONS)
   if (!(myLib = new XrdSysPlugin(&OssEroute, OssLib, "osslib",
                                  myOssSys.myVersion))) return 0;
#else
   if (!(myLib = new XrdSysPlugin(&OssEroute, OssLib))) return 0;
#endif

// Now get the entry point of the object creator
//
   ep = (XrdOss *(*)(XrdOss *, XrdSysLogger *, const char *, const char *))
                    (myLib->getPlugin("XrdOssGetStorageSystem"));
   if (!ep) return 0;

// Get the Object now
//
#if defined(HAVE_VERSIONS)
   myLib->Persist(); delete myLib;
#endif
   return ep((XrdOss *)&myOssSys, Logger, config_fn, OssParms);
}


Factory * Factory::m_factory = NULL;
XrdSysMutex Factory::m_factory_mutex;


void* TempDirCleanupThread(void*)
{
   Factory::GetInstance().TempDirCleanup();
   return NULL;
}


Factory::Factory()
    : m_log(0, "XrdFileCache_"),
      m_temp_directory("/var/tmp/xrootd-file-cache"),
      m_username("nobody"),
      m_cache_expire(172800)
{
   Dbg = 1;
}

extern "C"
{
XrdOucCache *XrdOucGetCache(XrdSysLogger *logger,
                            const char   *config_filename,
                            const char   *parameters)
{
    XrdSysError err(0, "XrdFileCache_");
    err.logger(logger);
    err.Emsg("Retrieve", "Retrieving a caching proxy factory.");
    Factory &factory = Factory::GetInstance();
    if (!factory.Config(logger, config_filename, parameters))
    {
        err.Emsg("Retrieve", "Error - unable to create a factory.");
        return NULL;
    }
    err.Emsg("Retrieve", "Success - returning a factory.");


    pthread_t tid;
    XrdSysThread::Run(&tid, TempDirCleanupThread, NULL, 0, "XrdFileCache TempDirCleanup");
    return &factory;
}
}

Factory &
Factory::GetInstance()
{
    // XrdSysMutexHelper monitor(&m_factory_mutex);
    //AMT !!!
       if (m_factory == NULL)
      m_factory = new Factory();
    return *m_factory;
}

XrdOucCache *
Factory::Create(Parms & parms, XrdOucCacheIO::aprParms * prParms)
{
    m_log.Emsg("Create", "Creating a new cache object.");
    return new Cache(m_stats, m_log);
}

bool
Factory::Config(XrdSysLogger *logger, const char *config_filename, const char *parameters)
{

    // XrdPosixXrootd::setEnv("DataServerConn_ttl",  300);
    // XrdPosixXrootd::setEnv("LBServerConn_ttl",    3600);

    m_log.logger(logger);
    m_log.Emsg("Config", "Configuring a file cache.");

    const char * cache_env;
    if (!(cache_env = getenv("XRDPOSIX_CACHE")) || !*cache_env)
        XrdOucEnv::Export("XRDPOSIX_CACHE", "mode=s&optwr=0");

    XrdOucEnv myEnv;
    XrdOucStream Config(&m_log, getenv("XRDINSTANCE"), &myEnv, "=====> ");

    if (!config_filename || !*config_filename)
    {
        m_log.Emsg("Config", "Configuration file not specified.");
        return false;
    }

    int fd;
    if ( (fd = open(config_filename, O_RDONLY, 0)) < 0)
    {
        m_log.Emsg("Config", errno, "open config file", config_filename);
        return false;
    }

    Config.Attach(fd);

    // Actual parsing of the config file.
    bool retval = true;
    int retc;
    char *var;
    while((var = Config.GetMyFirstWord()))
    {
        if ((strncmp(var, "oss.", 4) == 0) && (!ConfigXeq(var+4, Config)))
        {
            Config.Echo();
            retval = false;
            break;
        }
        if ((strncmp(var, "pss.", 4) == 0) && (!ConfigXeq(var+4, Config)))
        {
            Config.Echo();
            retval = false;
            break;
        }
    }

    if ((retc = Config.LastError()))
    {
        retval = false;
        m_log.Emsg("Config", retc, "read config file", config_filename);
    }

    Config.Close();

    m_config_filename = config_filename;

    if (retval)
        retval = ConfigParameters(parameters);

    m_log.Emsg("Config", "Cache user name ", m_username.c_str());
    m_log.Emsg("Config", "Cache temporary directory: ", m_temp_directory.c_str());
    {
       std::stringstream xss;  xss << Dbg;
       m_log.Emsg("Config", "Cache debug ", xss.str().c_str());
    }

    {
       std::stringstream xss;  xss << m_cache_expire << "[s]";
       m_log.Emsg("Config", "Cache expire ", xss.str().c_str());
    }

    if (retval)
    {
        XrdOss *output_fs = XrdOssGetSS(m_log.logger(), m_config_filename.c_str(), m_osslib_name.c_str(), NULL);
        if (!output_fs)
        {
            m_log.Emsg("Factory_Attach", "Unable to create a OSS object.");
           retval = false;
        }
        m_output_fs = output_fs;
    }

    if (retval) m_log.Emsg("Config", "Configuration of factory successful");
    else m_log.Emsg("Config", "Configuration of factory failed");

    return retval;
}

bool Factory::ConfigXeq(char *var, XrdOucStream &Config)
{
    TS_Xeq("osslib",        xolib);
    TS_Xeq("decisionlib" ,  xdlib);
    return true;
}

/* Function: xolib

   Purpose:  To parse the directive: osslib <path> [<parms>]

             <path>  the path of the oss library to be used.
             <parms> optional parameters to be passed.

  Output: true upon success or false upon failure.
*/
bool
Factory::xolib(XrdOucStream &Config)
{
    char *val, parms[2048];
    int pl;

    if (!(val = Config.GetWord()) || !val[0])
    {
        m_log.Emsg("Config", "osslib not specified");
        return false;
    }

    strcpy(parms, val);
    pl = strlen(val);
    *(parms+pl) = ' ';
    if (!Config.GetRest(parms+pl+1, sizeof(parms)-pl-1))
    {
        m_log.Emsg("Config", "osslib parameters too long");
        return false;
    }

    m_osslib_name = parms;
    return true;
}


/* Function: xdlib

   Purpose:  To parse the directive: decisionlib <path> [<parms>]

             <path>  the path of the decision library to be used.
             <parms> optional parameters to be passed.


   Output: true upon success or false upon failure.
*/
bool
Factory::xdlib(XrdOucStream &Config)
{
    const char*  val; 

    std::string libp;
    if (!(val = Config.GetWord()) || !val[0])
    {
        m_log.Emsg("Config", "decisionlib not specified; always caching files");
        libp = "XrdFileCacheAllowAlways";
    }
    else{
         libp = val;
    }

    const char* params;
    params = (val[0]) ?  Config.GetWord() : 0;

#if defined(HAVE_VERSIONS)
    XrdSysPlugin* myLib = new XrdSysPlugin(&m_log, libp.c_str(), "decisionlib", NULL);
#else
    XrdSysPlugin* myLib = new XrdSysPlugin(&m_log, libp.c_str());
#endif
    Decision *(*ep)(XrdSysError&);
    ep = (Decision *(*)(XrdSysError&))myLib->getPlugin("XrdFileCacheGetDecision");
    if (!ep) return false;

    Decision * d = ep(m_log);
    if (!d)
    {
       m_log.Emsg("Config", "decisionlib was not able to create a decision object");
       return false;
    }
    if (params) d->ConfigDecision(params)
    m_decisionpoints.push_back(d);
    return true;
}

bool
Factory::ConfigParameters(const char * parameters)
{
    if (!parameters || (!(*parameters)))
    {
        m_log.Emsg("Config", "No parameters passed; using defaults");
        return true;
    }

    istringstream is(parameters);
    string part;
    while (getline(is, part, ' '))
    {
       // cout << part << endl;
        if ( part == "-user" ) {
            getline(is, part, ' ');
            m_username = part.c_str();
            //std::string msg = "Set user to " +  part;
            // m_log.Emsg("Config",  msg.c_str());
        }
        else if  ( part == "-tmp" )
        {
            getline(is, part, ' ');
            m_temp_directory = part.c_str();
            //std::string msg = "Set cache directory to " +  part;
            // m_log.Emsg("Config",  msg.c_str());           
        }
        else if  ( part == "-expire" )
        {
            getline(is, part, ' ');
            m_cache_expire = atoi(part.c_str());
        }
        else if  ( part == "-debug" )
        {
            getline(is, part, ' ');
            Dbg = atoi(part.c_str());
        }
        else if  ( part == "-log" )
        {
            getline(is, part, ' ');
	    Rec.open(part.c_str(), std::fstream::in | std::fstream::out | std::fstream::app);
	    if (Rec.is_open())
	       m_log.Emsg("Config", "Write record in file", part.c_str());  
	    else
	    {
	       m_log.Emsg("Config", "Write record in default file /tmp/xroot_cache.log"); 
               Rec.open("/tmp/xroot_cache.log");
	    }
	}
        else if  ( part == "-exclude" )
        {
           getline(is, part, ' ');
           // m_log.Emsg("Config", "Excluded hosts ", part.c_str());          
           XrdClient::fDefaultExcludedHosts = part;
           part += ",";
        }
    }

    return true;
}

FilePtr
Factory::GetXfcFile(XrdOucCacheIO & io)
{
    std::string filename = io.Path();
    if (Dbg) m_log.Emsg("GetXcfFile from global map", "XcfFile object requested for ", filename.c_str());

    if (!Decide(filename))
    {
        FilePtr result;
        return result;
    }
     

    XrdSysMutexHelper monitor(&m_factory_mutex);
    FileWeakPtrMap::const_iterator it = m_file_map.find(filename);
    if (it == m_file_map.end())
    {
        FilePtr result;
        result.reset(new File(m_log, *m_output_fs, io));
        m_file_map[filename] = result;
        return result;
    }
    FilePtr result = it->second.lock();
    if (!result)
    {
        result.reset(new File(m_log, *m_output_fs, io));
        m_file_map[filename] = result;
        return result;
    }
    return result;
}


bool
Factory::Decide(std::string &filename)
{

    printf("AMT add decision points ... %p\n", (void*)m_xxx);
    m_xxx->testin();

    return true;
}



void Factory::CheckDirStatRecurse( XrdOssDF* df, std::string& path)
{   
   char buff[256];
   XrdOucEnv env;
   struct stat st;
   int  rdr;
   //  std::cerr << "CheckDirStatRecurse " << path << std::endl;
   while ( (rdr = df->Readdir(&buff[0], 256)) >= 0)
   {
      std::string np = path + "/" + std::string(buff); 
      if ( strlen(&buff[0]) == 0  )
      {
         // std::cout << "Finish read dir. Break loop \n";
         break;
      }


      if (strncmp("..", &buff[0], 2) && strncmp(".", &buff[0], 1))
      {
         std::auto_ptr<XrdOssDF> dh(m_output_fs->newDir(m_username.c_str()));
         std::auto_ptr<XrdOssDF> fh(m_output_fs->newFile(m_username.c_str()));
	 // std::cerr << "check " << np << std::endl;
         if ( dh->Opendir(np.c_str(), env)  >= 0 )
         {
            CheckDirStatRecurse(dh.get(), np);
         }
         else if ( fh->Open(np.c_str(),O_RDONLY, 0600, env) >= 0)
         {
            fh->Fstat(&st);
            if ( time(0) - st.st_mtime > m_cache_expire )
            {
               m_log.Emsg("CheckDirStatRecurse", "removing file", &buff[0]);
               m_output_fs->Unlink(np.c_str());
            }
         }
	 else
	 {
	     m_log.Emsg("CheckDirStatRecurse", "can't access file ", np.c_str());
	 }
      }
   }
}


void Factory::TempDirCleanup()
{
   XrdOucEnv env;
   int interval = (m_cache_expire > 7200) ? 7200 : m_cache_expire;
   while (1)
   {   
      // AMT: I think Opendir()/Close() should be enough, but it seems readdir does
      //      not work properly
      std::auto_ptr<XrdOssDF> dh(m_output_fs->newDir(m_username.c_str()));
      if (dh->Opendir(m_temp_directory.c_str(), env) >= 0)
         CheckDirStatRecurse(dh.get(), m_temp_directory);
      else
         m_log.Emsg("TempDirCleanup", "can't open file cache directory ", m_temp_directory.c_str());

      dh->Close();
      sleep(interval);   
   }
}
