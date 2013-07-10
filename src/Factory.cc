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
#include "Context.hh"


using namespace XrdFileCache;

#define TS_Xeq(x,m)    if (!strcmp(x,var)) return m(Config);

// Copy/paste from XrdOss/XrdOssApi.cc.  Unfortunately, this function
// is not part of the stable API for extension writers, necessitating
// the copy/paste.
//

Factory * Factory::m_factory = NULL;
XrdSysMutex Factory::m_factory_mutex;

XrdOss *
XrdOssGetSS(XrdSysLogger *Logger, const char *config_fn,
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
    if (!OssLib)
        OssLib = "libXrdOfs.so"
#else
    if (!OssLib || !*OssLib)
    {
        err.Emsg("GetOSS", "Attempting to initiate default OSS object.");
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


void*
TempDirCleanupThread(void*)
{
    Factory::GetInstance().TempDirCleanup();
    return NULL;
}


Factory::Factory()
    : m_log(0, "XFC_"),
      m_temp_directory("/var/tmp/xrootd-file-cache"),
      m_username("nobody"),
      m_cache_expire(172800)
{
    Dbg = kInfo;
}

extern "C"
{
XrdOucCache *
XrdOucGetCache(XrdSysLogger *logger,
               const char   *config_filename,
               const char   *parameters)
{
    XrdSysError err(0, "");
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
    if (m_factory == NULL)
        m_factory = new Factory();
    return *m_factory;
}

XrdOucCache *
Factory::Create(Parms & parms, XrdOucCacheIO::aprParms * prParms)
{
    aMsg(kInfo, "Factory::Create() new cache object");
    return new Cache(m_stats);
}

bool
Factory::Config(XrdSysLogger *logger, const char *config_filename, const char *parameters)
{
    m_log.logger(logger);


    const char * cache_env;
    if (!(cache_env = getenv("XRDPOSIX_CACHE")) || !*cache_env)
        XrdOucEnv::Export("XRDPOSIX_CACHE", "mode=s&optwr=0");

    XrdOucEnv myEnv;
    XrdOucStream Config(&m_log, getenv("XRDINSTANCE"), &myEnv, "=====> ");

    if (!config_filename || !*config_filename)
    {
        aMsg(kWarning, "Factory::Config() configuration file not specified.");
        return false;
    }

    int fd;
    if ( (fd = open(config_filename, O_RDONLY, 0)) < 0)
    {
        aMsg(kError, "Factory::Config() can't open configuration file %s", config_filename);
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
        aMsg(kError, "Factory::Config() error in parsing");
    }

    Config.Close();

    m_config_filename = config_filename;

    if (retval)
        retval = ConfigParameters(parameters);

    if (!Rec.is_open())
    {
       // AMT deside if records  are still necesasry, maybe debug stting 
       // debug level is enough
       Rec.open("/tmp/xroot_cache.log");
    }

    aMsg(kInfo,"Factory::Config() Cache user name %s", m_username.c_str());
    aMsg(kInfo,"Factory::Config() Cache temporary directory %s", m_temp_directory.c_str());
    aMsg(kInfo,"Factory::Config() Cache debug level %d", Dbg);
    aMsg(kInfo,"Factory::Config() Cache expire in %d [s]",m_cache_expire );
           
    if (retval)
    {
        XrdOss *output_fs = XrdOssGetSS(m_log.logger(), m_config_filename.c_str(), m_osslib_name.c_str(), NULL);
        if (!output_fs) {
            aMsg(kError, "Factory::Config() Unable to create a OSS object");
            retval = false;
        }
        m_output_fs = output_fs;
    }

    aMsg(kInfo, "Factory::Config() Configuration = %s ", retval ? "Success" : "Fail");
    return retval;
}

bool
Factory::ConfigXeq(char *var, XrdOucStream &Config)
{
    TS_Xeq("osslib",        xolib);
    TS_Xeq("decisionlib",  xdlib);
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
        aMsg(kInfo, "Factory::Config() osslib not specified");
        return false;
    }

    strcpy(parms, val);
    pl = strlen(val);
    *(parms+pl) = ' ';
    if (!Config.GetRest(parms+pl+1, sizeof(parms)-pl-1))
    {
        aMsg(kError, "Factory::Config() osslib parameters too long");
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
        aMsg(kInfo, " Factory:;Config() decisionlib not specified; always caching files");
        libp = "XrdFileCacheAllowAlways";
    }
    else
    {
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
        aMsg(kError, "Factory::Config() decisionlib was not able to create a decision object");
        return false;
    }
    if (params)
        d->ConfigDecision(params);

    m_decisionpoints.push_back(d);
    return true;
}

bool
Factory::ConfigParameters(const char * parameters)
{
    if (!parameters || (!(*parameters)))
    {
        return true;
    }

    istringstream is(parameters);
    string part;
    while (getline(is, part, ' '))
    {
        // cout << part << endl;
        if ( part == "-user" )
        {
            getline(is, part, ' ');
            m_username = part.c_str();
            aMsg(kInfo, "Factory::ConfigParameters() set user to %s", m_username.c_str());
        }
        else if  ( part == "-tmp" )
        {
            getline(is, part, ' ');
            m_temp_directory = part.c_str();
            aMsg(kInfo, "Factory::ConfigParameters() set temp. directory to %s", m_temp_directory.c_str());
        }
        else if  ( part == "-expire" )
        {
            getline(is, part, ' ');
            m_cache_expire = atoi(part.c_str());
        }
        else if  ( part == "-debug" )
        {
            getline(is, part, ' ');
            Dbg = (LogLevel)atoi(part.c_str());
        }
        else if  ( part == "-log" )
        {
            getline(is, part, ' ');
            Rec.open(part.c_str(), std::fstream::in | std::fstream::out | std::fstream::app);
            if (Rec.is_open())    
              aMsg(kInfo, "Factory::ConfigParameters() set user to %s", part.c_str());      
        }
        else if  ( part == "-exclude" )
        {
            getline(is, part, ' ');
            aMsg(kInfo, "Factory::ConfigParameters() Excluded hosts ", part.c_str());
            XrdClient::fDefaultExcludedHosts = part;
            part += ",";
        }
    }

    return true;
}

PrefetchPtr
Factory::GetPrefetch(XrdOucCacheIO & io, std::string& filename)
{
    aMsg(kInfo, "Factory::GetPrefetch(), object requested for %s ", filename.c_str());

  
    XrdSysMutexHelper monitor(&m_factory_mutex);
    PrefetchWeakPtrMap::const_iterator it = m_file_map.find(filename);
    if (it == m_file_map.end())
    {
        PrefetchPtr result;
        result.reset(new Prefetch(*m_output_fs, io, filename));
        m_file_map[filename] = result;
        return result;
    }
    PrefetchPtr result = it->second.lock();
    if (!result)
    {
       result.reset(new Prefetch(*m_output_fs, io, filename));
        m_file_map[filename] = result;
        return result;
    }
    return result;
}


bool
Factory::Decide(std::string &filename)
{
    std::vector<Decision*>::const_iterator it;
    for (it = m_decisionpoints.begin(); it != m_decisionpoints.end(); ++it)
    {
        Decision *d = *it;
        if (!d) continue;
        if (!d->Decide(filename, *m_output_fs))
        {
            return false;
        }
    }
    return true;
}



void
Factory::CheckDirStatRecurse( XrdOssDF* df, std::string& path)
{
    char buff[256];
    XrdOucEnv env;
    struct stat st;
    int rdr;
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
                    aMsg(kInfo, "Factory::CheckDirStatRecurse() removing file %s", &buff[0]);
                    m_output_fs->Unlink(np.c_str());
                }
            }
            else
            {
               aMsg(kError, "Factory::CheckDirStatRecurse() can't access file %s", np.c_str());
            }
        }
    }
}


void
Factory::TempDirCleanup()
{
    XrdOucEnv env;
    static int mingap = 7200;
    int interval = (m_cache_expire > mingap) ? mingap : m_cache_expire;
    while (1)
    {
        // AMT: I think Opendir()/Close() should be enough, but it seems readdir does
        //      not work properly
        std::auto_ptr<XrdOssDF> dh(m_output_fs->newDir(m_username.c_str()));
        if (dh->Opendir(m_temp_directory.c_str(), env) >= 0)
            CheckDirStatRecurse(dh.get(), m_temp_directory);
        else
           aMsg(kError, "Factory::CheckDirStatRecurse() can't open %s", m_temp_directory.c_str());

        dh->Close();
        sleep(interval);
    }
}

