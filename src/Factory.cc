
#include "XrdOuc/XrdOucLock.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOss/XrdOssApi.hh"

#include "Cache.hh"
#include "Factory.hh"
#include "Prefetch.hh"
#include "Decision.hh"

using namespace XrdFileCache;

#define TS_Xeq(x,m)    if (!strcmp(x,var)) return m(Config);

extern XrdOss *XrdOssGetSS(XrdSysLogger *, const char *, const char *);

Factory * Factory::m_factory = NULL;
XrdSysMutex Factory::m_factory_mutex;

Factory::Factory()
    : m_log(0, "XrdFileCache_"),
      m_temp_directory("/tmp"),
      m_username("nobody")
{
}

extern "C"
{
XrdOucCache *XrdOucGetCache(XrdSysLogger *logger,
                            const char   *config_filename,
                            const char   *parameters)
{
    Factory &factory = Factory::GetInstance();
    if (!factory.Config(logger, config_filename, parameters))
    {
        return NULL;
    }
    return &factory;
}
}

Factory &
Factory::GetInstance()
{
    XrdOucLock monitor(&m_factory_mutex);
    if (m_factory == NULL)
        m_factory = new Factory();
    return *m_factory;
}

XrdOucCache *
Factory::Create(Parms & parms, XrdOucCacheIO::aprParms * prParms)
{
    return new Cache(m_stats, m_log);
}

bool
Factory::Config(XrdSysLogger *logger, const char *config_filename, const char *parameters)
{
    m_log.logger(logger);
    m_log.Emsg("Config", "Configuring a file cache.");

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
        m_log.Emsg("Config", errno, "open config file", config_filename);;
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
        if ((strncmp(var, "filecache.", 4) == 0) && (!ConfigXeq(var+4, Config)))
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

    m_log.Emsg("Config", "Cache user name: ", m_username.c_str());
    m_log.Emsg("Config", "Cache temporary directory: ", m_temp_directory.c_str());

    if (retval)
    {
        XrdOss *output_fs = XrdOssGetSS(m_log.logger(), m_config_filename.c_str(), m_osslib_name.c_str());
        if (!output_fs)
        {
            m_log.Emsg("Factory_Attach", "Unable to create a OSS object.");
            retval = false;
        }
        m_output_fs = output_fs;
    }

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
    char *val; //, parms[2048];
    //int len;

    if (!(val = Config.GetWord()) || !val[0])
    {
        m_log.Emsg("Config", "decisionlib not specified");
        return false;
    }

    return false; // TODO: implement
}

bool
Factory::ConfigParameters(const char * parameters)
{
    if (!parameters || (!(*parameters)))
    {
        m_log.Emsg("Config", "No parameters passed; using defaults");
        return true;
    }

    XrdOucEnv myEnv;
    XrdOucStream Config(&m_log, getenv("XRDINSTANCE"), &myEnv, "=====> ");
    Config.Put(parameters);

    char * val;
    while ((val = Config.GetWord()))
    {
        if (!strcmp("-user", val))
        {
            if (!(val = Config.GetWord()))
            {
                m_log.Emsg("Config", "No username specified");
                return false;
            }
            m_username = val;
        }
        else if (!strcmp("-temp", val))
        {
            if (!(val = Config.GetWord()))
            {
                m_log.Emsg("Config", "No temporary directory specified.");
                return false;
            }
        }
    }

    return true;
}

PrefetchPtr
Factory::GetPrefetch(XrdOucCacheIO & io)
{
    std::string filename = io.Path();
    if (!Decide(filename))
    {
        PrefetchPtr result;
        return result;
    }
    XrdOucLock monitor(&m_factory_mutex);
    PrefetchWeakPtrMap::const_iterator it = m_prefetch_map.find(filename);

    if (it == m_prefetch_map.end())
    {
        PrefetchPtr result;
        result.reset(new Prefetch(m_log, *m_output_fs, io));
        m_prefetch_map[filename] = result;
        return result;
    }
    PrefetchPtr result = it->second.lock();
    if (!result)
    {
        result.reset(new Prefetch(m_log, *m_output_fs, io));
        m_prefetch_map[filename] = result;
        return result;
    }
    return result;
}

bool
Factory::Decide(std::string &filename)
{
    std::vector<Decision>::const_iterator it;
    for (it = m_decisionpoints.begin(); it != m_decisionpoints.end(); ++it)
    {
        if (!it->Decide(filename, *m_output_fs))
        {
            return false;
        }
    }
    return true;
}

