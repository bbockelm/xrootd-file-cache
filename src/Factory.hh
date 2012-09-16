
#ifndef __XRDFILECACHE_FACTORY_HH__
#define __XRDFILECACHE_FACTORY_HH__
/******************************************************************************/
/*                                                                            */
/* (c) 2012 University of Nebraksa-Lincoln                                    */
/*     by Brian Bockelman                                                     */
/*                                                                            */
/******************************************************************************/

#include <string>
#include <vector>

#include "XrdFileCacheFwd.hh"

#include <XrdSys/XrdSysPthread.hh>
#include <XrdOuc/XrdOucCache.hh>

namespace XrdFileCache {

class Cache;

class Factory : public XrdOucCache
{

friend class Cache;

public:

    Factory();

    XrdOucCacheIO *Attach(XrdOucCacheIO *, int Options=0) {return NULL;}

    int isAttached() {return false;}

    bool Config(XrdSysLogger *logger, const char *config_filename, const char *parameters);

    virtual XrdOucCache *Create(Parms &, XrdOucCacheIO::aprParms *aprP=0);

    std::string &GetUsername() {return m_username;}
    std::string &GetTempDirectory() {return m_temp_directory;}

    static Factory &GetInstance();

protected:

    PrefetchPtr GetPrefetch(XrdOucCacheIO &);
    void Detach(PrefetchPtr);

private:

    void Detach(XrdOucCacheIO *);

    bool ConfigParameters(const char *);
    bool ConfigXeq(char *, XrdOucStream &);
    bool xolib(XrdOucStream &);
    bool xdlib(XrdOucStream &);

    bool Decide(std::string &);

    static XrdSysMutex m_factory_mutex;
    static Factory * m_factory;

    XrdSysError m_log;
    XrdOucCacheStats m_stats;
    std::string m_osslib_name;
    std::string m_config_filename;
    std::string m_temp_directory;
    std::string m_username;
    PrefetchWeakPtrMap m_prefetch_map;
    XrdOss *m_output_fs;
    std::vector<Decision*> m_decisionpoints;

};

}

#endif
