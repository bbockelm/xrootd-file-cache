
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

class XrdOucStream;

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

    const std::string &GetUsername() const {return m_username;}
    const std::string GetTempDirectory() const {return m_temp_directory;}
    XrdOss*  GetOss() const {return m_output_fs;}

    void TempDirCleanup();
    static Factory &GetInstance();

protected:

    PrefetchPtr GetPrefetch(XrdOucCacheIO &);
    bool havePrefetchForIO(XrdOucCacheIO & io);
    void Detach(PrefetchPtr);
    bool HavePrefetchForIO(XrdOucCacheIO & io);

private:

    void Detach(XrdOucCacheIO *);

    bool ConfigParameters(const char *);
    bool ConfigXeq(char *, XrdOucStream &);
    bool xolib(XrdOucStream &);
    bool xdlib(XrdOucStream &);
    bool xexpire(XrdOucStream &);

    bool Decide(std::string &);

    void CheckDirStatRecurse( XrdOssDF* df, std::string& path);

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
    int m_cache_expire;
    std::vector<Decision*> m_decisionpoints;

};

}

#endif
