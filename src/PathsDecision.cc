#include "Decision.hh"

#include "XrdSys/XrdSysError.hh"

#include <vector>
#include <sstream>

#include <iostream>
#include <stdio.h>
/*
  A plugin which excludes paths whcih are given in configuration
  parameters.
*/

class PathsDecision : public XrdFileCache::Decision {

public:
    PathsDecision(){
        m_log = new  XrdSysError(0);
    }

    virtual ~PathsDecision() {}

    virtual bool Decide(std::string &url, XrdOss &) const
    {
        size_t split_loc = url.rfind("//");
        const char* path = &url[split_loc];

        if (split_loc == url.npos)
            return false;

        for(std::vector<std::string>::const_iterator i=m_excludes.begin(); i != m_excludes.end(); ++i)
        {
            if (strncmp(i->c_str(), path, i->size())) {
                XrdSysError err(0, "XrdFileCache_");
                m_log.Emsg( "Rejecting ", "url ", path);
                return false;
            }
        }
        return true;
    }

    virtual bool ConfigDecision(const char* parameters)
    {
        std::istringstream is(parameters);
        std::string part;
        while (getline(is, part, ' '))
        {
            m_excludes.push_back(part);
        }

        using namespace std;
        cerr  << "fff  " << m_excludes.size() << endl;

        return !m_excludes.empty();
    }
    virtual void testin() { printf("test inherited DEC \n");}

private:
    std::vector<std::string> m_excludes;

};

/******************************************************************************/
/*                          XrdFileCacheGetDecision                           */
/******************************************************************************/

// Return a decision object to use.
extern "C"
{
XrdFileCache::Decision * XrdFileCacheGetDecision(XrdSysError &)
{
    return new PathsDecision();
}
}

