
#include <string>
#include <iostream>
#include <stdio.h>
#include "XrdOss/XrdOss.hh"

namespace XrdFileCache {

class Decision {
    XrdSysError* m_log;

public:
    virtual bool Decide(std::string &, XrdOss &) const = 0;
    virtual ~Decision() {}

    virtual bool ConfigParams(const char*) { return true; }
};

}

