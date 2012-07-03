
#include <string>

#include "XrdOss/XrdOssApi.hh"

namespace XrdFileCache {

class Decision {

public:

    virtual bool Decide(std::string &, XrdOss &) const = 0;

};

}

