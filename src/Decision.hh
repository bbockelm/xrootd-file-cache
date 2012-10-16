
#include <string>

#include "XrdOss/XrdOss.hh"

namespace XrdFileCache {

class Decision {

public:

    virtual bool Decide(std::string &, XrdOss &) const = 0;

    virtual ~Decision() {}

};

}

