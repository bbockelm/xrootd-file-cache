#ifndef __XRDFILECACHE_FWD_H_
#define __XRDFILECACHE_FWD_H_

#include <tr1/memory>
#include <tr1/unordered_map>

namespace XrdFileCache
{
class File;
typedef std::tr1::shared_ptr<File> FilePtr;
typedef std::tr1::weak_ptr<File> FileWeakPtr;
typedef std::tr1::unordered_map<std::string, FileWeakPtr> FileWeakPtrMap;

class IO;
class Factory;
class Cache;
class Decision;
}

#endif

