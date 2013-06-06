#ifndef __XRDFILECACHE_FWD_H_
#define __XRDFILECACHE_FWD_H_

#include <tr1/memory>
#include <tr1/unordered_map>

namespace XrdFileCache
{
class Prefetch;
typedef std::tr1::shared_ptr<Prefetch> PrefetchPtr;
typedef std::tr1::weak_ptr<Prefetch> PrefetchWeakPtr;
typedef std::tr1::unordered_map<std::string, PrefetchWeakPtr> PrefetchWeakPtrMap;

class IO;
class Factory;
class Cache;
class Decision;
}

#endif

