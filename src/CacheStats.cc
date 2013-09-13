#include "CacheStats.hh"
#include "Context.hh"

using namespace XrdFileCache;

void CacheStats::Dump() const
{
   aMsg(kError, "StatDump bCP = %lld, bP = %lld, bD =  %lld\n", BytesCachedPrefetch, BytesPrefetch, BytesDisk);
}
