#include "CacheFileInfo.hh"
#include "Context.hh"
#include "CacheStats.hh"

#include <XrdOss/XrdOss.hh>
#include <assert.h>

#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>


#define BIT(n)       (1ULL << (n))
using namespace XrdFileCache;


CacheFileInfo::CacheFileInfo():
   m_bufferSize(PrefetchDefaultBufferSize),
   m_sizeInBits(0), m_buff(0), 
   m_accessCnt(0), 
   m_complete(false)
{
}

CacheFileInfo::~CacheFileInfo() {
   if (m_buff) delete [] m_buff;
}

//______________________________________________________________________________


void  CacheFileInfo::resizeBits(int s)
{
   m_sizeInBits = s;
   m_buff = (char*)malloc(getSizeInBytes());
   memset(m_buff, 0, getSizeInBytes());
}

//______________________________________________________________________________


int CacheFileInfo::Read(XrdOssDF* fp)
{
    // does not need lock, called only in Prefetch::Open
    // before Prefetch::Run() starts

   int off = 0;
   off += fp->Read(&m_bufferSize, off, sizeof(long long));
   if (off <= 0) return off;

   int sb;
   off += fp->Read(&sb, off, sizeof(int));
   resizeBits(sb);

   off += fp->Read(m_buff, off, getSizeInBytes());
   m_complete = isAnythingEmptyInRng(0, sb-1) ? false : true;

   assert (off = getHeaderSize());

   off += fp->Read(&m_accessCnt, off, sizeof(int));

   return off;
}

//______________________________________________________________________________


int CacheFileInfo::getHeaderSize() const
{
   return  sizeof(long long) + sizeof(int) + getSizeInBytes();
}

//______________________________________________________________________________
void  CacheFileInfo::WriteHeader(XrdOssDF* fp)
{
  m_writeMutex.Lock();
   long long  off = 0;
   off += fp->Write(&m_bufferSize, off, sizeof(long long));
   int nb = getSizeInBits();
   off += fp->Write(&nb, off, sizeof(int));
   off += fp->Write(m_buff, off, getSizeInBytes());

   assert (off == getHeaderSize());
  m_writeMutex.UnLock();
}

//______________________________________________________________________________
void  CacheFileInfo::AppendIOStat(const CacheStats* caches, XrdOssDF* fp)
{
  m_writeMutex.Lock();
   struct AStat {
      time_t AppendTime;
      time_t DetachTime;
      long long BytesRead;
      int Hits;
      int Miss;
   };

   m_accessCnt++;

   // get offset to append
   // not: XrdOssDF FStat doesn not sets stat 
 
   long long off = getHeaderSize();
   off += fp->Write(&m_accessCnt, off, sizeof(int));
   off += (m_accessCnt-1)*sizeof(AStat);
   AStat as;
   as.AppendTime = caches->AppendTime;
   as.DetachTime = time(0);
   as.BytesRead = caches->BytesGet; // confusion in Get/Read see XrdOucCacheStats::Add()
   as.Hits = caches->Hits;
   as.Miss = caches->Miss;

   aMsg(kInfo, "====================== CacheFileInfo::AppendIOStat off[%d] Write access cnt = %d , bread = %lld \n",(int)off, m_accessCnt, as.BytesRead );
   long long ws = fp->Write(&as, off, sizeof(AStat));
   assert(ws == sizeof(AStat));
   m_writeMutex.UnLock();
}

//______________________________________________________________________________


void  CacheFileInfo::print() const
{
   printf("blocksSize %lld \n",m_bufferSize );
   printf("printing [%d] blocks \n", m_sizeInBits);
   for (int i = 0; i < m_sizeInBits; ++i)
   {
      printf("%d ", testBit(i));
   }
   printf("\n");
   printf("printing complete %d\n", m_complete);
}
