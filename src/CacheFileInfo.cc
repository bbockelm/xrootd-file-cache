#include "CacheFileInfo.hh"
#include "Context.hh"

#include <XrdOss/XrdOss.hh>
#include <assert.h>

#include <time.h>
#include <string.h>
#include <stdlib.h>


#define BIT(n)       (1ULL << (n))
using namespace XrdFileCache;


CacheFileInfo::CacheFileInfo():
   m_bufferSize(PrefetchDefaultBufferSize),
   m_accessTime(0), m_accessCnt(0),
   m_sizeInBits(0), m_buff(0), 
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

void CacheFileInfo::touch()
{
   m_accessTime = time(NULL);
   m_accessCnt += 1;
}

//______________________________________________________________________________


int CacheFileInfo::read(XrdOssDF* fp)
{
   int bs;
   int off = fp->Read(&bs, off, sizeof(int));
   if (off <= 0) return off;

   m_bufferSize=bs;
   off += fp->Read(&m_accessTime, off, sizeof(time_t));
   off += fp->Read(&m_accessCnt, off, sizeof(int));
   int sb;
   off += fp->Read(&sb, off, sizeof(int));
   resizeBits(sb);

   off += fp->Read(m_buff, off, getSizeInBytes());
   m_complete = isAnythingEmptyInRng(0, sb) ? false : true;

   return off;
}


int  CacheFileInfo::write(XrdOssDF* fp) const
{
   long long  off = 0;
   off += fp->Write(&m_bufferSize, off, sizeof(long long));
   off += fp->Write(&m_accessTime, off, sizeof(time_t));
   off += fp->Write(&m_accessCnt, off, sizeof(int));

   int nb = getSizeInBits();
   off += fp->Write(&nb, off, sizeof(int));
   off += fp->Write(m_buff, off, getSizeInBytes()-1);

   return off;
}


//______________________________________________________________________________


void  CacheFileInfo::print()
{
   printf("bufferSize %lld accest %d accessCnt %d  \n",m_bufferSize, (int) m_accessTime, m_accessCnt );
   printf("printing b vec\n");
   for (int i = 0; i < m_sizeInBits; ++i)
   {
      printf("%d ", testBit(i));
   }
   printf("\n");
   printf("printing complete %d", m_complete);
}
