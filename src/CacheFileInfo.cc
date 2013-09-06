#include "CacheFileInfo.hh"
#include <XrdOss/XrdOss.hh>
 
#include <time.h>
#include <string.h>
#include <stdlib.h>


#define BIT(n)       (1ULL << (n))
using namespace XrdFileCache;

CacheFileInfo::CacheFileInfo(): 
   m_accessTime(0), m_accessCnt(0),
  m_sizeInBits(0), m_buff(0), 
   m_complete(false) 
{
}

CacheFileInfo::~CacheFileInfo() {
   if (m_buff) delete [] m_buff;
}

//______________________________________________________________________________


void CacheFileInfo:: setBit(int i)
{
   int cn = i/8;
   int off = i - cn*8;
   m_buff[cn] |= BIT(off);
}
//______________________________________________________________________________


bool  CacheFileInfo::testBit(int i) const
{
   int cn = i/8;
   int off = i - cn*8;
   return (m_buff[cn] & BIT(off)) == BIT(off);
}

int CacheFileInfo::getSizeInBytes() const
{
   return m_sizeInBits/8;
}

int CacheFileInfo::getSizeInBits() const
{
   return m_sizeInBits;
}

bool CacheFileInfo::isComplete() const
{
   return m_complete;
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
   int off = 0;
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
   off += fp->Write(&m_accessTime, off, sizeof(time_t));
   off += fp->Write(&m_accessCnt, off, sizeof(int));

   int nb = getSizeInBits();
   off += fp->Write(&nb, off, sizeof(int));
   off += fp->Write(m_buff, off, getSizeInBytes()-1);

   return off;
}

//______________________________________________________________________________

bool CacheFileInfo::isAnythingEmptyInRng(int firstIdx, int lastIdx) const
{
   for (int i = firstIdx; i <= lastIdx; ++i)
      if(! testBit(i)) return true;

   return false;
}
//______________________________________________________________________________


void  CacheFileInfo::print()
{
   printf("write accest %d accessCnt %d  \n",(int) m_accessTime, m_accessCnt );
   printf("printing b vec\n");
   for (int i = 0; i < m_sizeInBits; ++i)
   {
      printf("%d ", testBit(i));
   }
   printf("\n");
   printf("printing complete %d", m_complete);
}
