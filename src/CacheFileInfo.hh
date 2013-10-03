#ifndef __XRDFILECACHE_CFI_HH__
#define __XRDFILECACHE_CFI_HH__

#include <stdio.h>
#include <time.h>

#include <assert.h>
class XrdOssDF;

#define cfiBIT(n)       (1ULL << (n))

namespace XrdFileCache
{

class CacheFileInfo
{
public:
   CacheFileInfo();
   ~CacheFileInfo();

   void setBit(int i);
   void resizeBits(int s);
   void setComplete(int c);

   int read(XrdOssDF* fp);
   int write(XrdOssDF* fp) const;
   bool isAnythingEmptyInRng(int firstIdx, int lastIdx) const;

   int getSizeInBytes() const;
   int getSizeInBits() const;
   long getBufferSize() const;
   bool testBit(int i) const;

   bool isComplete() const;
   void checkComplete();

   void touch();
   void print();


private:
   long    m_bufferSize;
   time_t m_accessTime;
   int    m_accessCnt;
   int    m_sizeInBits; // number of file blocks

   char*  m_buff;

   bool   m_complete; //cached
};

inline bool  CacheFileInfo::testBit(int i) const
{
   int cn = i/8;
   assert(cn < getSizeInBytes());

   int off = i - cn*8;
   return (m_buff[cn] & cfiBIT(off)) == cfiBIT(off);
}

inline int CacheFileInfo::getSizeInBytes() const
{
   return ((m_sizeInBits -1)/8 + 1);
}

inline int CacheFileInfo::getSizeInBits() const
{
   return m_sizeInBits;
}

inline bool CacheFileInfo::isComplete() const
{
   return m_complete;
}

inline bool CacheFileInfo::isAnythingEmptyInRng(int firstIdx, int lastIdx) const
{
   for (int i = firstIdx; i <= lastIdx; ++i)
      if(! testBit(i)) return true;

   return false;
}

inline void CacheFileInfo::checkComplete()
{
   m_complete = !isAnythingEmptyInRng(0, m_sizeInBits-1);
}

inline void CacheFileInfo::setBit(int i)
{
   int cn = i/8;
   assert(cn < getSizeInBytes());

   int off = i - cn*8;
   m_buff[cn] |= cfiBIT(off);
}

inline long CacheFileInfo::getBufferSize() const
{
   return m_bufferSize;
}
}
#endif
