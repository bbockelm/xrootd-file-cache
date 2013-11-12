#ifndef __XRDFILECACHE_CFI_HH__
#define __XRDFILECACHE_CFI_HH__

#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <XrdSys/XrdSysPthread.hh>
class XrdOssDF;

#define cfiBIT(n)       (1ULL << (n))

namespace XrdFileCache
{
class CacheStats;

class CacheFileInfo
{
public:
   CacheFileInfo();
   ~CacheFileInfo();

   void setBit(int i);
   void resizeBits(int s);
   void setComplete(int c);

   int Read(XrdOssDF* fp);
   void  WriteHeader(XrdOssDF* fp);
   void AppendIOStat(const CacheStats* stat, XrdOssDF* fp);

   bool isAnythingEmptyInRng(int firstIdx, int lastIdx) const;

   int getSizeInBytes() const;
   int getSizeInBits() const;
   int getHeaderSize() const;

   long long getBufferSize() const;
   bool testBit(int i) const;

   bool isComplete() const;
   void checkComplete();

   void print() const;


private:
   long long    m_bufferSize;
   int    m_sizeInBits; // number of file blocks
   char*  m_buff;
   int    m_accessCnt;

   bool   m_complete; //cached
  
  XrdSysMutex  m_writeMutex;
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

inline long long CacheFileInfo::getBufferSize() const
{
   return m_bufferSize;
}
}
#endif
