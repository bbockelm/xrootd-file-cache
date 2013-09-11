#ifndef __XRDFILECACHE_CFI_HH__
#define __XRDFILECACHE_CFI_HH__

#include <stdio.h>
#include <time.h>

class XrdOssDF;

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
   bool testBit(int i) const;

   bool isComplete() const;
   void checkComplete();

   void touch();
   void print();


private:
   time_t m_accessTime;
   int    m_accessCnt;
   int    m_sizeInBits;

   char*  m_buff;

   bool   m_complete; //cached
};
}

#endif
