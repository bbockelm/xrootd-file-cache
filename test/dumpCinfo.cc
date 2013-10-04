#include <stdio.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>

#define BIT(n)       (1ULL << (n))

class Rec
{
private:
   long long    m_bufferSize;
   time_t m_accessTime;
   int    m_accessCnt;
   int    m_sizeInBits;

   char*  m_buff;

   bool   m_complete; //cached
public:
   Rec(): m_bufferSize(1024*1024), m_accessTime(0), m_accessCnt(0),
          m_buff(0), m_sizeInBits(0),
          m_complete(false) 
   {
   }

   ~Rec() {
      if (m_buff) delete [] m_buff;
   }

   void setBit(int i)
   {
      int cn = i/8;
      int off = i - cn*8;
      m_buff[cn] |= BIT(off);
   }

   bool testBit(int i) const
   {
      int cn = i/8;
      int off = i - cn*8;
      return (m_buff[cn] & BIT(off)) == BIT(off);
   }
   void resizeBits(int s)
   {
      m_sizeInBits = s;
      m_buff = (char*)malloc(getSizeInBytes());
      memset(m_buff, 0, getSizeInBytes());
   }

   int getSizeInBytes() const
   {
      return (m_sizeInBits-1)/8 + 1;
   }

   int getSizeInBits() const
   {
      return m_sizeInBits;
   }

   bool isComplete() const {
      return m_complete;
   }

   void setComplete(int c) {
      m_complete = c;
   }
   
   void touch() {
      m_accessTime = time(0);
      m_accessCnt ++;
   }

//______________________________________________________________________________

   
   int read(FILE* fp)
   {
      int off = fread(&m_bufferSize, sizeof(long long), 1, fp);
      if (off <=0) return -1;
      off = fread(&m_accessTime, sizeof(time_t), 1, fp);
      off = fread(&m_accessCnt, sizeof(int), 1, fp);

      int sb;
      if (fread(&sb, sizeof(int), 1, fp) != 1) return -1;
      resizeBits(sb);
      fread(m_buff,getSizeInBytes() , 1, fp);
      m_complete = isAnythingEmptyInRng(0, sb) ? false : true;
      return 1;
   }
//______________________________________________________________________________


   int write(FILE* fp)
   {
      if (fwrite(&m_bufferSize, sizeof(long long), 1, fp) != 1) return -1;
      if (fwrite(&m_accessTime, sizeof(time_t), 1, fp) != 1) return -1;
      if (fwrite(&m_accessCnt, sizeof(int), 1, fp) != 1) return -1; 

      int nb = getSizeInBits();
      fwrite(&nb, sizeof(int), 1, fp); 
      fwrite(m_buff, getSizeInBytes(), 1, fp);
   }
//______________________________________________________________________________



   bool isAnythingEmptyInRng(int firstIdx, int lastIdx)
   {
      for (int i = firstIdx; i <= lastIdx; ++i)
         if(! testBit(i)) return true;
   }
//______________________________________________________________________________


   void print()
   {

      int cntd = 0;
      //    printf("printing b vec\n");
      for (int i = 0; i < m_sizeInBits; ++i)
      {
         //         printf("%d ", testBit(i));
         cntd++;

      }
      //      printf("\n");

      printf("bufferSize %lld accest %d accessCnt %d nBlocks %d nDownlaoded %d %s\n",m_bufferSize,(int) m_accessTime, m_accessCnt, m_sizeInBits , cntd, (m_sizeInBits == cntd) ? " complete" :"");
   }

};

int main(int argc, char* argv[])
{/*
   {
      Rec r;
      FILE* f = fopen(argv[1],"w+");
      r.resizeBits(19);
      r.setBit(0);
      r.setBit(1);
      r.setBit(2);
      r.setBit(7);
      for(int i = 8; i < 18; ++i) {
         r.setBit(8+i);
      }
      r.touch();

      int res = r.write(f);
      printf("writing res = %d .....\n", res);
      r.print();
      fclose(f);
   }
 */
   {
      // printf("reading .....\n");
      Rec r;
      FILE* f = fopen(argv[1],"r");
      if ( r.read(f) < 0) {
         printf("read failed \n");
         exit(1);
      }

      r.print();
      r.touch();
      fclose(f);
      r.isAnythingEmptyInRng(7, 11);

   }

}
