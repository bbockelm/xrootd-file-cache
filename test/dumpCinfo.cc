#include <stdio.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <vector>

#define BIT(n)       (1ULL << (n))

class Rec
{
public:
   struct AccessStat {
      time_t m_openTime;
      time_t m_closeTime;
      long long  m_bytesTransfered;
      int  m_numHit;
      int m_numMiss;
   };

public:
   long long    m_bufferSize;
   int    m_sizeInBits;
   char*  m_buff;
   std::vector<AccessStat> m_stat;

   bool   m_complete; //cached

   Rec(): m_bufferSize(1024*1024),
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
   

   //______________________________________________________________________________

   
   int read(FILE* fp)
   {
      int off = fread(&m_bufferSize, sizeof(long long), 1, fp);
      int sb;
      if (fread(&sb, sizeof(int), 1, fp) != 1) return -1;
      resizeBits(sb);
      fread(m_buff,getSizeInBytes() , 1, fp);
 
      AccessStat stat;
      for (std::vector<AccessStat>::iterator i = m_stat.begin(); i != m_stat.end(); ++i) {
         int off = fread( &(*i), sizeof(AccessStat), 1, fp);
         if (off != sizeof(AccessStat))
            break;
      }

      m_complete = isAnythingEmptyInRng(0, sb) ? false : true;
      return 1;
   }
   //______________________________________________________________________________


   int write(FILE* fp)
   {
      if (fwrite(&m_bufferSize, sizeof(long long), 1, fp) != 1) return -1;
      int nb = getSizeInBits();
      fwrite(&nb, sizeof(int), 1, fp); 
      fwrite(m_buff, getSizeInBytes(), 1, fp);

      for (std::vector<AccessStat>::iterator i = m_stat.begin(); i != m_stat.end(); ++i) {
         fwrite( &(*i), sizeof(AccessStat), 1, fp);
      }
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
         if (testBit(i)) cntd++;

      }
      //      printf("\n");

      printf("State === bufferSize %lld nBlocks %d nDownlaoded %d %s\n",m_bufferSize, m_sizeInBits , cntd, (m_sizeInBits == cntd) ? " complete" :"");

      int cnta = 0;
      for (std::vector<AccessStat>::iterator i = m_stat.begin(); i != m_stat.end(); ++i) 
      {
         AccessStat &a = *i; 
         printf("%dopen[%d] close[%d] transfer[%lld], hit[%d] miss[%d] \n",
                cnta++,
                a.m_openTime, a.m_closeTime,
                a.m_bytesTransfered, 
                a.m_numHit, a.m_numMiss);
      
      }
   }

};

int main(int argc, char* argv[])
{
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

      r.m_stat.push_back(Rec::AccessStat());
      r.m_stat.back().m_closeTime = time(0);
      int res = r.write(f);
      printf("writing res = %d .....\n", res);
      r.print();
      fclose(f);
   }

   {
      printf("\n\n reading .....\n");
      Rec r;
      FILE* f = fopen(argv[1],"r");
      if ( r.read(f) < 0) {
         printf("read failed \n");
         exit(1);
      }

      r.print();
      fclose(f);
      r.isAnythingEmptyInRng(7, 11);

   }

}
