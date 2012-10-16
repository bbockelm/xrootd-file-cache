xrdloc := /home/alja/xrd/test/322/

xrdfragcp: xrdfragcp.cxx
	${CXX} -g -o $@ -I${xrdloc}/include/xrootd -L${xrdloc}/lib64 -lXrdClient -lXrdUtils -pthread $<

clean:
	rm -rf xrdfragcp
