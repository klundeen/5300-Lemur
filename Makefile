cpsc5300: cpsc5300.o
	g++ -L/usr/local/db6/lib -o $@ $< -ldb_cxx -lsqlparser

cpsc5300.o : cpsc5300.cpp
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

clean : 
	rm -f cpsc5300.o cpsc5300