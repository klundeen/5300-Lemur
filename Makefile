# Makefile, Diego Hoyos, Erika Skornia-Olsen
# Seattle University, CPSC5300, Spring 2022

#CCFLAGS = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
#COURSE = /usr/local/db6
#INCLUDE_DIR = $(COURSE)/include
#LIB_DIR = $(COURSE)/lib

# list of all the compiled object files needed for sql5300
#OBJS = sql5300.o

# rule for compilation
#%.0: %.cpp
#	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o $@ $<

# link to create executable
#sql5300: $(OBJS)
#	g++ -L$(LIB_DIR) -o $@ $< -ldb_cxx -lsqlparser

# remove non-source files
#clean:
#	rm -f sql5300 *.o

sql5300: sql5300.o
	g++ -L/usr/local/db6/lib -o $@ $< -ldb_cxx -lsqlparser

sql5300.o : sql5300.cpp
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

clean :
	rm -f sql5300.o sql5300
