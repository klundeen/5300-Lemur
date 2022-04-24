# Makefile, Diego Hoyos, Erika Skornia-Olsen
# Seattle University, CPSC5300, Spring 2022

CCFLAGS = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
COURSE = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR = $(COURSE)/lib

# list of all the compiled object files
OBJS = sql5300.o heap_storage.o

# link to create executable
sql5300 : $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $< -ldb_cxx -lsqlparser

sql5300.o : sql5300.cpp heap_storage.h storage_engine.h
heap_storage.o : heap_storage.h storage_engine.h

# rule for compilation
%.0: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o $@ $<

# remove non-source files
clean :
	rm -f sql5300 *.o
