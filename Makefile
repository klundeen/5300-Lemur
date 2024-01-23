COURSE_FLAGS = -ldb_cxx -lsqlparser
CCFLAGS      = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
COURSE       = /usr/local/db6
INCLUDE_DIR	 = $(COURSE)/include
LIB_DIR      = $(COURSE)/lib

SRC 		 = src
BUILD 		 = build
INCLUDE		 = include
OBJS         = $(BUILD)/*.o
HDRS		 = $(INCLUDE)/*.h
# OBJS         = $(BUILD)/sql5300.o $(BUILD)/heap_storage.o
# HDRS		 = $(INCLUDE)/heap_storage.h $(INCLUDE)/storage_engine.h

sql5300: sql5300.o heap_storage.o SqlShell.o SqlExec.o
	g++ -L$(LIB_DIR) -o $@ $(OBJS) -ldb_cxx -lsqlparser

sql5300.o 	: $(HDRS)
heap_storage.o : $(HDRS)
SqlShell.o : $(HDRS)
SqlExec.o : $(HDRS)

%.o: $(SRC)/%.cpp
	g++ -I$(INCLUDE_DIR) -I$(INCLUDE) $(CCFLAGS) -o $(BUILD)/$@ $<

clean:
	rm -f sql5300 $(BUILD)/*.o
