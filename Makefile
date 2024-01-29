# Makefile, Kevin Lundeen, Seattle University, CPSC5300, Winter Quarter 2024
# 
CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

SRC 		 = ./src
BUILD 		 = ./build
INCLUDE		 = ./include

# following is a list of all the compiled object files needed to build the sql5300 executable
OBJS       = sql5300.o heap_storage.o
HDRS	   = heap_storage.h storage_engine.h
HDRS_PATH  = $(addprefix $(INCLUDE)/, $(HDRS))
OBJS_PATH  = $(addprefix $(BUILD)/, $(OBJS))

# Rule for linking to create the executable
# Note that this is the default target since it is the first non-generic one in the Makefile: $ make
sql5300: mkdir_build $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS_PATH) -ldb_cxx -lsqlparser

sql5300.o : $(HDRS_PATH)
heap_storage.o : $(HDRS_PATH)

# General rule for compilation
%.o: $(SRC)/%.cpp
	g++ -I$(INCLUDE_DIR) -I$(INCLUDE) $(CCFLAGS) -o $(BUILD)/$@ $<

mkdir_build:
	mkdir -p $(BUILD)

# Rule for removing all non-source files (so they can get rebuilt from scratch)
# Note that since it is not the first target, you have to invoke it explicitly: $ make clean
clean:
	rm -rf $(BUILD)
	rm -f sql5300

