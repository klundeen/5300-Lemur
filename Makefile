CCFLAGS    	= -std=c++11 -c -g
CCFLAGS     	= -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
INCLUDE_DIR 	= /usr/local/db6/include
LIB_DIR     	= /usr/local/db6/lib

SRC 		= ./src
BUILD 		= ./build
INCLUDE		= ./include

OBJS       	= sql5300.o slotted_page.o heap_table.o heap_file.o sql_shell.o sql_exec.o schema_tables.o parse_tree_to_string.o test_heap_storage.o storage_engine.o
HDRS	   	= heap_storage.h heap_table.h heap_file.h slotted_page.h storage_engine.h debug.h schema_tables.h parse_tree_to_string.h
HDRS_PATH  	= $(addprefix $(INCLUDE)/, $(HDRS))
OBJS_PATH  	= $(addprefix $(BUILD)/, $(OBJS))

sql5300: mkdir_build $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS_PATH) -ldb_cxx -lsqlparser

sql5300.o 				: $(HDRS_PATH)
storage_engine.o		: $(HDRS_PATH)
heap_storage.o 			: $(HDRS_PATH)
slotted_page.o 			: $(HDRS_PATH)
heap_table.o 			: $(HDRS_PATH)
heap_file.o 			: $(HDRS_PATH)
sql_shell.o 			: $(HDRS_PATH)
sql_exec.o 				: $(HDRS_PATH)
schema_tables.o			: $(HDRS_PATH)
parse_tree_to_string.o	: $(HDRS_PATH)

%.o: $(SRC)/%.cpp
	g++ -I$(INCLUDE_DIR) -I$(INCLUDE) $(CCFLAGS) -o $(BUILD)/$@ $<

mkdir_build:
	mkdir -p $(BUILD)

clean:
	rm -f sql5300 $(BUILD)/*.o
