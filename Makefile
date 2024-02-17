CCFLAGS    	= -std=c++11 -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
INCLUDE_DIR = /usr/local/db6/include
LIB_DIR    	= /usr/local/db6/lib
 
SRC 		= ./src
BUILD 		= ./build
INCLUDE		= ./include
 
OBJS       	= sql5300.o slotted_page.o heap_table.o heap_file.o sql_shell.o sql_exec.o schema_tables.o parse_tree_to_string.o test_heap_storage.o storage_engine.o
HDRS	   	= heap_storage.h heap_table.h heap_file.h slotted_page.h storage_engine.h debug.h schema_tables.h parse_tree_to_string.h
HDRS_PATH  	= $(addprefix $(INCLUDE)/, $(HDRS))
OBJS_PATH  	= $(addprefix $(BUILD)/, $(OBJS))
 
sql5300: mkdir_build $(OBJS_PATH)
	g++ -L$(LIB_DIR) -o $@ $(OBJS_PATH) -ldb_cxx -lsqlparser
 
$(BUILD)/%.o: $(SRC)/%.cpp $(HDRS_PATH)
	g++ -I$(INCLUDE_DIR) -I$(INCLUDE) $(CCFLAGS) -o $@ $<
 
mkdir_build:
	mkdir -p $(BUILD)
 
clean:
	rm -f sql5300 $(BUILD)/*.o