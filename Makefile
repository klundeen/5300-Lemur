SRC_DIR 	= src
BUILD_DIR 	= build
DB6_DIR		= /usr/local/db6
CPPFLAGS	= -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c
DB6_FLAGS	= -ldb_cxx -lsqlparser

sql5300: sql5300.o
	g++ -L$(DB6_DIR)/lib -o $@ $(BUILD_DIR)/$< $(DB6_FLAGS)

sql5300.o: $(SRC_DIR)/sql5300.cpp
	g++ -I$(DB6_DIR)/include $(CPPFLAGS) -o $(BUILD_DIR)/$@ $<

clean : 
	rm -rf $(BUILD_DIR)/* sql5300