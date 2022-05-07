CXX := g++
CXXFLAGS := -I/usr/local/db6/include -std=c++17 -Wall -g
LFLAGS := -L/usr/local/db6/lib -ldb_cxx -lsqlparser

sql5300: sql5300.o SQLExecutor.o heap_storage.o ParseTreeToString.o schema_tables.o
	$(CXX) $(LFLAGS) -o $@ $^

test: test.o heap_storage.o 
	$(CXX) $(LFLAGS) -o $@ $^

test.o: heap_storage_test.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

sql5300.o: sql5300.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

schema_tables.o: schema_tables.cpp schema_tables.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<

ParseTreeToString.o: ParseTreeToString.cpp ParseTreeToString.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<

SQLExecutor.o: SQLExecutor.cpp SQLExecutor.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<

heap_storage.o : heap_storage.cpp heap_storage.h storage_engine.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<


clean : 
	rm -f *.o *.001 *.002 *.003 *.db
