#include <iostream>
#include "SqlShell.h"
#include "db_cxx.h"

using namespace std;

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }
    char *envHome = argv[1];

    SqlShellTest sqlShellTest(envHome);
    sqlShellTest.run();
    
    SqlShell shell;
    try {
        shell.run(envHome);
    } catch (DbException &exc) {
        cerr << "(sql5300: " << exc.what() << ")\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
