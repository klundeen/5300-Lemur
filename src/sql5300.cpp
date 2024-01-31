#include "heap_storage.h"
#include "sql_shell.h"

DbEnv *_DB_ENV;

/**
 * Main entry point of the sql5300 program
 * @args dbenvpath  the path to the BerkeleyDB database environment
 */
int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: sql5300 <dbenvpath>" << std::endl;
        return EXIT_FAILURE;
    }
    DbEnv env(0U);
    SqlShell shell;
    shell.initializeDbEnv(argv[1], &env);
    shell.run();    
    return EXIT_SUCCESS;
}