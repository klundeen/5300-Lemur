#include "heap_storage.h"
#include "sql_shell.h"

/**
 * Main entry point of the sql5300 program
 * @args dbenvpath  the path to the BerkeleyDB database environment
 */
int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: cpsc5300: dbenvpath" << std::endl;
        return EXIT_FAILURE;
    }
    const char *HOME = argv[1];

    SqlShell shell;
    shell.initializeDbEnv(HOME);
    shell.run();

    return EXIT_SUCCESS;
}