/**
 * @file sql5300.cpp
 * @author Luan (Remi) Ta, Helen Huang
 * @brief 
 * @version 0.1
 * @date 2022-03-30
 *  
 */

#include <string>
#include <iostream>
#include "db_cxx.h"
#include "SQLExecutor.h"

using namespace hsql;
DbEnv *_DB_ENV;

std::string execute(SQLParserResult* parseTree);

int main(int argc, char* argv[])
{
    if (argc != 2) {        
        std::cerr << "Usage: ./sql5300 env_path" << std::endl;
        return -1;
    }
    DbEnv* env = new DbEnv(0U);
    env->set_message_stream(&std::cout);
	env->set_error_stream(&std::cerr);
    env->open(argv[1], DB_CREATE, 0);

    std::string usrIn;
    std::cout << "SQL> ";
    std::getline(std::cin, usrIn);
    
    while (usrIn != "quit"){
        SQLParserResult* res = SQLParser::parseSQLString(usrIn);

        if (res->isValid()) {
            for (unsigned int i = 0; i < res->size(); i++) {
                std::cout << *std::unique_ptr<QueryResult>(SQLExec::execute(res->getStatement(i))) << std::endl;
            }
        } else {
            std::cout << "Invalid SQL: " << usrIn << std::endl;
        }

        std::cout << "SQL> ";
        std::getline(std::cin, usrIn);
    }
    SQLExec::closeMeta();
    delete _DB_ENV;
    return 0;
}