#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

//using namespace std;
using namespace hsql;

// executes SQL queries
//std::string execute(const SQLStatement stmt)
std::string execute(SQLParserResult* rslt)
{
  return "Test";
}
// Helper function
std::string toLower(std::string str);

int main(int argc, char **argv)
{
  if (argc != 2)
  {
    fprintf(stderr, "Usage: need file directory\n");
    return -1;
  }

  const char *home = std::getenv("HOME");
  std::string envDir = std::string(home) + "/" + argv[1];
  
  DbEnv env(0U);
  env.set_message_stream(&std::cout);
  env.set_error_stream(&std::cerr);
  env.open(envDir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

  std::string query;
  while (true)
  {
    std::cout << "SQL> ";
    getline(std::cin, query);
    if (toLower(query) == "quit")
      break;
    SQLParserResult* result = SQLParser::parseSQLString(query.c_str());
    if (result->isValid())
    {
      std::cout << execute(result) << std::endl;
    }
    else
      std::cout << "invalid" << std::endl;
  }
  return 0;
}

std::string toLower(std::string str)
{
  for (unsigned int i = 0; i < str.length(); i++)
    str[i] = tolower(str[i]);
  return str;
}
