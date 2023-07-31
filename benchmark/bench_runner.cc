#include <gtest/gtest.h>
#include "util/logging.h"
#include "cmdline.h"

using std::string;
int main(int argc, char** argv){
    cmdline::parser cmd;
    cmd.add<string>("role", 'r', "the role of process", true, "", cmdline::oneof<string>("c", "s"));
    cmd.add<string>("ip", 'a', "server ip address", true);
    cmd.add<int>("thread", 't', "thread num", false, 1);
    cmd.add<int>("coro", 'c', "coroutine per thread", false, 1);
    cmd.add<int>("task", 'n', "total task",false,100000);
    cmd.add<string>("benchmark", 'b', "benchmark type", true);
    bool server = cmd.get<string>("role") == "s";
    if(server){

    }else{
        
    }
    return 0;
}