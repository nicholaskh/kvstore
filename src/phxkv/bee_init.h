#ifndef __BEE_INIT__H
#define __BEE_INIT__H

#include "kv_grpc_server.h"
#include <iostream>
#include <memory>
#include <string>
#include "log.h"
#include "options.h"
#include <grpc++/grpc++.h>
#include "metrics.h"
#include "sysconfig.h"
using namespace phxpaxos;

using namespace std;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
#include <memory>
class bee_init
{
private:
    /* data */
public:
    bee_init(/* args */);
    ~bee_init();
    int start();
    void stop();

    PhxKVServiceImpl* oPhxKVServer;
    
    std::shared_ptr<StatusMng> statusMng;

};




#endif