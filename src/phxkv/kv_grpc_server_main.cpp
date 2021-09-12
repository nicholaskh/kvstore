/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#include "kv_grpc_server.h"
#include <iostream>
#include <memory>
#include <string>
#include "log.h"
#include "sysconfig.h"
#include <grpc++/grpc++.h>

using namespace phxpaxos;
using namespace phxkv;
using namespace std;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
#include "log.h"
#include "grpcpp/resource_quota.h"
#include "status_mng.h"
#include "rsync.h"
#include "learner.h"
extern Metrics *g_pmetrics;
int RUN_FLAG =0;// 0 正常启动  1 二次重启
 
uint64_t global_first_learn_id=0;
int global_first_learn = 1;//初次运行

int parse_ipport(const char * pcStr, NodeInfo & oNodeInfo)
{
    char sIP[32] = {0};
    int iPort = -1;

    int count = sscanf(pcStr, "%[^':']:%d", sIP, &iPort);
    if (count != 2)
    {
        return -1;
    }

    oNodeInfo.SetIPPort(sIP, iPort);

    return 0;
}

int parse_ipport_list(const char * pcStr, NodeInfoList & vecNodeInfoList)
{
    string sTmpStr;
    int iStrLen = strlen(pcStr);

    for (int i = 0; i < iStrLen; i++)
    {
        if (pcStr[i] == ',' || i == iStrLen - 1)
        {
            if (i == iStrLen - 1 && pcStr[i] != ',')
            {
                sTmpStr += pcStr[i];
            }
            
            NodeInfo oNodeInfo;
            int ret = parse_ipport(sTmpStr.c_str(), oNodeInfo);
            if (ret != 0)
            {
                return ret;
            }

            vecNodeInfoList.push_back(oNodeInfo);

            sTmpStr = "";
        }
        else
        {
            sTmpStr += pcStr[i];
        }
    }

    return 0;
}

static void BeeGlogInit()
{
    auto level = CConfig::Get()->log.level; 
    auto path = CConfig::Get()->log.path;
    auto type = CConfig::Get()->log.type;

    auto initLog = g_logger.init(path.c_str(), (log_level_t)level, 0, 2, 0);//1 控制台输出   0 输出到日志
    assert(initLog);
    if (!initLog)
    {
        printf("init log failed!\n");
    }
     
    INFO_LOG("init config : explevel(%d), level(%d) ", level, g_logger.getLevel());
     
}
static bool BeeConfInit(const std::string &path)
{
    return CConfig::Init(path);
}
int main(int argc, char ** argv)
{
     
    if (argc < 2)
    {
        printf("error param....\n");
        return -1;
    }
    string config_path = argv[1];// 
    
    bool stat = BeeConfInit( config_path );
    if( !stat){
        ERROR_LOG(" BeeConfInit error...");
        exit(1);
    }
    BeeGlogInit();
    std::string proListen = CConfig::Get()->prometheus.listen;
    g_pmetrics = new Metrics(proListen);//启动监控 10.90.81.9:8000
    printf("g_pmetrics listen %s",proListen.c_str() );

    NodeInfo oMyNode;//自己本身的节点
    string mynode = CConfig::Get()->paxosnodes.mynode;//自己的节点IP
    if (parse_ipport( mynode.c_str(), oMyNode) != 0)
    {
        ERROR_LOG("parse myip:myport fail\n");
        return -1;
    }

    phxpaxos::NodeInfoList vecNodeInfoList;//所有的tcp节点
    string modelist = CConfig::Get()->paxosnodes.nodelist;//集群的节点IP
    if (parse_ipport_list( modelist.c_str(), vecNodeInfoList) != 0)
    {
        ERROR_LOG("parse ip/port list fail\n");
        return -1;
    }

    string sKVDBPath = CConfig::Get()->kvdb.dataPath;//kv路径
    string sPaxosLogPath = CConfig::Get()->paxosnodes.log_path;//paxos log路径

    int ret = 0; 

    PhxKVServiceImpl oPhxKVServer(oMyNode, vecNodeInfoList, sKVDBPath, sPaxosLogPath);
    ret = oPhxKVServer.Init();
    if (ret != 0)
    {
        printf("server init fail, ret %d\n", ret);
        return ret;
    }else{
        printf("server init ok\n" );
    }
    
    //开启状态线程
    std::shared_ptr<StatusMng> statusMng = std::make_shared< StatusMng>( oPhxKVServer.GetPhxKV() );
    statusMng->start();

    global_rsync = new RsyncService();
    global_rsync->StartRsync();//开启同步服务

    LearnerFlag::getinstance()->init_learn();

    string sServerAddress = CConfig::Get()->grpc.address;
    printf("listen...%s\n",sServerAddress.c_str() );
    ServerBuilder oBuilder;
    
    oBuilder.SetSyncServerOption(ServerBuilder::NUM_CQS, CConfig::Get()->grpc.num_cqs);
    oBuilder.SetSyncServerOption(ServerBuilder::MIN_POLLERS, CConfig::Get()->grpc.min_pollers);
    oBuilder.SetSyncServerOption(ServerBuilder::MAX_POLLERS, CConfig::Get()->grpc.max_pollers);
    oBuilder.SetSyncServerOption(ServerBuilder::CQ_TIMEOUT_MSEC, CConfig::Get()->grpc.cq_timeout_msec);
    oBuilder.SetMaxReceiveMessageSize(CConfig::Get()->grpc.max_recv_msg_size);
    oBuilder.SetMaxSendMessageSize(CConfig::Get()->grpc.max_send_msg_size);
   
    oBuilder.AddListeningPort(sServerAddress, grpc::InsecureServerCredentials());
    oBuilder.RegisterService(&oPhxKVServer);
    std::unique_ptr<Server> server(oBuilder.BuildAndStart());
    INFO_LOG("Server listening on %s.. ",sServerAddress.c_str() ) ;
    server->Wait();

    return 0;
}


