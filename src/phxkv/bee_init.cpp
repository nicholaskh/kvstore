#include "bee_init.h"

int _parse_ipport(const char * pcStr, phxpaxos::NodeInfo & oNodeInfo)
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

int _parse_ipport_list(const char * pcStr, phxpaxos::NodeInfoList & vecNodeInfoList)
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
            int ret = _parse_ipport(sTmpStr.c_str(), oNodeInfo);
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


bee_init::bee_init(/* args */)
{
    INFO_LOG("bee_init.");
    g_pmetrics =NULL;
     
}

bee_init::~bee_init()
{
    INFO_LOG("~bee_init.start.");
    if( g_pmetrics!= NULL ){
        delete g_pmetrics;
        g_pmetrics=NULL;
    }
     
   if( oPhxKVServer!=NULL ){
      delete oPhxKVServer;
       
      oPhxKVServer=NULL;  
    }
     
}

int bee_init::start()
{
    std::string proListen = CConfig::Get()->prometheus.listen;
    g_pmetrics = new Metrics(proListen);//启动监控

    NodeInfo oMyNode;//自己本身的节点
    string mynode = CConfig::Get()->paxosnodes.mynode;//自己的节点IP
    if (_parse_ipport( mynode.c_str(), oMyNode) != 0)
    {
        ERROR_LOG("parse myip:myport fail\n");
        return -1;
    }

    phxpaxos::NodeInfoList vecNodeInfoList;//所有的节点
    string modelist = CConfig::Get()->paxosnodes.nodelist;//集群的节点IP
    if (_parse_ipport_list( modelist.c_str(), vecNodeInfoList) != 0)
    {
        ERROR_LOG("parse ip/port list fail\n");
        return -1;
    }

    string sKVDBPath = CConfig::Get()->kvdb.dataPath;//kv路径
    string sPaxosLogPath = CConfig::Get()->paxosnodes.log_path;//paxos log路径
   
     oPhxKVServer = new PhxKVServiceImpl(oMyNode, 
     vecNodeInfoList, sKVDBPath, sPaxosLogPath);
    
    int ret =oPhxKVServer->Init();//初始化本节点
    if (ret != 0)
    {
        ERROR_LOG("server init fail, ret %d\n", ret);
        return ret;
    }
    
    ret = oPhxKVServer->Start( );//运行本节点server
    if (ret != 0)
    {
        ERROR_LOG("server init fail, ret %d\n", ret);
        return ret;
    }
    //开启同步服务
    global_rsync = new RsyncService();
    global_rsync->StartRsync();//开启同步服务
    
    //开启状态线程
    statusMng = std::make_shared< StatusMng>( oPhxKVServer->GetPhxKV() );
    statusMng->start();
    oPhxKVServer->startServer();

    return 0;
}



void bee_init::stop()
{
    INFO_LOG("bee_init stop");
    global_rsync->StopRsync();
    statusMng->stop();
    oPhxKVServer->Stop();
}