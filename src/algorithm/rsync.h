#ifndef RSYNC_H_
#define RSYNC_H_


#include <string>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "sysconfig.h"
const std::string kRsyncSecretFile = "rsync.secret";
const std::string kRsyncConfFile = "rsync.conf";
const std::string kRsyncLogFile = "rsync.log";
const std::string kRsyncPidFile = "rsync.pid";
const std::string kRsyncLockFile = "rsync.lock";
const std::string kRsynModel = "cpmodel";

 


class RsyncService {
public:
    RsyncService();
    ~RsyncService();

    
    struct RsyncRemote {
    std::string host;
     int port;
     
    RsyncRemote(const std::string& _host ,int _port)
    : host(_host),port(_port) {}
    };

int StartRsync( );
int StopRsync( );
//将本地的文件  发送到对端上
int RsyncSendFile(const std::string& local_file_path, const RsyncRemote& remote);

private:
    
    bool FileExists(const std::string& path); 
    bool CreatePath(const std::string& path);
    int CreateSecretFile();//创建密钥文件
    bool CheckRsyncAlive();//服务存活
   
    
    std::string rsync_path_;//根路径
    std::string pid_path_;//进程ID文件
    int rsync_port;

    std::string secret_file;//密钥文件绝对路径
    std::string rsync_user;//用户
    std::string rsync_pawd;//密码
    std::string rsync_module;//模块名
    std::string rsync_module_receive_dir;//同步后的文件保存路径
};

extern RsyncService* global_rsync;
#endif