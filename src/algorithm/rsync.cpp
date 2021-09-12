#include <fstream>
#include <sstream>
#include <string>
#include "rsync.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include "util.h"

RsyncService::RsyncService()
{
 
    rsync_path_ = CConfig::Get()->dbrsync.rsync_conf_dir;
   
    secret_file = (rsync_path_ + kRsyncSecretFile);

    rsync_user =CConfig::Get()->dbrsync.rsync_user;

    rsync_pawd =CConfig::Get()->dbrsync.rsync_pwd;

    rsync_module = kRsynModel ;

    rsync_module_receive_dir = CConfig::Get()->kvdb.dataPath+"g0/";//
    pid_path_ = rsync_path_ + kRsyncPidFile;
    rsync_port = CConfig::Get()->dbrsync.rsync_port;
    
}
RsyncService::~RsyncService()
{

}
int RsyncService::StartRsync( ) {
  int ret = 0 ;
  ret = phxpaxos::FileUtils::CreateDir( rsync_path_ );
  if( ret != 0 ){
      ERROR_LOG("CreateDir error %s", rsync_path_.c_str() );
      return -1;
  }
  // Generate secret file
  ret = CreateSecretFile();
  if(ret!= 0 ){
    ERROR_LOG("CreateSecretFile error..ret=%d",ret);
    return -1;
  }

  // Generate conf file
  std::string conf_file(rsync_path_ + kRsyncConfFile);
  
  std::ofstream conf_stream(conf_file.c_str());
  if (!conf_stream) {
    ERROR_LOG("Open rsync conf file failed!");
    return -1;
  }

    conf_stream << "uid = dev" << std::endl;
    conf_stream << "gid = dev" << std::endl;
    conf_stream << "use chroot = no" << std::endl;
    conf_stream << "max connections = 10" << std::endl;
    conf_stream << "lock file = " << rsync_path_ + kRsyncLockFile << std::endl;
    conf_stream << "log file = " << rsync_path_ + kRsyncLogFile << std::endl;
    conf_stream << "pid file = " << rsync_path_ + kRsyncPidFile << std::endl;
    conf_stream << "port = "<< rsync_port<< std::endl;
    conf_stream << "list = no" << std::endl;
    conf_stream << "strict modes = no" << std::endl;
    conf_stream << "auth user = " <<rsync_user<< std::endl;//虚拟用户
    conf_stream << "secrets file = " << secret_file << std::endl;
    conf_stream << "[" << rsync_module << "]" << std::endl;
    conf_stream << "path = " << rsync_module_receive_dir << std::endl;//同步到db保存的路径
    conf_stream << "read only = no" << std::endl;
    conf_stream.close();

   
  std::stringstream ss;
  ss << "rsync --daemon --config=" << conf_file;
  std::string rsync_start_cmd = ss.str();
  ret = system(rsync_start_cmd.c_str());//启动进程
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {

    INFO_LOG("Start rsync deamon ok" );    
    return 0;
  }
  ERROR_LOG("Start rsync deamon failed : %d!", ret);
  return ret;

}
//停止服务
int RsyncService::StopRsync( ) {
  
  if (!FileExists(pid_path_)) {
    ERROR_LOG("no rsync pid file found,Rsync deamon is not exist %s",pid_path_.c_str() );
    return 0; 
  }

  //读取进程ID
  ifstream readstream( pid_path_.c_str(),std::ios::in);
  if(readstream.fail() ){
    ERROR_LOG("pid_path_ not exist %s..",pid_path_.c_str() );
    return -1;
  }
  
  std::string line="";
  getline( readstream, line);
  readstream.close();
  
  pid_t pid = atoi(line.c_str() );
  if (pid <= 1) {
    ERROR_LOG("read rsync pid err");
    return 0;
  }

  std::string rsync_stop_cmd = "kill -- -$(ps -o pgid= " + std::to_string(pid) + ")";
  
  int ret = system(rsync_stop_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    INFO_LOG("Stop rsync success!");
  } else {
    ERROR_LOG("Stop rsync deamon failed : %d!", ret);
  }
  
  return 1;
}
//发送文件
int RsyncService::RsyncSendFile(const std::string& local_file_path,
                 
                  const RsyncRemote& remote) {


  std::stringstream ss;
  const std::string& secret_file_path = secret_file ;

  ss << """rsync -avP"
    << " --password-file=" << secret_file_path
    << " --port=" << remote.port
    << " " << local_file_path
    << " " << rsync_user << "@" << remote.host
    << "::" << kRsynModel ; //<< "/" << remote_file_path;
  std::string rsync_cmd = ss.str();
  int ret = system(rsync_cmd.c_str());
  INFO_LOG("rsync info.%s " , rsync_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    INFO_LOG("system cmd ok");
    return 0;
  }
  INFO_LOG("system cmd=%d",ret );
  return ret;
}

//创建密钥文件
int RsyncService::CreateSecretFile()
{

  std::ofstream secret_stream(secret_file.c_str());
  if (!secret_stream) {
    ERROR_LOG("Open rsync secret file failed!");
    return -1;
  }
  secret_stream <<rsync_pawd;
  secret_stream.close();
  std::string cmd = "chmod 600 " + secret_file;
  int ret = system(cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    return 0;
  }
  return 0;
}
 
bool RsyncService::CheckRsyncAlive()//服务存活
{
  if(  FileExists(pid_path_)){
      std::ifstream pidfile(pid_path_.c_str() );
      std::string pidstr;
      if (std::getline(pidfile, pidstr)){
         int pid = atoi(pidstr.c_str() );
         if (0 == kill(pid, 0)){
            return true;
         }else{
            ERROR_LOG( "rsyn pid doest not exist :" );
        }
      }
  }else{
    ERROR_LOG( "rsyn pid file doest not exist :" );
  }

  return false;
}


bool RsyncService::CreatePath(const std::string& path)
{
    char DirName[256];  
    strcpy(DirName, path.c_str() );  
    int i,len = strlen(DirName);
    for(i=1; i<len; i++)  
    {  
        if(DirName[i]=='/')  
        {  
            DirName[i] = 0; 
            if(access(DirName, NULL)!=0)  
            {  
                if(mkdir(DirName, 0755)==-1)  
                {   
                    printf("mkdir   error\n");   
                    return false;   
                }  
            }  
            DirName[i] = '/';  
          }  
      }  

      return true;  
}


bool RsyncService::FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

RsyncService* global_rsync;