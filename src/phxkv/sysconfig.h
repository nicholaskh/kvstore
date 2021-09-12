
#pragma once
#include "INIReader.h"
#include <vector>

class CConfig
{

public:
  CConfig();
  ~CConfig();

  class grpc
  {
  public:
    std::string address;
    int port;
    int num_cqs;
    int min_pollers;
    int max_pollers;
    int cq_timeout_msec;
    int max_recv_msg_size;
    int max_send_msg_size;
    int client_num;
    int msgrecv_num;
  };
  class kvdb
  {
  public:
    std::string dataPath;
    std::string column_family;
  };
  class paxosnode
  {
  public:
    std::string mynode;
    std::string nodelist;
    std::string column_family;
    std::string log_path;
    int group_cnt;
    int min_group;
    int max_group;
    int lease_time;//毫秒
    int lease_ratio;//系数
     
  };
  class prometheus
  {
  public:
    std::string listen;
  };

  class log
  {
  public:
    int level;
    std::string path;
    int type;
  };

  class rocksdb
  {
  public:
    int max_file_opening_threads;
    int max_background_jobs;
    int max_subcompactions;
    int max_write_buffer_number;
    int level0_slowdown_writes_trigger;
    int level0_stop_writes_trigger;
    int level0_file_num_compaction_trigger;
    long soft_pending_compaction_bytes_limit;
    long  hard_pending_compaction_bytes_limit;
    size_t write_buffer_size;
    int min_write_buffer_number_to_merge;
    size_t db_write_buffer_size;
    bool optimize_filters_for_hits;
    int max_open_files;
    bool wal_disable;
    int stats_dump_period_sec;
    int row_cache_size;//监控row cache
    int paxoslog_max_size; //paxoslog文件大小,默认100
  };
 
  class node
  {
  public:
    int id;
    std::string version;
  };


  class backup
  {
  public:
    std::string backup_dir;
    int max_threads;
    int backup_rate_limit;
    std::string backup_checkpoints_dir;
    int32_t backup_checkpoints_max_nums;
    int32_t backup_checkpoints_expired;
    int backup_checkpoints_purge_duration;
    int backup_hour;
    int backup_minute;
  };

  class rsyncdb{
    public:
    int rsync_port;
    std::string checkpoint_dir;
    std::string rsync_conf_dir;
    std::string rsync_user;
    std::string rsync_pwd;
  };


  backup backup;
   
  node node;
  grpc grpc;
  kvdb kvdb;
  prometheus prometheus;
   
  log log;
  paxosnode paxosnodes;
  rocksdb rocksdb;
  rsyncdb  dbrsync;

  static bool Init(const std::string &path);
   
  static const CConfig *Get();

  void UnInit();

private:
  static CConfig *s_conf;
};
