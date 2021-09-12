#include "sysconfig.h"

 


#define CGRPC "grpc"
#define CKVDB "kvdb"
#define PROMETHEUS "prometheus"
#define GEOSEARCH "geosearch"
#define LOG "log"
#define BINLOG "binlog"
#define ROCKSDB "rocksdb"
#define ZOOKEEPER "zookeeper"
#define NODE "node"
#define ENGINE "engine"
#define BACKUP "backup"
#define PIKA "pika"

CConfig::CConfig()
{
}

CConfig::~CConfig()
{
}

const static int defaultMaxSendMsgSize = 100 * 4 * 1024 * 1024;
const static int defaultMaxRecvMsgSize = 100 * 4 * 1024 * 1024;

bool CConfig::Init(const std::string &path)
{
    s_conf = new CConfig();

    INIReader reader(path);

    if (reader.ParseError() != 0)
    {
        printf("load config failed : %s", path.c_str());
        return false;
    }

    // node
    s_conf->node.id = (int)reader.GetInteger(NODE, "id", 0);
    s_conf->node.version = reader.Get(NODE, "version", "0.1.0");

    // grpc
    s_conf->grpc.address = reader.Get(CGRPC, "address", "localhost");
    s_conf->grpc.port = reader.GetInteger(CGRPC, "port", 8000);
    s_conf->grpc.num_cqs = reader.GetInteger(CGRPC, "NUM_CQS", 48);
    s_conf->grpc.min_pollers = reader.GetInteger(CGRPC, "MIN_POLLERS", 1);
    s_conf->grpc.max_pollers = reader.GetInteger(CGRPC, "MAX_POLLERS", 2);
    s_conf->grpc.cq_timeout_msec = reader.GetInteger(CGRPC, "CQ_TIMEOUT_MSEC", 1000);
    s_conf->grpc.max_recv_msg_size = reader.GetInteger(CGRPC, "RECV_MSG_SIZE", defaultMaxRecvMsgSize);
    if (s_conf->grpc.max_recv_msg_size < defaultMaxRecvMsgSize)
        s_conf->grpc.max_recv_msg_size = defaultMaxRecvMsgSize;
    s_conf->grpc.client_num = reader.GetInteger(CGRPC, "CLIENT_NUM", 0);
    s_conf->grpc.msgrecv_num  = reader.GetInteger(CGRPC, "MSGRECV_NUM", 0);
     

    s_conf->grpc.max_send_msg_size = reader.GetInteger(CGRPC, "RECV_MSG_SIZE", defaultMaxSendMsgSize);
    if (s_conf->grpc.max_send_msg_size < defaultMaxSendMsgSize)
        s_conf->grpc.max_send_msg_size = defaultMaxSendMsgSize;
     

    // kvdb
    s_conf->kvdb.dataPath = reader.Get(CKVDB, "data_path", "");
    s_conf->kvdb.column_family = reader.Get(CKVDB, "column_family", "");

    //prometheus
    s_conf->prometheus.listen = reader.Get(PROMETHEUS, "listen", ":9999");

    // log
    s_conf->log.level = reader.GetInteger(LOG, "level", 1);
    s_conf->log.path = reader.Get(LOG, "path", "");
    s_conf->log.type = reader.GetInteger(LOG, "type", 1);

    // rocksdb
    s_conf->rocksdb.max_file_opening_threads = reader.GetInteger(ROCKSDB, "max_file_opening_threads", 16);
    s_conf->rocksdb.max_background_jobs = reader.GetInteger(ROCKSDB, "max_background_jobs", 2);
    s_conf->rocksdb.max_subcompactions = reader.GetInteger(ROCKSDB, "max_subcompactions", 1);
    s_conf->rocksdb.max_write_buffer_number = reader.GetInteger(ROCKSDB, "max_write_buffer_number", 2);
    s_conf->rocksdb.level0_slowdown_writes_trigger = reader.GetInteger(ROCKSDB, "level0_slowdown_writes_trigger", 20);
    s_conf->rocksdb.level0_file_num_compaction_trigger = reader.GetInteger(ROCKSDB, "level0_file_num_compaction_trigger", 4);
      
    s_conf->rocksdb.level0_stop_writes_trigger = reader.GetInteger(ROCKSDB, "level0_stop_writes_trigger", 36);
    s_conf->rocksdb.soft_pending_compaction_bytes_limit = (long  )reader.GetLongLong(ROCKSDB, "soft_pending_compaction_bytes_limit", 64 * 1073741824l);
    s_conf->rocksdb.hard_pending_compaction_bytes_limit = (long)reader.GetLongLong(ROCKSDB, "hard_pending_compaction_bytes_limit", 256 * 1073741824l);
    s_conf->rocksdb.write_buffer_size = (size_t)reader.GetInteger(ROCKSDB, "write_buffer_size", 64 << 20);
    s_conf->rocksdb.min_write_buffer_number_to_merge = reader.GetInteger(ROCKSDB, "min_write_buffer_number_to_merge", 1);
    s_conf->rocksdb.db_write_buffer_size = (size_t)reader.GetInteger(ROCKSDB, "db_write_buffer_size", 0);
    s_conf->rocksdb.optimize_filters_for_hits = reader.GetBoolean(ROCKSDB, "optimize_filters_for_hits", false);
    s_conf->rocksdb.max_open_files = reader.GetInteger(ROCKSDB, "max_open_files", -1);
    s_conf->rocksdb.wal_disable = reader.GetBoolean(ROCKSDB, "wal_disable", false);
    s_conf->rocksdb.stats_dump_period_sec = reader.GetInteger(ROCKSDB, "stats_dump_period_sec", 300);
    s_conf->rocksdb.row_cache_size = reader.GetInteger(ROCKSDB, "row_cache_size", 1024*1024*64 );
    s_conf->rocksdb.paxoslog_max_size = reader.GetInteger(ROCKSDB, "paxoslog_max_size", 104857600 );

     
    // backup
    s_conf->backup.backup_dir = reader.Get(BACKUP, "backup_dir", "");
    s_conf->backup.backup_rate_limit = reader.GetInteger(BACKUP, "backup_rate_limit", 300 * 1024 * 1024);
    s_conf->backup.max_threads = reader.GetInteger(BACKUP, "max_threads", 1);
    s_conf->backup.backup_checkpoints_dir = reader.Get(BACKUP, "backup_checkpoints_dir", "");
    s_conf->backup.backup_checkpoints_max_nums = reader.GetInteger(BACKUP, "backup_checkpoints_max_nums", 1);
    s_conf->backup.backup_checkpoints_expired = reader.GetInteger(BACKUP, "backup_checkpoints_expired", 187200);
    s_conf->backup.backup_checkpoints_purge_duration = reader.GetLongLong(BACKUP, "backup_checkpoints_purge_duration", 60 * 3600);
    s_conf->backup.backup_hour = reader.GetLongLong(BACKUP, "backup_hour", 2 );
    s_conf->backup.backup_minute = reader.GetLongLong(BACKUP, "backup_minute", 2 );

    //paxosnode
    s_conf->paxosnodes.mynode= reader.Get("paxosnode", "mynode", "");
    s_conf->paxosnodes.nodelist= reader.Get("paxosnode", "nodelist", "");
    s_conf->paxosnodes.log_path= reader.Get("paxosnode", "log_path", "");
    s_conf->paxosnodes.column_family= reader.Get("paxosnode", "column_family", "");
    s_conf->paxosnodes.group_cnt= reader.GetInteger("paxosnode", "group_cnt", 1);
    s_conf->paxosnodes.min_group = reader.GetInteger("paxosnode", "group_min", 1);;
    s_conf->paxosnodes.max_group = reader.GetInteger("paxosnode", "group_max", 1);;
    s_conf->paxosnodes.lease_time= reader.GetInteger("paxosnode", "lease_time", 10000);;
    s_conf->paxosnodes.lease_ratio= reader.GetInteger("paxosnode", "lease_ratio", 2);;

    //rsync
     
    std::string checkpoint_dir;
     s_conf->dbrsync.checkpoint_dir= reader.Get("rsync", "checkpoint_path", "");
     s_conf->dbrsync.rsync_port= reader.GetInteger("rsync", "port", 873 );
     s_conf->dbrsync.rsync_conf_dir= reader.Get("rsync", "rsync_conf_path", "");
     s_conf->dbrsync.rsync_user= reader.Get("rsync", "user", "");
     s_conf->dbrsync.rsync_pwd= reader.Get("rsync", "password", "");

    return true;
} 

void CConfig::UnInit()
{
    delete s_conf;
    s_conf = nullptr;
    return  ;
}

CConfig *CConfig::s_conf = nullptr;

const CConfig *CConfig::Get()
{
    return s_conf;
}



