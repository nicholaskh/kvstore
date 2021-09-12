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

#pragma once
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/db.h"
#include "rocksdb/comparator.h"
#include <vector>
#include <string>
#include <map>
#include "comm_include.h"
#include "phxpaxos/storage.h"
#include "log_store.h"
#include "kv_encode.h"
#include "rocksdb/db.h"
#include "rocksdb/comparator.h"
#include <vector>
#include <string>
#include <map>
#include "comm_include.h"
#include "phxpaxos/storage.h"
#include "log_store.h"
#include "rocksdb/utilities/checkpoint.h"
#include "sysconfig.h"
#include "metrics.h"
#include <memory.h>
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util.h"
#include "rocksdb/cache.h"
#include "phxkv.pb.h"
#include <set>
#include "kv_const.h"
namespace phxpaxos
{
class PaxosComparator : public rocksdb::Comparator
{
public:
    int Compare(const rocksdb::Slice & a, const rocksdb::Slice & b) const;
    
    static int PCompare(const rocksdb::Slice & a, const rocksdb::Slice & b);

    const char * Name() const {return "PaxosComparator";}

    void FindShortestSeparator(std::string *, const rocksdb::Slice &) const {}

    void FindShortSuccessor(std::string *) const {}
};

//////////////////////////////////////////

#define MINCHOSEN_KEY ((uint64_t)-1)
#define SYSTEMVARIABLES_KEY ((uint64_t)-2)
#define MASTERVARIABLES_KEY ((uint64_t)-3)

class Database
{
public:
    Database();
    ~Database();

    int Init(const std::string & sDBPath, const int MyGroupCount);
    int InitColumnFamilyTtl();
    int InitBucket();//初始化对log db的监控
    const std::string GetDBPath();

    int ClearAllLog(const int iGroupIdx );

    int Get(const int iGroupIdx,const uint64_t llInstanceID, std::string & sValue);

    int Put(const WriteOptions & oWriteOptions, const int iGroupIdx,const uint64_t llInstanceID, const std::string & sValue);

    int Del(const int iGroupIdx,const WriteOptions & oWriteOptions, const uint64_t llInstanceID);

    int ForceDel(const int iGroupIdx,const WriteOptions & oWriteOptions, const uint64_t llInstanceID);

    int GetMaxInstanceID(const int iGroupIdx,uint64_t & llInstanceID);
    
    int SetMinChosenInstanceID(const int iGroupIdx,const WriteOptions & oWriteOptions, const uint64_t llMinInstanceID);

    int GetMinChosenInstanceID(const int iGroupIdx,uint64_t & llMinInstanceID);

    int SetSystemVariables(const WriteOptions & oWriteOptions,  const int iGroupIdx,const std::string & sBuffer);

    int GetSystemVariables(const int iGroupIdx,std::string & sBuffer);

    int SetMasterVariables(const WriteOptions & oWriteOptions,const int iGroupIdx, const std::string & sBuffer);

    int GetMasterVariables(const int iGroupIdx,std::string & sBuffer);

    bool CreatCheckPoint(const std::string & path);


     
    //新增
    int KvGet(const std::string & sKey, std::string & sValue );

    int KvSet(const std::string & sKey, const std::string & sValue );;

    int KvDel(const std::string & sKey );

    bool KvBatchGet(const phxkv::KvBatchGetRequest* request, phxkv::KvBatchGetResponse* response );

    bool KvBatchSet(const ::phxkv::KvBatchPutRequest request);;

    int KvGetCheckpointInstanceID(const uint16_t iGroupIdx,uint64_t & llCheckpointInstanceID);;

    int KvSetCheckpointInstanceID(const uint16_t iGroupIdx,const uint64_t llCheckpointInstanceID);;

    void DumpRocksDBStats();

    bool GetLevelSstProperty(const string &property,string &_value);//获取属性 
    void SetLevelSstProperty(const string &cf ,int size );
    int GetUsedRowCache();//row_cache
    int Del_myself(const std::string & sKey);
    
public:
    int GetMaxInstanceIDFileID(const int iGroupIdx,std::string & sFileID, uint64_t & llInstanceID);

    int RebuildOneIndex( const int iGroupIdx, const uint64_t llInstanceID, const std::string & sFileID);
    
private:
    int ValueToFileID(const WriteOptions & oWriteOptions, const int iGroupIdx,const uint64_t llInstanceID, const std::string & sValue, std::string & sFileID);

    int FileIDToValue(const int iGroupIdx,const std::string & sFileID, uint64_t & llInstanceID, std::string & sValue);

    int GetFromLevelDB(const int iGroupIdx,const uint64_t llInstanceID, std::string & sValue);
    
    int PutToLevelDB(const bool bSync, const int iGroupIdx,const uint64_t llInstanceID, const std::string & sValue);
        
private:
    std::string GenGroupId(const uint16_t iGroupIdx);
    std::string GenKey(const uint64_t llInstanceID);

    const uint64_t GetInstanceIDFromKey(const std::string & sKey,uint16_t & iGroupIdx );//同时返回groupid
public:
    rocksdb::TransactionDB * GetDbObj(){
        return m_rocksdb;
    }
    rocksdb::ColumnFamilyHandle* GetCfData(){
        return vec_cf[1];
    }

    //hash相关
    bool HasOutTime(const uint32_t ttl);//判断是否过期
    int HashDel(const ::phxkv::HashRequest* request ,std::string hash_key );
    int HashSet( const ::phxkv::HashRequest* request ,std::string hash_key);
    int HashGet( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,string hsh_key);
    int HashGetAll(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response ,std::string hash_key  );
    int HashExist(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key );
     
    int HashIncrByInt(const ::phxkv::HashRequest* request,std::string hash_key,std::string & ret_value,const int type=0 );//1 代表float，默认为int
    int HashIncrByFloat(const ::phxkv::HashRequest* request,std::string hash_key,std::string & ret_value);
    int HashKeys( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key);
    int HashLen(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response ,std::string hash_key,uint64_t &length);
    int HashMget(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response ,std::string hash_key);
    int HashMset(const ::phxkv::HashRequest* request ,std::string hash_key );
    int HashSetNx(const ::phxkv::HashRequest* request ,std::string hash_key );
    int HashValues( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key);

    //list==================
    int ListLpush(const ::phxkv::ListRequest* request,std::string list_key,const int type=0);//默认为0，为1时表示ListLpushx
    int ListLpushx(const ::phxkv::ListRequest* request,std::string list_key);
    int ListLpop(const ::phxkv::ListRequest* request,std::string list_key,std::string& value);
    int ListLength(const ::phxkv::ListRequest* request,::phxkv::ListResponse* response,std::string list_key,uint64_t& length);
    int ListRpop(  const ::phxkv::ListRequest* request, std::string list_key ,std::string& value);//弹出表尾
    int ListRpopLpush( const  ::phxkv::ListRequest* request,const std::string list_key ,std::string &ret_value);//尾部进行头插
    int ListIndex(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key);//插入到指定位置
    int ListInsert(  const ::phxkv::ListRequest* request, std::string list_key );//插入到指定位置
    int ListRange(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key);
    int ListRem(  const ::phxkv::ListRequest* request, std::string list_key  );//删除N个等值元素
    int ListRemRight(  const ::phxkv::ListRequest* request, std::string list_key  );//从右边删除N个等值元素
    int ListSet(  const ::phxkv::ListRequest* request, std::string list_key );//根据下表进行更新元素
    int ListTtim(  const ::phxkv::ListRequest* request, std::string list_key );//保留指定区间元素，其余删除
    int ListRpush(  const ::phxkv::ListRequest* request, std::string list_key,const int type=0 );//尾插法,默认为0，为1时表示ListRpushx
    int ListRpushx(  const ::phxkv::ListRequest* request, std::string list_key );//链表存在时，才执行尾部插入

    //=============set===================
    int SAdd(  const ::phxkv::SetRequest* request,const std::string & set_key );
    int SRem(  const ::phxkv::SetRequest* request, const std::string & set_key);
    int SCard(  const ::phxkv::SetRequest* request, uint64_t & length,const std::string & set_key);
    int Members(const std::string & set_key , set<std::string>& resust);//读取集合元素
    int SMembers(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string & set_key);
    int DeleteKey(const std::string & set_key);
    int SUnionStore(  const ::phxkv::SetRequest* request,  const std::string & set_key);
    int SUnion(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response );
    int SInterStore(  const ::phxkv::SetRequest* request,  const std::string & set_key);
    int SInter(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response );
    int SDiffStore(  const ::phxkv::SetRequest* request, const std::string & set_key);
    int SDiff(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response );
    int SIsMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string & set_key);
    int SPop(  const ::phxkv::SetRequest* request, const std::string & set_key , std::string &ret_key);
    int SRandMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string & set_key);
    int SMove(  const ::phxkv::SetRequest* request, const std::string & set_key  );
    //====================zset=======================
    int ZAdd( const ::phxkv::ZsetRequest* request,const std::string & zset_key);//一个或多个
    int ZCard( const ::phxkv::ZsetRequest* request, uint64_t & length,const std::string & zset_key);//元素数量
    int ZCount( const ::phxkv::ZsetRequest* request, uint64_t& length,const std::string & zset_key);//有序集合中，在区间内的数量
    int ZRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);//有序集合区间内的成员
    int ZIncrby( const ::phxkv::ZsetRequest* request,  const std::string & zset_key,std::string & ret_key);//为score增加或减少
    struct ZsetStruct{
        std::string str_member;
        int64_t n_score;
        int8_t data_size=0;
        ZsetStruct(){
            str_member = "";
            n_score = 0;
            data_size=0;
        }
    };
    int MemBers(const std::string & zse_key, std::map<std::string , ZsetStruct * > &member_zset  );//根据集合名称，读取字段与score
    int ZUnionStore( const ::phxkv::ZsetRequest* ,const std::string & zset_key);//并集不输出
    int ZInterStore( const ::phxkv::ZsetRequest* request,const std::string & zset_key);//交集不输出
    int ZRangebyscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);
    int ZRem( const ::phxkv::ZsetRequest* request ,const std::string & zset_key);//移除有序集 key 中的一个或多个成员
    int ZRank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key);//返回member的排名，
    int ZRevrank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key);//member逆序排名
    int ZScore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);//member的score
    int ZREVRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);//指定区间成员，逆序输出
    int ZREVRangebylscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);
    int ZRemrangebyrank( const ::phxkv::ZsetRequest* request, const std::string & zset_key);
    int ZRemrangebyscore( const ::phxkv::ZsetRequest* request, const std::string & zset_key);
   //============删除=================
    bool KeyType(const string& key,int &key_type);
    int DelKey( const string& key );
    int ExpireKey(  const string& key,const int ttl);

public:
//private:
    rocksdb::TransactionDB * m_rocksdb;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    std::vector<rocksdb::ColumnFamilyHandle*> vec_cf;//column_family_handle
   
    rocksdb::Checkpoint* checkpoint_ptr;
    
    PaxosComparator m_oPaxosCmp;
    bool m_bHasInit;
    std::vector<LogStore *> vec_poValueStore;
    //LogStore * m_poValueStore;//只用一个文件
    
    std::string m_sDBPath;



private:    
    // rocks metrics
    std::shared_ptr<rocksdb::Statistics> stats;
    std::shared_ptr<rocksdb::Cache> def_row_cache;

    std::unique_ptr<Guage> BLOCK_CACHE_MISS;
    std::unique_ptr<Guage> BLOCK_CACHE_HIT;
    std::unique_ptr<Guage> BLOCK_CACHE_ADD;
    std::unique_ptr<Guage> BLOCK_CACHE_ADD_FAILURES;
    std::unique_ptr<Guage> BLOCK_CACHE_INDEX_MISS;
    std::unique_ptr<Guage> BLOCK_CACHE_INDEX_HIT;
    std::unique_ptr<Guage> BLOCK_CACHE_INDEX_ADD;
    std::unique_ptr<Guage> BLOCK_CACHE_INDEX_BYTES_INSERT;
    std::unique_ptr<Guage> BLOCK_CACHE_INDEX_BYTES_EVICT;
    std::unique_ptr<Guage> BLOCK_CACHE_FILTER_MISS;
    std::unique_ptr<Guage> BLOCK_CACHE_FILTER_HIT;
    std::unique_ptr<Guage> BLOCK_CACHE_FILTER_ADD;
    std::unique_ptr<Guage> BLOCK_CACHE_FILTER_BYTES_INSERT;
    std::unique_ptr<Guage> BLOCK_CACHE_FILTER_BYTES_EVICT;
    std::unique_ptr<Guage> BLOCK_CACHE_DATA_MISS;
    std::unique_ptr<Guage> BLOCK_CACHE_DATA_HIT;
    std::unique_ptr<Guage> BLOCK_CACHE_DATA_ADD;
    std::unique_ptr<Guage> BLOCK_CACHE_DATA_BYTES_INSERT;
    std::unique_ptr<Guage> BLOCK_CACHE_BYTES_READ;
    std::unique_ptr<Guage> BLOCK_CACHE_BYTES_WRITE;
    std::unique_ptr<Guage> BLOOM_FILTER_USEFUL;
    std::unique_ptr<Guage> BLOOM_FILTER_FULL_POSITIVE;
    std::unique_ptr<Guage> BLOOM_FILTER_FULL_TRUE_POSITIVE;
    std::unique_ptr<Guage> PERSISTENT_CACHE_HIT;
    std::unique_ptr<Guage> PERSISTENT_CACHE_MISS;
    std::unique_ptr<Guage> SIM_BLOCK_CACHE_HIT;
    std::unique_ptr<Guage> SIM_BLOCK_CACHE_MISS;
    std::unique_ptr<Guage> MEMTABLE_HIT;
    std::unique_ptr<Guage> MEMTABLE_MISS;
    std::unique_ptr<Guage> GET_HIT_L0;
    std::unique_ptr<Guage> GET_HIT_L1;
    std::unique_ptr<Guage> GET_HIT_L2_AND_UP;
    std::unique_ptr<Guage> COMPACTION_KEY_DROP_NEWER_ENTRY;
    std::unique_ptr<Guage> COMPACTION_KEY_DROP_OBSOLETE;
    std::unique_ptr<Guage> COMPACTION_KEY_DROP_RANGE_DEL;
    std::unique_ptr<Guage> COMPACTION_KEY_DROP_USER;
    std::unique_ptr<Guage> COMPACTION_RANGE_DEL_DROP_OBSOLETE;
    std::unique_ptr<Guage> COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE;
    std::unique_ptr<Guage> COMPACTION_CANCELLED;
    std::unique_ptr<Guage> NUMBER_KEYS_WRITTEN;
    std::unique_ptr<Guage> NUMBER_KEYS_READ;
    std::unique_ptr<Guage> NUMBER_KEYS_UPDATED;
    std::unique_ptr<Guage> BYTES_WRITTEN;
    std::unique_ptr<Guage> BYTES_READ;
    std::unique_ptr<Guage> NUMBER_DB_SEEK;
    std::unique_ptr<Guage> NUMBER_DB_NEXT;
    std::unique_ptr<Guage> NUMBER_DB_PREV;
    std::unique_ptr<Guage> NUMBER_DB_SEEK_FOUND;
    std::unique_ptr<Guage> NUMBER_DB_NEXT_FOUND;
    std::unique_ptr<Guage> NUMBER_DB_PREV_FOUND;
    std::unique_ptr<Guage> ITER_BYTES_READ;
    std::unique_ptr<Guage> NO_FILE_CLOSES;
    std::unique_ptr<Guage> NO_FILE_OPENS;
    std::unique_ptr<Guage> NO_FILE_ERRORS;
    std::unique_ptr<Guage> STALL_L0_SLOWDOWN_MICROS;
    std::unique_ptr<Guage> STALL_MEMTABLE_COMPACTION_MICROS;
    std::unique_ptr<Guage> STALL_L0_NUM_FILES_MICROS;
    std::unique_ptr<Guage> STALL_MICROS;
    std::unique_ptr<Guage> DB_MUTEX_WAIT_MICROS;
    std::unique_ptr<Guage> RATE_LIMIT_DELAY_MILLIS;
    std::unique_ptr<Guage> NO_ITERATORS;
    std::unique_ptr<Guage> NUMBER_MULTIGET_CALLS;
    std::unique_ptr<Guage> NUMBER_MULTIGET_KEYS_READ;
    std::unique_ptr<Guage> NUMBER_MULTIGET_BYTES_READ;
    std::unique_ptr<Guage> NUMBER_FILTERED_DELETES;
    std::unique_ptr<Guage> NUMBER_MERGE_FAILURES;
    std::unique_ptr<Guage> BLOOM_FILTER_PREFIX_CHECKED;
    std::unique_ptr<Guage> BLOOM_FILTER_PREFIX_USEFUL;
    std::unique_ptr<Guage> NUMBER_OF_RESEEKS_IN_ITERATION;
    std::unique_ptr<Guage> GET_UPDATES_SINCE_CALLS;
    std::unique_ptr<Guage> BLOCK_CACHE_COMPRESSED_MISS;
    std::unique_ptr<Guage> BLOCK_CACHE_COMPRESSED_HIT;
    std::unique_ptr<Guage> BLOCK_CACHE_COMPRESSED_ADD;
    std::unique_ptr<Guage> BLOCK_CACHE_COMPRESSED_ADD_FAILURES;
    std::unique_ptr<Guage> WAL_FILE_SYNCED;
    std::unique_ptr<Guage> WAL_FILE_BYTES;
    std::unique_ptr<Guage> WRITE_DONE_BY_SELF;
    std::unique_ptr<Guage> WRITE_DONE_BY_OTHER;
    std::unique_ptr<Guage> WRITE_TIMEDOUT;
    std::unique_ptr<Guage> WRITE_WITH_WAL;
    std::unique_ptr<Guage> COMPACT_READ_BYTES;
    std::unique_ptr<Guage> COMPACT_WRITE_BYTES;
    std::unique_ptr<Guage> FLUSH_WRITE_BYTES;
    std::unique_ptr<Guage> NUMBER_DIRECT_LOAD_TABLE_PROPERTIES;
    std::unique_ptr<Guage> NUMBER_SUPERVERSION_ACQUIRES;
    std::unique_ptr<Guage> NUMBER_SUPERVERSION_RELEASES;
    std::unique_ptr<Guage> NUMBER_SUPERVERSION_CLEANUPS;
    std::unique_ptr<Guage> NUMBER_BLOCK_COMPRESSED;
    std::unique_ptr<Guage> NUMBER_BLOCK_DECOMPRESSED;
    std::unique_ptr<Guage> NUMBER_BLOCK_NOT_COMPRESSED;
    std::unique_ptr<Guage> MERGE_OPERATION_TOTAL_TIME;
    std::unique_ptr<Guage> FILTER_OPERATION_TOTAL_TIME;
    std::unique_ptr<Guage> ROW_CACHE_HIT;
    std::unique_ptr<Guage> ROW_CACHE_MISS;
    std::unique_ptr<Guage> READ_AMP_ESTIMATE_USEFUL_BYTES;
    std::unique_ptr<Guage> READ_AMP_TOTAL_READ_BYTES;
    std::unique_ptr<Guage> NUMBER_RATE_LIMITER_DRAINS;
    std::unique_ptr<Guage> NUMBER_ITER_SKIP;
    std::unique_ptr<Guage> BLOB_DB_NUM_PUT;
    std::unique_ptr<Guage> BLOB_DB_NUM_WRITE;
    std::unique_ptr<Guage> BLOB_DB_NUM_GET;
    std::unique_ptr<Guage> BLOB_DB_NUM_MULTIGET;
    std::unique_ptr<Guage> BLOB_DB_NUM_SEEK;
    std::unique_ptr<Guage> BLOB_DB_NUM_NEXT;
    std::unique_ptr<Guage> BLOB_DB_NUM_PREV;
    std::unique_ptr<Guage> BLOB_DB_NUM_KEYS_WRITTEN;
    std::unique_ptr<Guage> BLOB_DB_NUM_KEYS_READ;
    std::unique_ptr<Guage> BLOB_DB_BYTES_WRITTEN;
    std::unique_ptr<Guage> BLOB_DB_BYTES_READ;
    std::unique_ptr<Guage> BLOB_DB_WRITE_INLINED;
    std::unique_ptr<Guage> BLOB_DB_WRITE_INLINED_TTL;
    std::unique_ptr<Guage> BLOB_DB_WRITE_BLOB;
    std::unique_ptr<Guage> BLOB_DB_WRITE_BLOB_TTL;
    std::unique_ptr<Guage> BLOB_DB_BLOB_FILE_BYTES_WRITTEN;
    std::unique_ptr<Guage> BLOB_DB_BLOB_FILE_BYTES_READ;
    std::unique_ptr<Guage> BLOB_DB_BLOB_FILE_SYNCED;
    std::unique_ptr<Guage> BLOB_DB_BLOB_INDEX_EXPIRED_COUNT;
    std::unique_ptr<Guage> BLOB_DB_BLOB_INDEX_EXPIRED_SIZE;
    std::unique_ptr<Guage> BLOB_DB_BLOB_INDEX_EVICTED_COUNT;
    std::unique_ptr<Guage> BLOB_DB_BLOB_INDEX_EVICTED_SIZE;
    std::unique_ptr<Guage> BLOB_DB_GC_NUM_FILES;
    std::unique_ptr<Guage> BLOB_DB_GC_NUM_NEW_FILES;
    std::unique_ptr<Guage> BLOB_DB_GC_FAILURES;
    std::unique_ptr<Guage> BLOB_DB_GC_NUM_KEYS_OVERWRITTEN;
    std::unique_ptr<Guage> BLOB_DB_GC_NUM_KEYS_EXPIRED;
    std::unique_ptr<Guage> BLOB_DB_GC_NUM_KEYS_RELOCATED;
    std::unique_ptr<Guage> BLOB_DB_GC_BYTES_OVERWRITTEN;
    std::unique_ptr<Guage> BLOB_DB_GC_BYTES_EXPIRED;
    std::unique_ptr<Guage> BLOB_DB_GC_BYTES_RELOCATED;
    std::unique_ptr<Guage> BLOB_DB_FIFO_NUM_FILES_EVICTED;
    std::unique_ptr<Guage> BLOB_DB_FIFO_NUM_KEYS_EVICTED;
    std::unique_ptr<Guage> BLOB_DB_FIFO_BYTES_EVICTED;
    std::unique_ptr<Guage> TXN_PREPARE_MUTEX_OVERHEAD;
    std::unique_ptr<Guage> TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD;
    std::unique_ptr<Guage> TXN_DUPLICATE_KEY_OVERHEAD;
    std::unique_ptr<Guage> TXN_SNAPSHOT_MUTEX_OVERHEAD;
    std::unique_ptr<Guage> NUMBER_MULTIGET_KEYS_FOUND;

    //////////统计瞬时值/////////
    std::unique_ptr<Guage> row_cache_used_guaga;//已用
    std::unique_ptr<Guage> row_cache_total_guaga;//总大小
    std::unique_ptr<Guage> cfindex_level_sst_guaga;//
    std::unique_ptr<Guage> cfdata_level_sst_guaga;//

    ///////////分布统计写log与写db///////////////
    std::shared_ptr<TimerMetric> Write_Paxoslog_Time;//记录 paxos log耗时
    std::shared_ptr<TimerMetric> Put_Rocksdb_Time;//记录 rocksdb put 耗时
};

//////////////////////////////////////////

class MultiDatabase : public LogStorage
{
public:
    MultiDatabase();
    ~MultiDatabase();

    int Init(const std::string & sDBPath, const int iGroupCount);

    const std::string GetLogStorageDirPath(const int iGroupIdx);

    int Get(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue);

    int Put(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue);

    int Del(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID);

    int ForceDel(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID);

    int GetMaxInstanceID(const int iGroupIdx, uint64_t & llInstanceID);

    int SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llMinInstanceID);

    int GetMinChosenInstanceID(const int iGroupIdx, uint64_t & llMinInstanceID);

    int ClearAllLog(const int iGroupCnt);

    int SetSystemVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer);

    int GetSystemVariables(const int iGroupIdx, std::string & sBuffer);
    
    int SetMasterVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer);

    int GetMasterVariables(const int iGroupIdx, std::string & sBuffer);

    bool CreteCheckPoint(std::string &backpath);//备份到这个路径下
   
    //新增
    int KvGet(const std::string & sKey, std::string & sValue );

    int KvSet(const std::string & sKey, const std::string & sValue );;

    int KvDel(const std::string & sKey );

    int KvGetCheckpointInstanceID(const uint16_t iGroupIdx,uint64_t & llCheckpointInstanceID);;

    int KvSetCheckpointInstanceID(const uint16_t iGroupIdx,const uint64_t llCheckpointInstanceID);;

    void DumpRocksDBStats();

    bool KvBatchGet(const phxkv::KvBatchGetRequest* request, ::phxkv::KvBatchGetResponse* response);

    bool KvBatchSet(const ::phxkv::KvBatchPutRequest request);;

    bool CreateCheckpoint( const string checkpoint_dir ){
        return m_vecDBList[0]->CreatCheckPoint(checkpoint_dir);
    }

    rocksdb::TransactionDB * GetDbObj(){
        return m_vecDBList[0]->GetDbObj();
    }
    rocksdb::ColumnFamilyHandle* GetCfData(){
        return m_vecDBList[0]->GetCfData();
    }

    int HashDel(const ::phxkv::HashRequest* request ,std::string hash_key);
    int HashSet( const ::phxkv::HashRequest* request,std::string hash_key );
    int HashGet( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,string hsh_key);
    virtual int HashGetAll(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response ,std::string hash_key  );
    virtual int HashExist(const ::phxkv::HashRequest* request, ::phxkv::HashResponse* response ,std::string hash_key);
    virtual int HashIncrByInt(const ::phxkv::HashRequest* request ,std::string hash_key,std::string & ret_value);
    virtual int HashIncrByFloat(const ::phxkv::HashRequest* request ,std::string hash_key,std::string & ret_value);
    virtual int HashKeys( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key);
    virtual int HashLen(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response  ,std::string hash_key,uint64_t &length);//返回长度
    virtual int HashMget(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response ,std::string hash_key);
    virtual int HashMset(const ::phxkv::HashRequest* request ,std::string hash_key );
    virtual int HashSetNx(const ::phxkv::HashRequest* request ,std::string hash_key );
    virtual int HashValues( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key);

    //===============删除==============
    int DelKey(const string& key);
    int ExpireKey(const string& key,const int ttl);
    //=============list=================
    int ListLpush(const ::phxkv::ListRequest* request,std::string list_key);
    int ListLpushx(const ::phxkv::ListRequest* request,std::string list_key);
    int ListLpop(const ::phxkv::ListRequest* request,std::string list_key,std::string& value);
    int ListLength(const ::phxkv::ListRequest* request,::phxkv::ListResponse* response,std::string list_key,uint64_t& length);
    int ListRpop(  const ::phxkv::ListRequest* request, std::string list_key ,std::string& value);//弹出表尾
    int ListRpopLpush(  const ::phxkv::ListRequest* request,std::string list_key ,std::string &ret_value);//尾部进行头插
    int ListIndex(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key);//插入到指定位置
    int ListInsert(  const ::phxkv::ListRequest* request, std::string list_key );//插入到指定位置
    int ListRange(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key);
    int ListRem(  const ::phxkv::ListRequest* request, std::string list_key );//删除N个等值元素
    int ListSet(  const ::phxkv::ListRequest* request, std::string list_key );//根据下表进行更新元素
    int ListTtim(  const ::phxkv::ListRequest* request, std::string list_key );//保留指定区间元素，其余删除
    int ListRpush(  const ::phxkv::ListRequest* request, std::string list_key );//尾插法
    int ListRpushx(  const ::phxkv::ListRequest* request, std::string list_key );//链表存在时，才执行尾部插入
    //=============set===================
     int SAdd(  const ::phxkv::SetRequest* request, const std::string& set_key);
     int SRem(  const ::phxkv::SetRequest* request, const std::string& set_key );
     int SCard(  const ::phxkv::SetRequest* request, uint64_t & length, const std::string& set_key);
     int SMembers(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string& set_key);
     int SUnionStore(  const ::phxkv::SetRequest* request,const std::string& set_key );
     int SUnion(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response );
     int SInterStore(  const ::phxkv::SetRequest* request, const std::string& set_key );
     int SInter(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response );
     int SDiffStore(  const ::phxkv::SetRequest* request,const std::string& set_key );
     int SDiff(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response );
     int SIsMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, const std::string& set_key);
     int SPop(  const ::phxkv::SetRequest* request, const std::string& set_key, std::string &ret_key );
     int SRandMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, const std::string& set_key);
     int SMove(  const ::phxkv::SetRequest* request,const std::string& set_key );

    //=======================zset=============================================
    int ZAdd( const ::phxkv::ZsetRequest* request,const std::string & zset_key);//一个或多个
    int ZCard( const ::phxkv::ZsetRequest* request, uint64_t & length,const std::string & zset_key);//元素数量
    int ZCount( const ::phxkv::ZsetRequest* request, uint64_t& length,const std::string & zset_key);//有序集合中，在区间内的数量
    int ZRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);//有序集合区间内的成员
    int ZIncrby( const ::phxkv::ZsetRequest* request,  const std::string & zset_key,std::string & ret_key);//为score增加或减少
    int ZUnionStore( const ::phxkv::ZsetRequest* ,const std::string & zset_key);//并集不输出
    int ZInterStore( const ::phxkv::ZsetRequest* request,const std::string & zset_key);//交集不输出
    int ZRangebyscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);
    int ZRem( const ::phxkv::ZsetRequest* request ,const std::string & zset_key);//移除有序集 key 中的一个或多个成员
    int ZRank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key);//返回member的排名，
    int ZRevrank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key);//member逆序排名
    int ZScore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);//member的score
    int ZREVRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);//指定区间成员，逆序输出
    int ZREVRangebylscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key);
    int ZRemrangebyrank( const ::phxkv::ZsetRequest* request, const std::string & zset_key);
    int ZRemrangebyscore( const ::phxkv::ZsetRequest* request, const std::string & zset_key);
private:
    std::vector<Database *> m_vecDBList;
};

}
    

