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
#include "log.h"
#include "db.h"
#include "commdef.h"
#include "utils_include.h"
#include "sysconfig.h"

#define KV_CHECKPOINT_KEY ((uint64_t)-1)   //DB中，数据存放后，更新一下该ID。
using namespace phxkv;

namespace phxpaxos
{

int PaxosComparator :: Compare(const rocksdb::Slice & a, const rocksdb::Slice & b) const
{
    return PCompare(a, b);
}

int PaxosComparator :: PCompare(const rocksdb::Slice & a, const rocksdb::Slice & b) 
{
    if (a.size() != sizeof(uint64_t))
    {
        ERROR_LOG("assert a.size %zu b.size %zu", a.size(), b.size());
        assert(a.size() == sizeof(uint64_t));
    }

    if (b.size() != sizeof(uint64_t))
    {
        ERROR_LOG("assert a.size %zu b.size %zu", a.size(), b.size());
        assert(b.size() == sizeof(uint64_t));
    }
    
    uint64_t lla = 0;
    uint64_t llb = 0;

    memcpy(&lla, a.data(), sizeof(uint64_t));
    memcpy(&llb, b.data(), sizeof(uint64_t));

    if (lla == llb)
    {
        return 0;
    }

    return lla < llb ? -1 : 1;
}

////////////////////////

Database :: Database() : m_rocksdb(nullptr)/*, m_poValueStore(nullptr)*/
{
    vec_poValueStore.clear();
    //m_poValueStore =NULL;
    m_bHasInit = false;
    //m_iMyGroupIdx = -1;
}

Database :: ~Database()
{
    for(int i=0 ; i<vec_poValueStore.size();i++ ){
        if( vec_poValueStore.at(i) != NULL ){
            delete vec_poValueStore.at(i);
            vec_poValueStore.at(i)=NULL;
        }
    }
    vec_poValueStore.clear();
    
    delete m_rocksdb;
    m_rocksdb=NULL;
    DEBUG_LOG(" m_rocksdb Deleted. Path %s", m_sDBPath.c_str());
}

int Database :: ClearAllLog(const int iGroupCnt )
{
    //清理旧数据之前，保留旧数据
    int ret = 0;
    map<int,string> map_SystemVariablesBuffer;
    map<int,string> map_MasterVariablesBuffer;

    for(int i=0;i<iGroupCnt;i++  ){
        const int iGroupIdx = i;
        string sSystemVariablesBuffer;
        ret = GetSystemVariables(iGroupIdx , sSystemVariablesBuffer);
        if (ret != 0 && ret != 1)
        {
            ERROR_LOG("GetSystemVariables fail, ret %d", ret);
            return ret;
        }
        string sMasterVariablesBuffer;
        ret = GetMasterVariables(iGroupIdx ,sMasterVariablesBuffer);
        if (ret != 0 && ret != 1)
        {
            ERROR_LOG("GetMasterVariables fail, ret %d", ret);
            return ret;
        }
        m_bHasInit = false;
        delete vec_poValueStore[iGroupIdx];
        vec_poValueStore[iGroupIdx]=NULL;
        //delete m_poValueStore;
        map_SystemVariablesBuffer.insert(make_pair( iGroupIdx,sSystemVariablesBuffer ) );
        map_MasterVariablesBuffer.insert(make_pair( iGroupIdx, sMasterVariablesBuffer) );

    }

    delete m_rocksdb;
    m_rocksdb = NULL;
    m_bHasInit = false;//
    for(int i=0 ; i<vec_poValueStore.size();i++ ){
        if( vec_poValueStore.at(i) != NULL ){
            delete vec_poValueStore.at(i);
            vec_poValueStore.at(i)=NULL;
        }
    }
    vec_poValueStore.clear();
    
    return 0;

    /*ret = Init(m_sDBPath, iGroupCnt );//此时直接返回，只执行接受操作，其他操作忽略，没意义
    if (ret != 0)
    {
        ERROR_LOG("Init again fail, ret %d", ret);
        return ret;
    }else{
        INFO_LOG("Init again ok, ret %d", ret);
    }

    WriteOptions oWriteOptions;
    oWriteOptions.bSync = true;
    
    map<int,string>::iterator iter = map_SystemVariablesBuffer.begin();
    for( ;iter != map_SystemVariablesBuffer.end() ; iter++ ){

        string sSystemVariable = iter->second;
        const int groupid = iter->first;

        if (sSystemVariable.size() > 0)
        {
            ret = SetSystemVariables(oWriteOptions, groupid, sSystemVariable);
            if (ret != 0)
            {
                ERROR_LOG("SetSystemVariables fail, ret %d", ret);
                return ret;
            }
        }
    }

    iter= map_MasterVariablesBuffer.begin();
    for( ; iter != map_MasterVariablesBuffer.end();iter++ ){

        const int groupid = iter->first;
        string sMasterVariable = iter->second;

        if (sMasterVariable.size() > 0)
        {
            ret = SetMasterVariables(oWriteOptions,groupid, sMasterVariable);
            if (ret != 0)
            {
                ERROR_LOG("SetMasterVariables fail, ret %d", ret);
                return ret;
            }
        }
    }*/
    return 0;
}

int Database :: InitColumnFamilyTtl()
{
    rocksdb::ColumnFamilyDescriptor  desp1;
    desp1.name =CConfig::Get()->paxosnodes.column_family;
    column_families.push_back( desp1 );

    rocksdb::ColumnFamilyDescriptor  desp2;
    desp2.name =CConfig::Get()->kvdb.column_family;
    column_families.push_back( desp2 );

    rocksdb::ColumnFamilyDescriptor dep3;
    dep3.name ="default" ;
    column_families.push_back( dep3 );
    return 0;
}

int Database :: InitBucket()
{
    Write_Paxoslog_Time.reset( g_pmetrics->NewTimer("write_paxoslog_cost", "", {{"op", "write_paxoslog"}}));;
    Put_Rocksdb_Time.reset( g_pmetrics->NewTimer("put_rocksdb_cost", "", {{"op", "put_rocksdb"}}));;

    return 0;
}
int Database :: Init(const std::string & sDBPath, const int MyGroupCount )
{
    INFO_LOG("Database :: Init.%s, MyGroupCount %d",sDBPath.c_str(), MyGroupCount );
    if (m_bHasInit)
    {
        return 0;
    }
    
    //m_iMyGroupIdx = iMyGroupIdx;
    m_sDBPath = sDBPath;
    checkpoint_ptr = NULL;
    
    std::map<std::string, std::string> labels = {{"db", std::to_string( 0 )}};

    BLOCK_CACHE_MISS.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_MISS", "", labels));
    BLOCK_CACHE_HIT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_HIT", "", labels));
    BLOCK_CACHE_ADD.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_ADD", "", labels));
    BLOCK_CACHE_ADD_FAILURES.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_ADD_FAILURES", "", labels));
    BLOCK_CACHE_INDEX_MISS.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_INDEX_MISS", "", labels));
    BLOCK_CACHE_INDEX_HIT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_INDEX_HIT", "", labels));
    BLOCK_CACHE_INDEX_ADD.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_INDEX_ADD", "", labels));
    BLOCK_CACHE_INDEX_BYTES_INSERT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_INDEX_BYTES_INSERT", "", labels));
    BLOCK_CACHE_INDEX_BYTES_EVICT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_INDEX_BYTES_EVICT", "", labels));
    BLOCK_CACHE_FILTER_MISS.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_FILTER_MISS", "", labels));
    BLOCK_CACHE_FILTER_HIT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_FILTER_HIT", "", labels));
    BLOCK_CACHE_FILTER_ADD.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_FILTER_ADD", "", labels));
    BLOCK_CACHE_FILTER_BYTES_INSERT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_FILTER_BYTES_INSERT", "", labels));
    BLOCK_CACHE_FILTER_BYTES_EVICT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_FILTER_BYTES_EVICT", "", labels));
    BLOCK_CACHE_DATA_MISS.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_DATA_MISS", "", labels));
    BLOCK_CACHE_DATA_HIT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_DATA_HIT", "", labels));
    BLOCK_CACHE_DATA_ADD.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_DATA_ADD", "", labels));
    BLOCK_CACHE_DATA_BYTES_INSERT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_DATA_BYTES_INSERT", "", labels));
    BLOCK_CACHE_BYTES_READ.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_BYTES_READ", "", labels));
    BLOCK_CACHE_BYTES_WRITE.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_BYTES_WRITE", "", labels));
    BLOOM_FILTER_USEFUL.reset(g_pmetrics->NewGuage("rocksdb_BLOOM_FILTER_USEFUL", "", labels));
    BLOOM_FILTER_FULL_POSITIVE.reset(g_pmetrics->NewGuage("rocksdb_BLOOM_FILTER_FULL_POSITIVE", "", labels));
    BLOOM_FILTER_FULL_TRUE_POSITIVE.reset(g_pmetrics->NewGuage("rocksdb_BLOOM_FILTER_FULL_TRUE_POSITIVE", "", labels));
    PERSISTENT_CACHE_HIT.reset(g_pmetrics->NewGuage("rocksdb_PERSISTENT_CACHE_HIT", "", labels));
    PERSISTENT_CACHE_MISS.reset(g_pmetrics->NewGuage("rocksdb_PERSISTENT_CACHE_MISS", "", labels));
    SIM_BLOCK_CACHE_HIT.reset(g_pmetrics->NewGuage("rocksdb_SIM_BLOCK_CACHE_HIT", "", labels));
    SIM_BLOCK_CACHE_MISS.reset(g_pmetrics->NewGuage("rocksdb_SIM_BLOCK_CACHE_MISS", "", labels));
    MEMTABLE_HIT.reset(g_pmetrics->NewGuage("rocksdb_MEMTABLE_HIT", "", labels));
    MEMTABLE_MISS.reset(g_pmetrics->NewGuage("rocksdb_MEMTABLE_MISS", "", labels));
    GET_HIT_L0.reset(g_pmetrics->NewGuage("rocksdb_GET_HIT_L0", "", labels));
    GET_HIT_L1.reset(g_pmetrics->NewGuage("rocksdb_GET_HIT_L1", "", labels));
    GET_HIT_L2_AND_UP.reset(g_pmetrics->NewGuage("rocksdb_GET_HIT_L2_AND_UP", "", labels));
    COMPACTION_KEY_DROP_NEWER_ENTRY.reset(g_pmetrics->NewGuage("rocksdb_COMPACTION_KEY_DROP_NEWER_ENTRY", "", labels));
    COMPACTION_KEY_DROP_OBSOLETE.reset(g_pmetrics->NewGuage("rocksdb_COMPACTION_KEY_DROP_OBSOLETE", "", labels));
    COMPACTION_KEY_DROP_RANGE_DEL.reset(g_pmetrics->NewGuage("rocksdb_COMPACTION_KEY_DROP_RANGE_DEL", "", labels));
    COMPACTION_KEY_DROP_USER.reset(g_pmetrics->NewGuage("rocksdb_COMPACTION_KEY_DROP_USER", "", labels));
    COMPACTION_RANGE_DEL_DROP_OBSOLETE.reset(g_pmetrics->NewGuage("rocksdb_COMPACTION_RANGE_DEL_DROP_OBSOLETE", "", labels));
    COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE.reset(g_pmetrics->NewGuage("rocksdb_COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE", "", labels));
    COMPACTION_CANCELLED.reset(g_pmetrics->NewGuage("rocksdb_COMPACTION_CANCELLED", "", labels));
    NUMBER_KEYS_WRITTEN.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_KEYS_WRITTEN", "", labels));
    NUMBER_KEYS_READ.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_KEYS_READ", "", labels));
    NUMBER_KEYS_UPDATED.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_KEYS_UPDATED", "", labels));
    BYTES_WRITTEN.reset(g_pmetrics->NewGuage("rocksdb_BYTES_WRITTEN", "", labels));
    BYTES_READ.reset(g_pmetrics->NewGuage("rocksdb_BYTES_READ", "", labels));
    NUMBER_DB_SEEK.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_DB_SEEK", "", labels));
    NUMBER_DB_NEXT.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_DB_NEXT", "", labels));
    NUMBER_DB_PREV.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_DB_PREV", "", labels));
    NUMBER_DB_SEEK_FOUND.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_DB_SEEK_FOUND", "", labels));
    NUMBER_DB_NEXT_FOUND.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_DB_NEXT_FOUND", "", labels));
    NUMBER_DB_PREV_FOUND.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_DB_PREV_FOUND", "", labels));
    ITER_BYTES_READ.reset(g_pmetrics->NewGuage("rocksdb_ITER_BYTES_READ", "", labels));
    NO_FILE_CLOSES.reset(g_pmetrics->NewGuage("rocksdb_NO_FILE_CLOSES", "", labels));
    NO_FILE_OPENS.reset(g_pmetrics->NewGuage("rocksdb_NO_FILE_OPENS", "", labels));
    NO_FILE_ERRORS.reset(g_pmetrics->NewGuage("rocksdb_NO_FILE_ERRORS", "", labels));
    STALL_L0_SLOWDOWN_MICROS.reset(g_pmetrics->NewGuage("rocksdb_STALL_L0_SLOWDOWN_MICROS", "", labels));
    STALL_MEMTABLE_COMPACTION_MICROS.reset(g_pmetrics->NewGuage("rocksdb_STALL_MEMTABLE_COMPACTION_MICROS", "", labels));
    STALL_L0_NUM_FILES_MICROS.reset(g_pmetrics->NewGuage("rocksdb_STALL_L0_NUM_FILES_MICROS", "", labels));
    STALL_MICROS.reset(g_pmetrics->NewGuage("rocksdb_STALL_MICROS", "", labels));
    DB_MUTEX_WAIT_MICROS.reset(g_pmetrics->NewGuage("rocksdb_DB_MUTEX_WAIT_MICROS", "", labels));
    RATE_LIMIT_DELAY_MILLIS.reset(g_pmetrics->NewGuage("rocksdb_RATE_LIMIT_DELAY_MILLIS", "", labels));
    NO_ITERATORS.reset(g_pmetrics->NewGuage("rocksdb_NO_ITERATORS", "", labels));
    NUMBER_MULTIGET_CALLS.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_MULTIGET_CALLS", "", labels));
    NUMBER_MULTIGET_KEYS_READ.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_MULTIGET_KEYS_READ", "", labels));
    NUMBER_MULTIGET_BYTES_READ.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_MULTIGET_BYTES_READ", "", labels));
    NUMBER_FILTERED_DELETES.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_FILTERED_DELETES", "", labels));
    NUMBER_MERGE_FAILURES.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_MERGE_FAILURES", "", labels));
    BLOOM_FILTER_PREFIX_CHECKED.reset(g_pmetrics->NewGuage("rocksdb_BLOOM_FILTER_PREFIX_CHECKED", "", labels));
    BLOOM_FILTER_PREFIX_USEFUL.reset(g_pmetrics->NewGuage("rocksdb_BLOOM_FILTER_PREFIX_USEFUL", "", labels));
    NUMBER_OF_RESEEKS_IN_ITERATION.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_OF_RESEEKS_IN_ITERATION", "", labels));
    GET_UPDATES_SINCE_CALLS.reset(g_pmetrics->NewGuage("rocksdb_GET_UPDATES_SINCE_CALLS", "", labels));
    BLOCK_CACHE_COMPRESSED_MISS.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_COMPRESSED_MISS", "", labels));
    BLOCK_CACHE_COMPRESSED_HIT.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_COMPRESSED_HIT", "", labels));
    BLOCK_CACHE_COMPRESSED_ADD.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_COMPRESSED_ADD", "", labels));
    BLOCK_CACHE_COMPRESSED_ADD_FAILURES.reset(g_pmetrics->NewGuage("rocksdb_BLOCK_CACHE_COMPRESSED_ADD_FAILURES", "", labels));
    WAL_FILE_SYNCED.reset(g_pmetrics->NewGuage("rocksdb_WAL_FILE_SYNCED", "", labels));
    WAL_FILE_BYTES.reset(g_pmetrics->NewGuage("rocksdb_WAL_FILE_BYTES", "", labels));
    WRITE_DONE_BY_SELF.reset(g_pmetrics->NewGuage("rocksdb_WRITE_DONE_BY_SELF", "", labels));
    WRITE_DONE_BY_OTHER.reset(g_pmetrics->NewGuage("rocksdb_WRITE_DONE_BY_OTHER", "", labels));
    WRITE_TIMEDOUT.reset(g_pmetrics->NewGuage("rocksdb_WRITE_TIMEDOUT", "", labels));
    WRITE_WITH_WAL.reset(g_pmetrics->NewGuage("rocksdb_WRITE_WITH_WAL", "", labels));
    COMPACT_READ_BYTES.reset(g_pmetrics->NewGuage("rocksdb_COMPACT_READ_BYTES", "", labels));
    COMPACT_WRITE_BYTES.reset(g_pmetrics->NewGuage("rocksdb_COMPACT_WRITE_BYTES", "", labels));
    FLUSH_WRITE_BYTES.reset(g_pmetrics->NewGuage("rocksdb_FLUSH_WRITE_BYTES", "", labels));
    NUMBER_DIRECT_LOAD_TABLE_PROPERTIES.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_DIRECT_LOAD_TABLE_PROPERTIES", "", labels));
    NUMBER_SUPERVERSION_ACQUIRES.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_SUPERVERSION_ACQUIRES", "", labels));
    NUMBER_SUPERVERSION_RELEASES.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_SUPERVERSION_RELEASES", "", labels));
    NUMBER_SUPERVERSION_CLEANUPS.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_SUPERVERSION_CLEANUPS", "", labels));
    NUMBER_BLOCK_COMPRESSED.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_BLOCK_COMPRESSED", "", labels));
    NUMBER_BLOCK_DECOMPRESSED.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_BLOCK_DECOMPRESSED", "", labels));
    NUMBER_BLOCK_NOT_COMPRESSED.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_BLOCK_NOT_COMPRESSED", "", labels));
    MERGE_OPERATION_TOTAL_TIME.reset(g_pmetrics->NewGuage("rocksdb_MERGE_OPERATION_TOTAL_TIME", "", labels));
    FILTER_OPERATION_TOTAL_TIME.reset(g_pmetrics->NewGuage("rocksdb_FILTER_OPERATION_TOTAL_TIME", "", labels));
    ROW_CACHE_HIT.reset(g_pmetrics->NewGuage("rocksdb_ROW_CACHE_HIT", "", labels));
    ROW_CACHE_MISS.reset(g_pmetrics->NewGuage("rocksdb_ROW_CACHE_MISS", "", labels));
    READ_AMP_ESTIMATE_USEFUL_BYTES.reset(g_pmetrics->NewGuage("rocksdb_READ_AMP_ESTIMATE_USEFUL_BYTES", "", labels));
    READ_AMP_TOTAL_READ_BYTES.reset(g_pmetrics->NewGuage("rocksdb_READ_AMP_TOTAL_READ_BYTES", "", labels));
    NUMBER_RATE_LIMITER_DRAINS.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_RATE_LIMITER_DRAINS", "", labels));
    NUMBER_ITER_SKIP.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_ITER_SKIP", "", labels));
    BLOB_DB_NUM_PUT.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_NUM_PUT", "", labels));
    BLOB_DB_NUM_WRITE.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_NUM_WRITE", "", labels));
    BLOB_DB_NUM_GET.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_NUM_GET", "", labels));
    BLOB_DB_NUM_MULTIGET.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_NUM_MULTIGET", "", labels));
    BLOB_DB_NUM_SEEK.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_NUM_SEEK", "", labels));
    BLOB_DB_NUM_NEXT.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_NUM_NEXT", "", labels));
    BLOB_DB_NUM_PREV.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_NUM_PREV", "", labels));
    BLOB_DB_NUM_KEYS_WRITTEN.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_NUM_KEYS_WRITTEN", "", labels));
    BLOB_DB_NUM_KEYS_READ.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_NUM_KEYS_READ", "", labels));
    BLOB_DB_BYTES_WRITTEN.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_BYTES_WRITTEN", "", labels));
    BLOB_DB_BYTES_READ.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_BYTES_READ", "", labels));
    BLOB_DB_WRITE_INLINED.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_WRITE_INLINED", "", labels));
    BLOB_DB_WRITE_INLINED_TTL.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_WRITE_INLINED_TTL", "", labels));
    BLOB_DB_WRITE_BLOB.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_WRITE_BLOB", "", labels));
    BLOB_DB_WRITE_BLOB_TTL.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_WRITE_BLOB_TTL", "", labels));
    BLOB_DB_BLOB_FILE_BYTES_WRITTEN.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_BLOB_FILE_BYTES_WRITTEN", "", labels));
    BLOB_DB_BLOB_FILE_BYTES_READ.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_BLOB_FILE_BYTES_READ", "", labels));
    BLOB_DB_BLOB_FILE_SYNCED.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_BLOB_FILE_SYNCED", "", labels));
    BLOB_DB_BLOB_INDEX_EXPIRED_COUNT.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_BLOB_INDEX_EXPIRED_COUNT", "", labels));
    BLOB_DB_BLOB_INDEX_EXPIRED_SIZE.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_BLOB_INDEX_EXPIRED_SIZE", "", labels));
    BLOB_DB_BLOB_INDEX_EVICTED_COUNT.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_BLOB_INDEX_EVICTED_COUNT", "", labels));
    BLOB_DB_BLOB_INDEX_EVICTED_SIZE.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_BLOB_INDEX_EVICTED_SIZE", "", labels));
    BLOB_DB_GC_NUM_FILES.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_GC_NUM_FILES", "", labels));
    BLOB_DB_GC_NUM_NEW_FILES.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_GC_NUM_NEW_FILES", "", labels));
    BLOB_DB_GC_FAILURES.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_GC_FAILURES", "", labels));
    BLOB_DB_GC_NUM_KEYS_OVERWRITTEN.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_GC_NUM_KEYS_OVERWRITTEN", "", labels));
    BLOB_DB_GC_NUM_KEYS_EXPIRED.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_GC_NUM_KEYS_EXPIRED", "", labels));
    BLOB_DB_GC_NUM_KEYS_RELOCATED.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_GC_NUM_KEYS_RELOCATED", "", labels));
    BLOB_DB_GC_BYTES_OVERWRITTEN.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_GC_BYTES_OVERWRITTEN", "", labels));
    BLOB_DB_GC_BYTES_EXPIRED.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_GC_BYTES_EXPIRED", "", labels));
    BLOB_DB_GC_BYTES_RELOCATED.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_GC_BYTES_RELOCATED", "", labels));
    BLOB_DB_FIFO_NUM_FILES_EVICTED.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_FIFO_NUM_FILES_EVICTED", "", labels));
    BLOB_DB_FIFO_NUM_KEYS_EVICTED.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_FIFO_NUM_KEYS_EVICTED", "", labels));
    BLOB_DB_FIFO_BYTES_EVICTED.reset(g_pmetrics->NewGuage("rocksdb_BLOB_DB_FIFO_BYTES_EVICTED", "", labels));
    TXN_PREPARE_MUTEX_OVERHEAD.reset(g_pmetrics->NewGuage("rocksdb_TXN_PREPARE_MUTEX_OVERHEAD", "", labels));
    TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD.reset(g_pmetrics->NewGuage("rocksdb_TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD", "", labels));
    TXN_DUPLICATE_KEY_OVERHEAD.reset(g_pmetrics->NewGuage("rocksdb_TXN_DUPLICATE_KEY_OVERHEAD", "", labels));
    TXN_SNAPSHOT_MUTEX_OVERHEAD.reset(g_pmetrics->NewGuage("rocksdb_TXN_SNAPSHOT_MUTEX_OVERHEAD", "", labels));
    NUMBER_MULTIGET_KEYS_FOUND.reset(g_pmetrics->NewGuage("rocksdb_NUMBER_MULTIGET_KEYS_FOUND", "", labels));

    
    rocksdb::Options oOptions;
    oOptions.create_if_missing = true;
    oOptions.create_missing_column_families = true;
    oOptions.comparator = &m_oPaxosCmp;
    //every group have different buffer size to avoid all group compact at the same time.

    //rocksdb::ColumnFamilyOptions cfoptions;
    oOptions.level0_slowdown_writes_trigger = CConfig::Get()->rocksdb.level0_slowdown_writes_trigger;
    oOptions.level0_stop_writes_trigger = CConfig::Get()->rocksdb.level0_stop_writes_trigger;
    oOptions.soft_pending_compaction_bytes_limit = CConfig::Get()->rocksdb.soft_pending_compaction_bytes_limit;
    oOptions.hard_pending_compaction_bytes_limit = CConfig::Get()->rocksdb.hard_pending_compaction_bytes_limit;
    oOptions.write_buffer_size = CConfig::Get()->rocksdb.write_buffer_size;
    oOptions.min_write_buffer_number_to_merge = CConfig::Get()->rocksdb.min_write_buffer_number_to_merge;
    oOptions.max_write_buffer_number = CConfig::Get()->rocksdb.max_write_buffer_number;
    oOptions.level0_file_num_compaction_trigger = CConfig::Get()->rocksdb.level0_file_num_compaction_trigger;
    oOptions.optimize_filters_for_hits = CConfig::Get()->rocksdb.optimize_filters_for_hits;

    oOptions.statistics = rocksdb::CreateDBStatistics();
    stats = oOptions.statistics;

    int cacheSize = CConfig::Get()->rocksdb.row_cache_size;
    oOptions.row_cache= rocksdb::NewLRUCache( cacheSize ); //std::make_shared<rocksdb::Cache>( );
    def_row_cache = oOptions.row_cache;

    InitColumnFamilyTtl( );
    InitBucket();
    rocksdb::TransactionDBOptions txn_db_options;
    rocksdb::Status oStatus = rocksdb::TransactionDB::Open(oOptions,txn_db_options, sDBPath,column_families,&vec_cf, &m_rocksdb );
    if (!oStatus.ok())
    {
        ERROR_LOG("Open rocksdb fail, db_path %s %s", sDBPath.c_str(),oStatus.ToString().c_str() );
        return -1;
    }else{
        INFO_LOG("Open rocksdb ok");
    }
    m_bHasInit = true;
    oStatus = rocksdb::Checkpoint::Create( m_rocksdb, &checkpoint_ptr);
    if( !oStatus.ok() ){
         ERROR_LOG("Checkpoint, db_path %s %s", sDBPath.c_str(),oStatus.ToString().c_str() );
         return -1;
    }
    WARNING_LOG("init db start...");
    for(int i=0;i< MyGroupCount;i++ )
    //for(int i=0;i< 1;i++ )
    {
        const int groupid = i;
        LogStore* pValueStore = new LogStore();
        assert( pValueStore != nullptr);
        vec_poValueStore.push_back(pValueStore );
        //m_poValueStore = pValueStore;
        
        int ret = vec_poValueStore[groupid]->Init(sDBPath, groupid, (Database *)this);
        //int ret = m_poValueStore->Init(sDBPath, groupid, (Database *)this);
        if (ret != 0)
        {
            ERROR_LOG("value store init fail, ret %d", ret);
            return -1;
        }
    }
    WARNING_LOG("init db finish...");
    //m_poValueStore = new LogStore(); 
    //assert(m_poValueStore != nullptr);

    //int ret = m_poValueStore->Init(sDBPath, iMyGroupIdx, (Database *)this);
    //if (ret != 0)
    //{
        //ERROR_LOG("value store init fail, ret %d", ret);
        //return -1;
    //}
    
    
    cfindex_level_sst_guaga.reset(g_pmetrics->NewGuage("rocksdb_CFINDEX_SST", "", labels));
    cfdata_level_sst_guaga.reset(g_pmetrics->NewGuage("rocksdb_CFDATA_SST", "", labels));

    row_cache_used_guaga.reset(g_pmetrics->NewGuage("rocksdb_CACHE_USED", "", labels));
    row_cache_total_guaga.reset(g_pmetrics->NewGuage("rocksdb_CACHE_TOTAL", "", labels));
    INFO_LOG("OK, db_path %s", sDBPath.c_str());
    return 0;
}

int Database :: GetUsedRowCache()
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    int max_size = CConfig::Get()->rocksdb.row_cache_size; //def_row_cache->GetCapacity();
    row_cache_total_guaga->SetValue( max_size );
    int used_size = def_row_cache->GetUsage();
    row_cache_used_guaga->SetValue( used_size );
    DEBUG_LOG("max_size %lu used_size%lu", max_size , used_size );
    return used_size;
}//分cf,每层文件数,每层文件大小
void Database :: SetLevelSstProperty(const string &cf ,const int size )
{
    DEBUG_LOG("debug cf %s", cf.c_str() );
    if( CConfig::Get()->paxosnodes.column_family == cf ){

        cfindex_level_sst_guaga->SetValue( size );
    }else if( CConfig::Get()->kvdb.column_family == cf){

        cfdata_level_sst_guaga->SetValue( size );
    }else{
        return;
    }
}
bool Database :: GetLevelSstProperty( const string &property,string &_value )
{
     for(int index=0;index<vec_cf.size()-1;index++ ){

         bool ret = m_rocksdb->GetProperty(vec_cf[index], property,&_value );
        if(ret ){
            int cf_size=0;//统计空间占用
            std::vector<string> vec_ret;
            StringUtil::splitstr(_value,"\n", vec_ret);
            DEBUG_LOG("GetProperty %s,vecsize %d",_value.c_str() ,vec_ret.size() );
            if( vec_ret.size()!=9 ){
                return false;
            }
            for(int k=2;k<vec_ret.size();k++ ){
            
                if( !vec_ret[k].empty() ){
                    std::vector<string> temp;
                    StringUtil::splitstr(vec_ret[k]," ", temp);
                     
                    if(temp.size()==3 ){
                        string level = temp[0];
                        string file_num = temp[1];
                        string size = temp[2];
                        cf_size+= atoi( size.c_str() );
                        INFO_LOG("cf [%s] level [%s] file_num [%s] levelsize [%s] M",vec_cf[index]->GetName().c_str(),
                         level.c_str(), file_num.c_str(),size.c_str() );  
                    }
                }
            }
            SetLevelSstProperty( vec_cf[index]->GetName(), cf_size );
        }else{
            ERROR_LOG("GetProperty error,property %s",property.c_str() );
            return false;
        }
     }

    return true;
}

const std::string Database :: GetDBPath()
{
    return m_sDBPath;
}

int Database :: GetMaxInstanceIDFileID(const int iGroupIdx, std::string & sFileID, uint64_t & llInstanceID)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    uint64_t llMaxInstanceID = 0;
    int ret = GetMaxInstanceID( iGroupIdx , llMaxInstanceID);//最大实例ID
    if (ret != 0 && ret != 1)
    {
        return ret;
    }
    INFO_LOG( "GetMaxInstanceIDFileID gropid %d llInstanceID %lu" , iGroupIdx, llMaxInstanceID );
    if (ret == 1)
    {
        sFileID = "";
        return 0;
    }
    string groupid = GenGroupId(iGroupIdx);
    string sKey = GenKey(llMaxInstanceID);
    sKey = groupid+sKey;
    rocksdb::Status oStatus = m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[0], sKey, &sFileID);
    if (!oStatus.ok())
    {
        if (oStatus.IsNotFound())
        {
            BP->GetLogStorageBP()->LevelDBGetNotExist();
            ERROR_LOG("LevelDB.Get not found %s", sKey.c_str());
            return 1;
        }
        
        BP->GetLogStorageBP()->LevelDBGetFail();
        ERROR_LOG("LevelDB.Get fail");
        return -1;
    }

    llInstanceID = llMaxInstanceID;

    return 0;
}

int Database :: RebuildOneIndex(const int iGroupIdx , const uint64_t llInstanceID, const std::string & sFileID)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    string gid = GenGroupId( iGroupIdx );
    string sKey = GenKey(llInstanceID);
    sKey =gid + sKey;
    rocksdb::WriteOptions oLevelDBWriteOptions;
    oLevelDBWriteOptions.sync = false;

    rocksdb::Status oStatus = m_rocksdb->Put(oLevelDBWriteOptions,vec_cf[0], sKey, sFileID);
    if (!oStatus.ok())
    {
        BP->GetLogStorageBP()->LevelDBPutFail();
        ERROR_LOG("LevelDB.Put fail, instanceid %lu valuelen %zu", llInstanceID, sFileID.size());
        return -1;
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////

int Database :: GetFromLevelDB(const int iGroupIdx,const uint64_t llInstanceID, std::string & sValue)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    string strgroupid = GenGroupId(iGroupIdx);
    string sKey = GenKey(llInstanceID);
    sKey = strgroupid+ sKey;
     
    rocksdb::Status oStatus = m_rocksdb->Get(rocksdb::ReadOptions(), vec_cf[0], sKey, &sValue);
    if (!oStatus.ok())
    {
        if (oStatus.IsNotFound())
        {
            BP->GetLogStorageBP()->LevelDBGetNotExist();
            ERROR_LOG("LevelDB.Get not found, groupid=%d instanceid %lu",iGroupIdx, llInstanceID);
            return 1;
        }
        
        BP->GetLogStorageBP()->LevelDBGetFail();
        ERROR_LOG("LevelDB.Get fail, instanceid %lu", llInstanceID);
        return -1;
    }

    return 0;
}

int Database :: Get(const int iGroupIdx,const uint64_t llInstanceID, std::string & sValue)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }

    string sFileID;
    int ret = GetFromLevelDB(iGroupIdx,llInstanceID, sFileID);
    if (ret != 0)
    {
        return ret;
    }

    uint64_t llFileInstanceID = 0;
    ret = FileIDToValue(iGroupIdx , sFileID, llFileInstanceID, sValue);
    if (ret != 0)
    {
        BP->GetLogStorageBP()->FileIDToValueFail();
        return ret;
    }

    if (llFileInstanceID != llInstanceID)
    {
        ERROR_LOG("file instanceid %lu not equal to key.instanceid %lu", llFileInstanceID, llInstanceID);
        return -2;
    }

    return 0;
}

int Database :: ValueToFileID(const WriteOptions & oWriteOptions,const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue, std::string & sFileID)
{
   
    if (iGroupIdx >= (int)vec_poValueStore.size())
    {
        return -1;
    }//只有此处调用APPEND方法,写文件
    int ret = vec_poValueStore[iGroupIdx]->Append(oWriteOptions, llInstanceID, sValue, sFileID);
    //int ret = m_poValueStore->Append(oWriteOptions, llInstanceID, sValue, sFileID);
    if (ret != 0)
    {
        BP->GetLogStorageBP()->ValueToFileIDFail();
        ERROR_LOG("fail, ret %d", ret);
        return ret;
    }

    return 0;
}

int Database :: FileIDToValue(const int iGroupIdx,const std::string & sFileID, uint64_t & llInstanceID, std::string & sValue)
{
    
    if (iGroupIdx >= (int)vec_poValueStore.size())
    {
        return -1;
    }
    int ret = vec_poValueStore[iGroupIdx]->Read(sFileID, llInstanceID, sValue);
    //int ret = m_poValueStore->Read(sFileID, llInstanceID, sValue);
    if (ret != 0)
    {
        ERROR_LOG("fail, ret %d", ret);
        return ret;
    }

    return 0;
}

int Database :: PutToLevelDB(const bool bSync, const int iGroupIdx,const uint64_t llInstanceID, const std::string & sValue)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    string str_groupid = GenGroupId( iGroupIdx );
    string sKey = GenKey(llInstanceID);
    sKey = str_groupid+ sKey;
     
    rocksdb::WriteOptions oLevelDBWriteOptions;
    oLevelDBWriteOptions.sync = bSync;

    
    rocksdb::Status oStatus = m_rocksdb->Put(oLevelDBWriteOptions,vec_cf[0], sKey, sValue);
    if (!oStatus.ok())
    {
        BP->GetLogStorageBP()->LevelDBPutFail();
        ERROR_LOG("LevelDB.Put fail, instanceid %lu valuelen %zu", llInstanceID, sValue.size());
        return -1;
    }
    return 0;
}

int Database :: Put(const WriteOptions & oWriteOptions, const int iGroupIdx,const uint64_t llInstanceID, const std::string & sValue)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    auto start_paxoslog = std::chrono::system_clock::now();
    std::string sFileID;//先写paxos log,只有put时调用该方法
    int ret =ValueToFileID(oWriteOptions,iGroupIdx, llInstanceID, sValue, sFileID);
    if (ret != 0)
    {
        return ret;
    }
    auto end_paxoslog = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds_paxoslog = end_paxoslog - start_paxoslog;
    Write_Paxoslog_Time->Observe(elapsed_seconds_paxoslog.count());

    //在写数据库
    auto start_db = std::chrono::system_clock::now();
    ret =PutToLevelDB(false,iGroupIdx, llInstanceID, sFileID);
    auto end_db = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds_db = end_db - start_db;
    Put_Rocksdb_Time->Observe(elapsed_seconds_db.count());
    
    return ret;
}

int Database :: ForceDel(const int iGroupIdx,const WriteOptions & oWriteOptions, const uint64_t llInstanceID)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    string str_groupid = GenGroupId(iGroupIdx);
    string sKey = GenKey(llInstanceID);
    sKey =str_groupid + sKey;
    string sFileID;
    
    rocksdb::Status oStatus = m_rocksdb->Get(rocksdb::ReadOptions(), vec_cf[0], sKey, &sFileID);
    if (!oStatus.ok())
    {
        if (oStatus.IsNotFound())
        {
            ERROR_LOG("LevelDB.Get not found, instanceid %lu", llInstanceID);
            return 0;
        }
        
        ERROR_LOG("LevelDB.Get fail, instanceid %lu", llInstanceID);
        return -1;
    }
    if (iGroupIdx >= (int)vec_poValueStore.size())
    {
        return -1;
    }
    int ret = vec_poValueStore[iGroupIdx]->ForceDel(sFileID, llInstanceID);
    //int ret = m_poValueStore->ForceDel(sFileID, llInstanceID);
    if (ret != 0)
    {
        return ret;
    }

    rocksdb::WriteOptions oLevelDBWriteOptions;
    oLevelDBWriteOptions.sync = oWriteOptions.bSync;
    
    oStatus = m_rocksdb->Delete(oLevelDBWriteOptions, vec_cf[0] , sKey);
    if (!oStatus.ok())
    {
        ERROR_LOG("LevelDB.Delete fail, instanceid %lu", llInstanceID);
        return -1;
    }

    return 0;
}

int Database :: Del(const int iGroupIdx,const WriteOptions & oWriteOptions, const uint64_t llInstanceID)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    string str_grouipd = GenGroupId( iGroupIdx );
    string sKey = GenKey(llInstanceID);
    sKey =str_grouipd + sKey;
    //if (OtherUtils::FastRand() % 100 < 1)修改随机删除
    {
        //no need to del vfile every times.
        string sFileID;
        rocksdb::Status oStatus = m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[0], sKey, &sFileID);
        if (!oStatus.ok())
        {
            if (oStatus.IsNotFound())
            {
                ERROR_LOG("LevelDB.Get not found, instanceid %lu", llInstanceID);
                return 0;
            }
            
            ERROR_LOG("LevelDB.Get fail, instanceid %lu", llInstanceID);
            return -1;
        }
        if (iGroupIdx >= (int)vec_poValueStore.size())
        {
            return -1;
        }
        int ret = vec_poValueStore[iGroupIdx]->Del(sFileID, llInstanceID);
        //int ret = m_poValueStore->Del(sFileID, llInstanceID);
        if (ret != 0)
        {
            return ret;
        }
    }

    rocksdb::WriteOptions oLevelDBWriteOptions;
    oLevelDBWriteOptions.sync = oWriteOptions.bSync;
    
    rocksdb::Status oStatus = m_rocksdb->Delete(oLevelDBWriteOptions,vec_cf[0], sKey);
    if (!oStatus.ok())
    {
        ERROR_LOG("LevelDB.Delete fail, instanceid %lu", llInstanceID);
        return -1;
    }

    return 0;
}

int Database :: GetMaxInstanceID(const int iGroupIdx,uint64_t & llInstanceID)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    llInstanceID = MINCHOSEN_KEY;
    rocksdb::Iterator * it = m_rocksdb->NewIterator(rocksdb::ReadOptions(), vec_cf[0] );
    it->SeekToLast();

    while (it->Valid())
    {
        uint16_t cur_GroupIdx=0;
        llInstanceID = GetInstanceIDFromKey(it->key().ToString() , cur_GroupIdx );
        if( cur_GroupIdx!= iGroupIdx  ){ //必须是同一个分组

            it->Prev();
            continue;
        }
        if (llInstanceID == MINCHOSEN_KEY
                || llInstanceID == SYSTEMVARIABLES_KEY
                || llInstanceID == MASTERVARIABLES_KEY)
        {
            it->Prev();
        }
        else
        {
            delete it;
            return 0;
        }
    }

    delete it;
    return 1;
}
std::string Database :: GenGroupId(const uint16_t iGroupIdx)
{
    string sKey;
    sKey.append(  (char *)&iGroupIdx, sizeof(uint16_t) );
    return sKey;
}

std::string Database :: GenKey(const uint64_t llInstanceID)
{
    string sKey;
    sKey.append((char *)&llInstanceID, sizeof(uint64_t));
    return sKey;
}

const uint64_t Database :: GetInstanceIDFromKey(const std::string & sKey , uint16_t & iGroupIdx)
{
    iGroupIdx = 999;
    memcpy( &iGroupIdx,sKey.data() ,sizeof(uint16_t) );

    uint64_t llInstanceID = 0;
    memcpy(&llInstanceID, sKey.data()+sizeof(uint16_t) , sizeof(uint64_t));
    return llInstanceID;
}

int Database :: GetMinChosenInstanceID(const int iGroupIdx,uint64_t & llMinInstanceID)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
     
    static uint64_t llMinKey = MINCHOSEN_KEY;
    std::string sValue;
    int ret = GetFromLevelDB(iGroupIdx,llMinKey, sValue);
    if (ret != 0 && ret != 1)
    {
        ERROR_LOG(" GetFromLevelDB fail, ret %d", ret);
        return ret;
    }

    if (ret == 1)
    {
        ERROR_LOG("no min chosen instanceid");
        llMinInstanceID = 0;
        return 0;
    }

    if (iGroupIdx >= (int)vec_poValueStore.size())
    {
        return -1;
    }
     //if (m_poValueStore->IsValidFileID(sValue))
    if (vec_poValueStore[iGroupIdx]->IsValidFileID(sValue))
    {
        ret = Get(iGroupIdx,llMinKey, sValue);
        if (ret != 0 && ret != 1)
        {
            ERROR_LOG("Get from log store fail, ret %d", ret);
            return ret;
        }
    }

    if (sValue.size() != sizeof(uint64_t))
    {
        ERROR_LOG("fail, mininstanceid size wrong");
        return -2;
    }

    memcpy(&llMinInstanceID, sValue.data(), sizeof(uint64_t));

    INFO_LOG("ok, min chosen instanceid %lu", llMinInstanceID);

    return 0;
}

int Database :: SetMinChosenInstanceID(const int iGroupIdx,const WriteOptions & oWriteOptions, const uint64_t llMinInstanceID)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }

    static uint64_t llMinKey = MINCHOSEN_KEY;
    string sValue = GenKey(llMinInstanceID);
    int ret = PutToLevelDB(/*false*/true, iGroupIdx , llMinKey, sValue );
    if (ret != 0)
    {
        return ret;
    }
    INFO_LOG("ok, set min chosen instanceid %lu groupid=%d", llMinInstanceID ,iGroupIdx );
    return 0;
}


int Database :: SetSystemVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer)
{
    static uint64_t llSystemVariablesKey = SYSTEMVARIABLES_KEY;
    return PutToLevelDB(/*true*/ false, iGroupIdx,llSystemVariablesKey, sBuffer);
}

int Database :: GetSystemVariables(const int iGroupIdx,std::string & sBuffer)
{
    static uint64_t llSystemVariablesKey = SYSTEMVARIABLES_KEY;
    return GetFromLevelDB(iGroupIdx,llSystemVariablesKey, sBuffer);
}

int Database :: SetMasterVariables(const WriteOptions & oWriteOptions,const int iGroupIdx, const std::string & sBuffer)
{
    static uint64_t llMasterVariablesKey = MASTERVARIABLES_KEY;
    return PutToLevelDB(/*true*/ false,iGroupIdx, llMasterVariablesKey, sBuffer);
}

int Database :: GetMasterVariables(const int iGroupIdx,std::string & sBuffer)
{
    static uint64_t llMasterVariablesKey = MASTERVARIABLES_KEY;
    return GetFromLevelDB( iGroupIdx, llMasterVariablesKey, sBuffer);
}

bool Database :: CreatCheckPoint(const std::string & path)
{
    bool bIsDir=false;
    int a = FileUtils::IsDir(path,  bIsDir);
    if( bIsDir ){
        int b = FileUtils::DeleteDir(path);
        if( b!=0 ){
            ERROR_LOG("DeleteDir error");
            return false;
        }
    }
    
    auto start = std::chrono::system_clock::now();

    rocksdb::Status oStatus = checkpoint_ptr->CreateCheckpoint( path );
    auto end = std::chrono::system_clock::now();
     std::chrono::duration<double> elapsed_seconds = end - start;

    if( oStatus.ok() ){
        int size = FileUtils::GetdirSize( path );//获取路径大小
        INFO_LOG( "CreateCheckpoint ok,path %s ,take %lu [s],size %lu[m]", path.c_str(), elapsed_seconds.count(),size );
        return true;
    }
    ERROR_LOG("CreatCheckPoint error. status %s,path %s,take %lu [s]",oStatus.ToString().c_str(),
    path.c_str(), elapsed_seconds.count() );
    return false; 
}

int Database :: KvGet(const std::string & sKey, std::string & sValue )
{
     auto start = std::chrono::system_clock::now();
    rocksdb::Status oStatus = m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], sKey, &sValue);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    
    if (!oStatus.ok())
    {
        if (oStatus.IsNotFound())
        {
            ERROR_LOG("KvGet not found, key %s", sKey.c_str());
            return KVCLIENT_KEY_NOTEXIST;
        }
        
        ERROR_LOG("KvGet fail, key %s", sKey.c_str());
        return KVCLIENT_SYS_FAIL;
    }
    std::string field_value = "";
    uint32_t ttl=0;
    kv_encode::DecodeStringValue( sValue,ttl , field_value );
    if (ttl!= 0 && ttl <= Time::GetTimestampSec() )
    {
        ERROR_LOG("rocksdb.Get key already ttl, key %s", sKey.c_str());
        return KVCLIENT_KEY_NOTEXIST;
    }
    sValue = field_value;

    INFO_LOG("KvGet OK, key %s value_size %d ", sKey.c_str(), sValue.length() );

    return KVCLIENT_OK;
}
int Database :: KvSet(const std::string & sKey, const std::string & sValue )
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    rocksdb::Status oStatus = m_rocksdb->Put(rocksdb::WriteOptions(), vec_cf[1] , sKey, sValue);
    
    if (!oStatus.ok())
    {
        ERROR_LOG("KvSet fail, key %s bufferlen %zu", sKey.c_str(), sValue.size());
        return KVCLIENT_SYS_FAIL;
    }

    INFO_LOG("KvSet OK, key: %s value: %s ", sKey.c_str(), sValue.c_str());

    return KVCLIENT_OK;
}
int Database :: Del_myself(const std::string & sKey)
{
    rocksdb::Status oStatus = m_rocksdb->Delete(rocksdb::WriteOptions(),  vec_cf[1] ,sKey );
    if (!oStatus.ok())
    {
        if (oStatus.IsNotFound())
        {
            return KVCLIENT_KEY_NOTEXIST;
        }
        
        return KVCLIENT_SYS_FAIL;
    }

    return KVCLIENT_OK;
}

int Database :: KvDel(const std::string & sKey )
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    std::string sServerValue;
    int ret = KvGet(sKey, sServerValue );
    if (ret != KVClientRet::KVCLIENT_OK && ret != KVClientRet::KVCLIENT_KEY_NOTEXIST)
    {
        return KVClientRet::KVCLIENT_SYS_FAIL;
    }
    
    rocksdb::Status oStatus = m_rocksdb->Delete(rocksdb::WriteOptions(), vec_cf[1] , sKey);
    if (!oStatus.ok())
    {
        ERROR_LOG("KvDel fail, key %s bufferlen %zu", sKey.c_str(), sKey.size());
        return KVCLIENT_SYS_FAIL;
    }

    INFO_LOG("KvDel OK, key %s ", sKey.c_str());

    return KVCLIENT_OK;
    
}
int Database :: KvGetCheckpointInstanceID(const uint16_t iGroupIdx,uint64_t & llCheckpointInstanceID)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    string sKey;
    sKey.append((char *)&iGroupIdx, sizeof(uint16_t));

    static uint64_t llCheckpointInstanceIDKey = KV_CHECKPOINT_KEY;
    sKey.append((char *)&llCheckpointInstanceIDKey, sizeof(uint64_t));

    string sBuffer;
    rocksdb::Status oStatus = m_rocksdb->Get(rocksdb::ReadOptions(),  vec_cf[1] ,sKey, &sBuffer);
    if (!oStatus.ok())
    {
        if (oStatus.IsNotFound())
        {
            return KVCLIENT_KEY_NOTEXIST;
        }
        
        return KVCLIENT_SYS_FAIL;
    }

    memcpy(&llCheckpointInstanceID, sBuffer.data(), sizeof(uint64_t));

    INFO_LOG("OK, get CheckpointInstanceID %lu", llCheckpointInstanceID);

    return KVCLIENT_OK;
}
int Database :: KvSetCheckpointInstanceID(const uint16_t iGroupIdx,const uint64_t llCheckpointInstanceID)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    string sKey;
    sKey.append((char *)&iGroupIdx, sizeof(uint16_t));
    
    static uint64_t llCheckpointInstanceIDKey = KV_CHECKPOINT_KEY;
    sKey.append((char *)&llCheckpointInstanceIDKey, sizeof(uint64_t));

    string sBuffer;
    sBuffer.append((char *)&llCheckpointInstanceID, sizeof(uint64_t));

    rocksdb::WriteOptions oWriteOptions;
    //must fync
    //FIXME
    oWriteOptions.sync = false;

    rocksdb::Status oStatus = m_rocksdb->Put(oWriteOptions, vec_cf[1] , sKey, sBuffer);
    if (!oStatus.ok())
    {
        ERROR_LOG("rocksdb.Put fail, bufferlen %zu", sBuffer.size());
        return KVCLIENT_SYS_FAIL;
    }

    INFO_LOG("OK, set CheckpointInstanceID %lu", llCheckpointInstanceID);

    return KVCLIENT_OK;
}

void Database :: DumpRocksDBStats()
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return ;
    }
    DEBUG_LOG("Database DumpRocksDBStats");
    BLOCK_CACHE_MISS->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_MISS));
    BLOCK_CACHE_HIT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_HIT));
    BLOCK_CACHE_ADD->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_ADD));
    BLOCK_CACHE_ADD_FAILURES->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_ADD_FAILURES));
    BLOCK_CACHE_INDEX_MISS->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_INDEX_MISS));
    BLOCK_CACHE_INDEX_HIT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_INDEX_HIT));
    BLOCK_CACHE_INDEX_ADD->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_INDEX_ADD));
    BLOCK_CACHE_INDEX_BYTES_INSERT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_INDEX_BYTES_INSERT));
    BLOCK_CACHE_INDEX_BYTES_EVICT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_INDEX_BYTES_EVICT));
    BLOCK_CACHE_FILTER_MISS->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_FILTER_MISS));
    BLOCK_CACHE_FILTER_HIT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_FILTER_HIT));
    BLOCK_CACHE_FILTER_ADD->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_FILTER_ADD));
    BLOCK_CACHE_FILTER_BYTES_INSERT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_FILTER_BYTES_INSERT));
    BLOCK_CACHE_FILTER_BYTES_EVICT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_FILTER_BYTES_EVICT));
    BLOCK_CACHE_DATA_MISS->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_DATA_MISS));
    BLOCK_CACHE_DATA_HIT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_DATA_HIT));
    BLOCK_CACHE_DATA_ADD->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_DATA_ADD));
    BLOCK_CACHE_DATA_BYTES_INSERT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_DATA_BYTES_INSERT));
    BLOCK_CACHE_BYTES_READ->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_BYTES_READ));
    BLOCK_CACHE_BYTES_WRITE->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_BYTES_WRITE));
    BLOOM_FILTER_USEFUL->SetValue(this->stats->getTickerCount(rocksdb::BLOOM_FILTER_USEFUL));
    BLOOM_FILTER_FULL_POSITIVE->SetValue(this->stats->getTickerCount(rocksdb::BLOOM_FILTER_FULL_POSITIVE));
    BLOOM_FILTER_FULL_TRUE_POSITIVE->SetValue(this->stats->getTickerCount(rocksdb::BLOOM_FILTER_FULL_TRUE_POSITIVE));
    PERSISTENT_CACHE_HIT->SetValue(this->stats->getTickerCount(rocksdb::PERSISTENT_CACHE_HIT));
    PERSISTENT_CACHE_MISS->SetValue(this->stats->getTickerCount(rocksdb::PERSISTENT_CACHE_MISS));
    SIM_BLOCK_CACHE_HIT->SetValue(this->stats->getTickerCount(rocksdb::SIM_BLOCK_CACHE_HIT));
    SIM_BLOCK_CACHE_MISS->SetValue(this->stats->getTickerCount(rocksdb::SIM_BLOCK_CACHE_MISS));
    MEMTABLE_HIT->SetValue(this->stats->getTickerCount(rocksdb::MEMTABLE_HIT));
    MEMTABLE_MISS->SetValue(this->stats->getTickerCount(rocksdb::MEMTABLE_MISS));
    GET_HIT_L0->SetValue(this->stats->getTickerCount(rocksdb::GET_HIT_L0));
    GET_HIT_L1->SetValue(this->stats->getTickerCount(rocksdb::GET_HIT_L1));
    GET_HIT_L2_AND_UP->SetValue(this->stats->getTickerCount(rocksdb::GET_HIT_L2_AND_UP));
    COMPACTION_KEY_DROP_NEWER_ENTRY->SetValue(this->stats->getTickerCount(rocksdb::COMPACTION_KEY_DROP_NEWER_ENTRY));
    COMPACTION_KEY_DROP_OBSOLETE->SetValue(this->stats->getTickerCount(rocksdb::COMPACTION_KEY_DROP_OBSOLETE));
    COMPACTION_KEY_DROP_RANGE_DEL->SetValue(this->stats->getTickerCount(rocksdb::COMPACTION_KEY_DROP_RANGE_DEL));
    COMPACTION_KEY_DROP_USER->SetValue(this->stats->getTickerCount(rocksdb::COMPACTION_KEY_DROP_USER));
    COMPACTION_RANGE_DEL_DROP_OBSOLETE->SetValue(this->stats->getTickerCount(rocksdb::COMPACTION_RANGE_DEL_DROP_OBSOLETE));
    COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE->SetValue(this->stats->getTickerCount(rocksdb::COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE));
    COMPACTION_CANCELLED->SetValue(this->stats->getTickerCount(rocksdb::COMPACTION_CANCELLED));
    NUMBER_KEYS_WRITTEN->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_KEYS_WRITTEN));
    NUMBER_KEYS_READ->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_KEYS_READ));
    NUMBER_KEYS_UPDATED->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_KEYS_UPDATED));
    BYTES_WRITTEN->SetValue(this->stats->getTickerCount(rocksdb::BYTES_WRITTEN));
    BYTES_READ->SetValue(this->stats->getTickerCount(rocksdb::BYTES_READ));
    NUMBER_DB_SEEK->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_DB_SEEK));
    NUMBER_DB_NEXT->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_DB_NEXT));
    NUMBER_DB_PREV->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_DB_PREV));
    NUMBER_DB_SEEK_FOUND->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_DB_SEEK_FOUND));
    NUMBER_DB_NEXT_FOUND->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_DB_NEXT_FOUND));
    NUMBER_DB_PREV_FOUND->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_DB_PREV_FOUND));
    ITER_BYTES_READ->SetValue(this->stats->getTickerCount(rocksdb::ITER_BYTES_READ));
    NO_FILE_CLOSES->SetValue(this->stats->getTickerCount(rocksdb::NO_FILE_CLOSES));
    NO_FILE_OPENS->SetValue(this->stats->getTickerCount(rocksdb::NO_FILE_OPENS));
    NO_FILE_ERRORS->SetValue(this->stats->getTickerCount(rocksdb::NO_FILE_ERRORS));
    STALL_L0_SLOWDOWN_MICROS->SetValue(this->stats->getTickerCount(rocksdb::STALL_L0_SLOWDOWN_MICROS));
    STALL_MEMTABLE_COMPACTION_MICROS->SetValue(this->stats->getTickerCount(rocksdb::STALL_MEMTABLE_COMPACTION_MICROS));
    STALL_L0_NUM_FILES_MICROS->SetValue(this->stats->getTickerCount(rocksdb::STALL_L0_NUM_FILES_MICROS));
    STALL_MICROS->SetValue(this->stats->getTickerCount(rocksdb::STALL_MICROS));
    DB_MUTEX_WAIT_MICROS->SetValue(this->stats->getTickerCount(rocksdb::DB_MUTEX_WAIT_MICROS));
    RATE_LIMIT_DELAY_MILLIS->SetValue(this->stats->getTickerCount(rocksdb::RATE_LIMIT_DELAY_MILLIS));
    NO_ITERATORS->SetValue(this->stats->getTickerCount(rocksdb::NO_ITERATORS));
    NUMBER_MULTIGET_CALLS->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_MULTIGET_CALLS));
    NUMBER_MULTIGET_KEYS_READ->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_MULTIGET_KEYS_READ));
    NUMBER_MULTIGET_BYTES_READ->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_MULTIGET_BYTES_READ));
    NUMBER_FILTERED_DELETES->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_FILTERED_DELETES));
    NUMBER_MERGE_FAILURES->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_MERGE_FAILURES));
    BLOOM_FILTER_PREFIX_CHECKED->SetValue(this->stats->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_CHECKED));
    BLOOM_FILTER_PREFIX_USEFUL->SetValue(this->stats->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_USEFUL));
    NUMBER_OF_RESEEKS_IN_ITERATION->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_OF_RESEEKS_IN_ITERATION));
    GET_UPDATES_SINCE_CALLS->SetValue(this->stats->getTickerCount(rocksdb::GET_UPDATES_SINCE_CALLS));
    BLOCK_CACHE_COMPRESSED_MISS->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_COMPRESSED_MISS));
    BLOCK_CACHE_COMPRESSED_HIT->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_COMPRESSED_HIT));
    BLOCK_CACHE_COMPRESSED_ADD->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_COMPRESSED_ADD));
    BLOCK_CACHE_COMPRESSED_ADD_FAILURES->SetValue(this->stats->getTickerCount(rocksdb::BLOCK_CACHE_COMPRESSED_ADD_FAILURES));
    WAL_FILE_SYNCED->SetValue(this->stats->getTickerCount(rocksdb::WAL_FILE_SYNCED));
    WAL_FILE_BYTES->SetValue(this->stats->getTickerCount(rocksdb::WAL_FILE_BYTES));
    WRITE_DONE_BY_SELF->SetValue(this->stats->getTickerCount(rocksdb::WRITE_DONE_BY_SELF));
    WRITE_DONE_BY_OTHER->SetValue(this->stats->getTickerCount(rocksdb::WRITE_DONE_BY_OTHER));
    WRITE_TIMEDOUT->SetValue(this->stats->getTickerCount(rocksdb::WRITE_TIMEDOUT));
    WRITE_WITH_WAL->SetValue(this->stats->getTickerCount(rocksdb::WRITE_WITH_WAL));
    COMPACT_READ_BYTES->SetValue(this->stats->getTickerCount(rocksdb::COMPACT_READ_BYTES));
    COMPACT_WRITE_BYTES->SetValue(this->stats->getTickerCount(rocksdb::COMPACT_WRITE_BYTES));
    FLUSH_WRITE_BYTES->SetValue(this->stats->getTickerCount(rocksdb::FLUSH_WRITE_BYTES));
    NUMBER_DIRECT_LOAD_TABLE_PROPERTIES->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_DIRECT_LOAD_TABLE_PROPERTIES));
    NUMBER_SUPERVERSION_ACQUIRES->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_SUPERVERSION_ACQUIRES));
    NUMBER_SUPERVERSION_RELEASES->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_SUPERVERSION_RELEASES));
    NUMBER_SUPERVERSION_CLEANUPS->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_SUPERVERSION_CLEANUPS));
    NUMBER_BLOCK_COMPRESSED->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_BLOCK_COMPRESSED));
    NUMBER_BLOCK_DECOMPRESSED->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_BLOCK_DECOMPRESSED));
    NUMBER_BLOCK_NOT_COMPRESSED->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_BLOCK_NOT_COMPRESSED));
    MERGE_OPERATION_TOTAL_TIME->SetValue(this->stats->getTickerCount(rocksdb::MERGE_OPERATION_TOTAL_TIME));
    FILTER_OPERATION_TOTAL_TIME->SetValue(this->stats->getTickerCount(rocksdb::FILTER_OPERATION_TOTAL_TIME));
    ROW_CACHE_HIT->SetValue(this->stats->getTickerCount(rocksdb::ROW_CACHE_HIT));
    ROW_CACHE_MISS->SetValue(this->stats->getTickerCount(rocksdb::ROW_CACHE_MISS));
    READ_AMP_ESTIMATE_USEFUL_BYTES->SetValue(this->stats->getTickerCount(rocksdb::READ_AMP_ESTIMATE_USEFUL_BYTES));
    READ_AMP_TOTAL_READ_BYTES->SetValue(this->stats->getTickerCount(rocksdb::READ_AMP_TOTAL_READ_BYTES));
    NUMBER_RATE_LIMITER_DRAINS->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_RATE_LIMITER_DRAINS));
    NUMBER_ITER_SKIP->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_ITER_SKIP));
    BLOB_DB_NUM_PUT->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_NUM_PUT));
    BLOB_DB_NUM_WRITE->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_NUM_WRITE));
    BLOB_DB_NUM_GET->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_NUM_GET));
    BLOB_DB_NUM_MULTIGET->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_NUM_MULTIGET));
    BLOB_DB_NUM_SEEK->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_NUM_SEEK));
    BLOB_DB_NUM_NEXT->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_NUM_NEXT));
    BLOB_DB_NUM_PREV->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_NUM_PREV));
    BLOB_DB_NUM_KEYS_WRITTEN->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_NUM_KEYS_WRITTEN));
    BLOB_DB_NUM_KEYS_READ->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_NUM_KEYS_READ));
    BLOB_DB_BYTES_WRITTEN->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_BYTES_WRITTEN));
    BLOB_DB_BYTES_READ->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_BYTES_READ));
    BLOB_DB_WRITE_INLINED->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_WRITE_INLINED));
    BLOB_DB_WRITE_INLINED_TTL->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_WRITE_INLINED_TTL));
    BLOB_DB_WRITE_BLOB->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_WRITE_BLOB));
    BLOB_DB_WRITE_BLOB_TTL->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_WRITE_BLOB_TTL));
    BLOB_DB_BLOB_FILE_BYTES_WRITTEN->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_BLOB_FILE_BYTES_WRITTEN));
    BLOB_DB_BLOB_FILE_BYTES_READ->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_BLOB_FILE_BYTES_READ));
    BLOB_DB_BLOB_FILE_SYNCED->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_BLOB_FILE_SYNCED));
    BLOB_DB_BLOB_INDEX_EXPIRED_COUNT->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_BLOB_INDEX_EXPIRED_COUNT));
    BLOB_DB_BLOB_INDEX_EXPIRED_SIZE->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_BLOB_INDEX_EXPIRED_SIZE));
    BLOB_DB_BLOB_INDEX_EVICTED_COUNT->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_BLOB_INDEX_EVICTED_COUNT));
    BLOB_DB_BLOB_INDEX_EVICTED_SIZE->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_BLOB_INDEX_EVICTED_SIZE));
    BLOB_DB_GC_NUM_FILES->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_GC_NUM_FILES));
    BLOB_DB_GC_NUM_NEW_FILES->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_GC_NUM_NEW_FILES));
    BLOB_DB_GC_FAILURES->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_GC_FAILURES));
    BLOB_DB_GC_NUM_KEYS_OVERWRITTEN->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_GC_NUM_KEYS_OVERWRITTEN));
    BLOB_DB_GC_NUM_KEYS_EXPIRED->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_GC_NUM_KEYS_EXPIRED));
    BLOB_DB_GC_NUM_KEYS_RELOCATED->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_GC_NUM_KEYS_RELOCATED));
    BLOB_DB_GC_BYTES_OVERWRITTEN->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_GC_BYTES_OVERWRITTEN));
    BLOB_DB_GC_BYTES_EXPIRED->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_GC_BYTES_EXPIRED));
    BLOB_DB_GC_BYTES_RELOCATED->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_GC_BYTES_RELOCATED));
    BLOB_DB_FIFO_NUM_FILES_EVICTED->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_FIFO_NUM_FILES_EVICTED));
    BLOB_DB_FIFO_NUM_KEYS_EVICTED->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_FIFO_NUM_KEYS_EVICTED));
    BLOB_DB_FIFO_BYTES_EVICTED->SetValue(this->stats->getTickerCount(rocksdb::BLOB_DB_FIFO_BYTES_EVICTED));
    TXN_PREPARE_MUTEX_OVERHEAD->SetValue(this->stats->getTickerCount(rocksdb::TXN_PREPARE_MUTEX_OVERHEAD));
    TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD->SetValue(this->stats->getTickerCount(rocksdb::TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD));
    TXN_DUPLICATE_KEY_OVERHEAD->SetValue(this->stats->getTickerCount(rocksdb::TXN_DUPLICATE_KEY_OVERHEAD));
    TXN_SNAPSHOT_MUTEX_OVERHEAD->SetValue(this->stats->getTickerCount(rocksdb::TXN_SNAPSHOT_MUTEX_OVERHEAD));
    NUMBER_MULTIGET_KEYS_FOUND->SetValue(this->stats->getTickerCount(rocksdb::NUMBER_MULTIGET_KEYS_FOUND));

    //////通过接口获取rocksdb状态///////
    string property="rocksdb.levelstats";
    string _value="";
    bool stat = GetLevelSstProperty( property, _value);
    int used_size = GetUsedRowCache();

}

bool Database :: KvBatchGet( const phxkv::KvBatchGetRequest* request, phxkv::KvBatchGetResponse* response )
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    DEBUG_LOG("Database :: KvBatchGet.");
    std::vector<rocksdb::Slice> keys;
    std::vector<std::string> vecValue;
    std::vector<rocksdb::ColumnFamilyHandle *> cfh;

    auto subs = request->subs();
    int idx = 0;
    for (const phxkv::KvBatchGetSubRequest &it : subs)
    {
        keys.push_back(rocksdb::Slice(it.key().data(), it.key().size()));
        cfh.push_back( vec_cf[1] );//从数据列 cf 中进行查询
    }
 
    rocksdb::ReadOptions ropt;
    auto rets = m_rocksdb->MultiGet(ropt, cfh, keys, &vecValue);

    if (rets.size() != vecValue.size() || vecValue.size() != keys.size())
    {
        ERROR_LOG( "results size != values size" );
        return false;
    }
 
    // results
    for (size_t i = 0; i < rets.size(); i++)
    {
         
        KvBatchGetSubResponse* sub = response->add_values();
        string sValue = vecValue[i];
        
        std::string field_value = "";
        uint32_t ttl=0;
        kv_encode::DecodeStringValue( sValue,ttl , field_value );
        if (ttl!= 0 && ttl <= Time::GetTimestampSec() )
        {
            ERROR_LOG("rocksdb.Get key already ttl, key %s", keys[i].data() );
            return KVCLIENT_KEY_NOTEXIST;
        }

        sub->set_value( field_value );
        sub->set_key( keys.at(i).data() );
        auto ret = rets[i];
        switch (ret.code())
        {
        case rocksdb::Status::kOk:
            sub->set_ret( KVCLIENT_OK );
            break;
        case rocksdb::Status::kNotFound:
            sub->set_ret( KVCLIENT_KEY_NOTEXIST );
            break;
        default:
            sub->set_ret(KVCLIENT_SYS_FAIL );
            break;
        }
    }
    DEBUG_LOG("Database :: KvBatchGet.5");
    return true;
}

bool Database :: KvBatchSet(const ::phxkv::KvBatchPutRequest request)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return -1;
    }
    rocksdb::WriteBatch batch;
    auto sub = request.subs();
    for (const phxkv::KvBatchPutSubRequest &it : sub)
    {
        string sValue= it.value();
        auto ret = batch.Put( vec_cf[1], it.key(), sValue);
        if (!ret.ok())
        {
            ERROR_LOG("KvBatchSet put error ret %s ",ret.ToString().c_str() );
            return false;
        }
    }

    rocksdb::WriteOptions wopt;
    auto ret = m_rocksdb->Write(wopt, &batch);
    if (!ret.ok())
    {
        ERROR_LOG("batch put error ret %s ",ret.ToString().c_str() );
        return false;    
    }

    return true;
}
bool Database :: HasOutTime(const uint32_t ttl)//判断是否过期
{
    if( ttl!= 0 && ttl < Time::GetTimestampSec() ){
         return true;
    }

    return false;
}
int Database :: HashDel(const ::phxkv::HashRequest* request,string hash_key)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    
    int cnt = 0;
    for (int k=0;k<request->field_size();k++ )
    {
        string key= request->field(k).field_key();
        string value="";
        rocksdb::Status oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], key, &value);
        if( !value.empty() ){
            cnt++;
            txn->Delete( vec_cf[1], key);
        }
    }
    if(cnt == 0 ){
        delete txn;
        ERROR_LOG("HashDel KVCLIENT_KEY_NOTEXIST");
        return KVCLIENT_KEY_NOTEXIST;
    }
    string str_meta_key =kv_encode::EncodeMetaKey( hash_key,HASH_META );
    string str_meta_value ="";
    rocksdb::Status oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( str_meta_value.empty() ){
        ERROR_LOG("HashDel KVCLIENT_META_NOTEXIST");
        delete txn;
        return KVCLIENT_META_NOTEXIST;
    }
    uint32_t ttl=0;
    uint64_t length =0;
    kv_encode::DecodeHashMetaValue(str_meta_value,ttl ,length );
    if(length < cnt ){
        ERROR_LOG("HashDel length error.cur %lu other %lu", length, cnt );
        delete txn;
        return KVCLIENT_PARAM_ERROR;
    }
    length = length- cnt;
    str_meta_value = kv_encode::EncodeHashMetaValue(ttl ,length );
    if(length!= 0 ){
        txn->Put(vec_cf[1], str_meta_key , str_meta_value );//还有剩余元素，则更新长度
    }else{
        txn->Delete(vec_cf[1], str_meta_key  );//没有元素了，则删除hash
    }
    oStatus = txn->Commit();
    if( !oStatus.ok() ){
        txn->Rollback();
        ERROR_LOG("HashDel KVCLIENT_BUSY.");
        delete txn;
        return KVCLIENT_BUSY;
    }
    INFO_LOG("HashDel ok");
    delete txn;
    return KVCLIENT_OK;
}
int Database :: HashGet(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response,string hash_key)
{
    INFO_LOG("HashGet start");
    return HashMget( request, response, hash_key);
}
int Database :: HashSet(const ::phxkv::HashRequest* request ,std::string hash_key)
{
    INFO_LOG("HashSet start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if(request->field_size()==0  ){
        ERROR_LOG("HashSet KVCLIENT_PARAM_ERROR field_size=0");
        return KVCLIENT_PARAM_ERROR;
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
    if( 0== request->field_size() ){
        ERROR_LOG("HashSet KVCLIENT_PARAM_ERROR field_size=0");
        return KVCLIENT_PARAM_ERROR;
    }
    string key= request->field(0).field_key();
    string value= "";
    int cnt=0;
    rocksdb::Status oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], key, &value);
    if( value.empty()){
        cnt++;
    }
    txn->Put(vec_cf[1], key,request->field(0).field_value()  ); //数据

    uint32_t ttl=0;
    uint64_t length =0;
    string str_meta_key =kv_encode::EncodeMetaKey( hash_key,HASH_META );
    string str_meta_value ="";
    oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( !str_meta_value.empty() ){
        kv_encode::DecodeHashMetaValue(str_meta_value,ttl ,length );
        length+=cnt;
    }else{
        length =cnt;
    }
    
    str_meta_value = kv_encode::EncodeHashMetaValue(ttl ,length );

    txn->Put(vec_cf[1], str_meta_key , str_meta_value );//meta
    oStatus = txn->Commit();
    if (!oStatus.ok())
    {
        ERROR_LOG("HashSet  error ret %s ",oStatus.ToString().c_str() );
        txn->Rollback();
        delete txn;
        return KVCLIENT_ROCKSDB_ERR;    
    }
    delete txn;
    INFO_LOG("HashSet ok");
    return KVCLIENT_OK;
}
int Database :: HashGetAll( const ::phxkv::HashRequest* request,::phxkv::HashResponse* response,string hash_key )
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    string str_meta_key =kv_encode::EncodeMetaKey( hash_key ,HASH_META);
    string str_meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( str_meta_value.empty() ){
        ERROR_LOG("meta not exist.");
        return KVCLIENT_META_NOTEXIST;
    }
    uint32_t ttl=0;
    uint64_t length =0;
    kv_encode::DecodeHashMetaValue(str_meta_value,ttl ,length );
    
    if( HasOutTime(ttl) ){
        ERROR_LOG("hash del has ttl");
        return KVCLIENT_KEY_NOTEXIST;
    }

    std::string dbkey;
    
    std::string key_start = hash_key;
    key_start = kv_encode::EncodeKey(key_start,HASH_TYPE);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    it = m_rocksdb->NewIterator(iterate_options, vec_cf[1] );
    it->Seek(key_start);
    while (it->Valid()) {
        string key = it->key().ToString();
        string value = it->value().ToString();
         if ( (it->key())[0] != HASH_TYPE) {  //类型相等
            break;
       }
       kv_encode::DecodeKey(key, dbkey);
       if (dbkey == hash_key) {//加个验证
           dbkey="";
           phxkv::HashField * field = response->add_field();
           field->set_field_key(key);
           field->set_field_value(value);
       } else {
           INFO_LOG("length error key size=%d dbkey size=%d",hash_key.size(),dbkey.size() );
           break;
       }
       it->Next();
    }
    
    delete it;
    INFO_LOG("HashGetAll ok");
    return KVCLIENT_OK;
}
int Database :: HashExist( const ::phxkv::HashRequest* request,::phxkv::HashResponse* response,string hash_key)
{
    INFO_LOG("HashExist start");
    return HashGet( request ,  response, hash_key);
}
int Database ::  HashIncrByInt(const ::phxkv::HashRequest* request,string hash_key,std::string & ret_value,const int type )
{
    INFO_LOG("HashIncrBy start type=%d" , type );
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if( 0 ==  request->field_size() ){
        ERROR_LOG("HashIncrBy KVCLIENT_PARAM_ERROR type=%d", type );
        return KVCLIENT_PARAM_ERROR;
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
    std::string meta_key = kv_encode::EncodeMetaKey(hash_key ,HASH_META );

    std::string meta_value = "";
    rocksdb::Status oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;
    uint64_t length=0;
    
    string field_key= request->field(0).field_key();
    string field_value="";

    if( meta_value.empty() ){//空表
        if( 0 == type ){
            field_value = StringFormat::IntToStr(  request->int_value() );
        }else{
            field_value = StringFormat::FloatToStr(  request->float_value() );
        }
        ret_value = field_value;
        txn->Put( vec_cf[1] , field_key, field_value);//更新字段
        length++;
    }else{
        kv_encode::DecodeHashMetaValue(meta_value , ttl,length );
        //是否存在
        field_value="";
        txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);

        if( !field_value.empty() ){//存在，判断是不是数值
            if( 0 == type  ){
                if( !StringFormat::StrIsInt( field_value ) ){
                    ERROR_LOG("HashIncrByint KVCLIENT_PARAM_ERROR");
                    delete txn;
                    return KVCLIENT_PARAM_ERROR;
                }
                INFO_LOG("keyyyy=%s",field_value.c_str() );
                field_value = StringFormat::IntToStr(  request->int_value() + atoi(field_value.c_str() )  );
            }else{
                if( !StringFormat::StrIsFloat( field_value ) ){
                    ERROR_LOG("HashIncrByfloat KVCLIENT_PARAM_ERROR");
                    delete txn;
                    return KVCLIENT_PARAM_ERROR;
                }
                INFO_LOG("keyyyy=%s",field_value.c_str() );
                field_value = StringFormat::FloatToStr(  request->float_value() + atof(field_value.c_str() )  );
            }
            ret_value = field_value;
            txn->Put( vec_cf[1] , field_key, field_value);//更新字段
        }else{
            if(0 == type ){
                field_value = StringFormat::IntToStr(  request->int_value() );
            }else{
                field_value = StringFormat::FloatToStr(  request->float_value() );
            }
            ret_value = field_value;
            txn->Put( vec_cf[1] , field_key, field_value);//更新字段
            length++;
        }
    }
    //更新meta
    meta_value =kv_encode::EncodeHashMetaValue(ttl ,length );
    txn->Put(vec_cf[1] , meta_key, meta_value);
    oStatus = txn->Commit();
    if (!oStatus.ok())
    {   txn->Rollback();
        delete txn;
        ERROR_LOG("HashIncrBy fail, key %s ,type=%d ", hash_key.c_str() ,type );
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("HashIncrBy ok type = %d",type );
    return KVCLIENT_OK;
}
int Database ::  HashIncrByFloat(const ::phxkv::HashRequest* request,string hash_key,std::string & ret_value)
{
    INFO_LOG("HashIncrByFloat start");
    return HashIncrByInt(request,hash_key,ret_value , 1 );
}
int Database :: HashKeys(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,string hash_key)
{
    return HashGetAll(request , response,hash_key);
     
}
int Database :: HashLen(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response ,string hash_key,uint64_t &length)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    length=0;
    string str_meta_key =kv_encode::EncodeMetaKey(hash_key,HASH_META );
    string str_meta_value ="";
    rocksdb::Status oStatus = m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( str_meta_value.empty() ){
        ERROR_LOG("meta not exist.");
        return KVCLIENT_META_NOTEXIST;
    }
    uint32_t ttl=0;
    kv_encode::DecodeHashMetaValue(str_meta_value,ttl ,length );
    
    if( HasOutTime(ttl) ){
        ERROR_LOG("hash del has ttl");
        return KVCLIENT_KEY_NOTEXIST;
    }
    INFO_LOG("hash length =%d", length );
    return KVCLIENT_OK;
}
int Database :: HashMget( const ::phxkv::HashRequest* request,::phxkv::HashResponse* response,string hash_key)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //是否过期
     string str_meta_key =kv_encode::EncodeMetaKey( hash_key ,HASH_META);
    string str_meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( str_meta_value.empty() ){
        ERROR_LOG("meta not exist.");
        return KVCLIENT_META_NOTEXIST;
    }
    uint32_t ttl=0;
    uint64_t length =0;
    kv_encode::DecodeHashMetaValue(str_meta_value,ttl ,length );
    
    if( HasOutTime(ttl) ){
        ERROR_LOG("hash del has ttl");
        return KVCLIENT_KEY_NOTEXIST;
    }

    std::vector<rocksdb::Slice> keys;
    std::vector<std::string> vecValue;
    std::vector<rocksdb::ColumnFamilyHandle *> cfh;
    
    for (int k=0;k< request->field_size();k++ )
    {
        string str_key = request->field(k).field_key();
        keys.push_back(rocksdb::Slice( str_key ) );
        cfh.push_back( vec_cf[1] );//从数据列 cf 中进行查询
    }
 
    rocksdb::ReadOptions ropt;
    auto rets = m_rocksdb->MultiGet(ropt, cfh, keys, &vecValue);
    if (rets.size() != vecValue.size() || vecValue.size() != keys.size())
    {
        ERROR_LOG( "results size != values size" );
        return KVCLIENT_SYS_FAIL;
    }
    if( vecValue.size() == 1 && vecValue[0].empty() ){
        ERROR_LOG("HashMget no exist " );
        return KVCLIENT_KEY_EXIST;
    }
    for(int l=0;l<vecValue.size();l++ ){
        phxkv::HashField * field = response->add_field();
        field->set_field_key( keys.at(l).data() );
        field->set_field_value( vecValue.at(l) );
    }
    INFO_LOG("HashMget ok=size=%d",vecValue.size() );
    return KVCLIENT_OK;
}
int Database :: HashMset(const ::phxkv::HashRequest* request ,string hash_key)
{
    INFO_LOG("HashMset start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
     
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
    uint16_t cnt=0;
    
    for(int k=0;k<request->field_size();k++  ){
        string key= request->field(k).field_key();
        string value= request->field(k).field_value();
        
        string dbvalue="";
       rocksdb::Status oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], key, &dbvalue);
       if( dbvalue.empty()){
           cnt++;
       }
        txn->Put(vec_cf[1], key,value  ); //数据
    }
    INFO_LOG("mset1 key size=%d cnt=%d", request->field_size() ,cnt );
    uint32_t ttl = 0;
    uint64_t length =0;
    string str_meta_key =kv_encode::EncodeMetaKey( hash_key ,HASH_META);
    string str_meta_value ="";
    rocksdb::Status oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( !str_meta_value.empty() ){
        kv_encode::DecodeHashMetaValue(str_meta_value,ttl ,length );
        length+= cnt;
    }else{
        length = cnt;
    }
     INFO_LOG("mset2 key size=%d cnt=%d", length ,cnt );
    str_meta_value = kv_encode::EncodeHashMetaValue(ttl ,length );
    rocksdb::WriteOptions wopt;
    txn->Put(vec_cf[1], str_meta_key , str_meta_value );//meta
    oStatus = txn->Commit();
    if (!oStatus.ok())
    {   txn->Rollback();
        delete txn;
        ERROR_LOG("Hasmset  error ret %s ",oStatus.ToString().c_str() );
        return KVCLIENT_ROCKSDB_ERR;    
    }
    delete txn;
    INFO_LOG("hashmset ok");
    return KVCLIENT_OK;
}
int Database :: HashSetNx(const ::phxkv::HashRequest* request,string hash_key )
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }//存在则返回
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);

    string key= request->field(0).field_key();
    string value= request->field(0).field_value();
    txn->Put(vec_cf[1], key,value  ); //数据

    string str_meta_key =kv_encode::EncodeMetaKey( hash_key,HASH_META );
    string str_meta_value ="";
    rocksdb::Status oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    
    uint32_t ttl=0;
    uint64_t length =0;
    if( str_meta_value.empty() ){
        length =1;
    }else{
        kv_encode::DecodeHashMetaValue(str_meta_value,ttl,length );
        length+=1;
    }
    str_meta_value = kv_encode::EncodeHashMetaValue(ttl ,length );
     
    txn->Put(vec_cf[1], str_meta_key , str_meta_value );//meta
    oStatus = txn->Commit();
    if (!oStatus.ok())
    {
        txn->Rollback();
        delete txn;
        ERROR_LOG("HashDel  error ret %s ",oStatus.ToString().c_str() );
        return KVCLIENT_ROCKSDB_ERR;    
    }
    delete txn;
    INFO_LOG("HashSetNx ok");
    return KVCLIENT_OK;
     
}
int Database :: HashValues(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,string hash_key)
{
    INFO_LOG("HashValues start");
    return HashGetAll(request , response,hash_key);
}

//==key相关===
bool Database::KeyType(const string& key,int &key_type)
{
     
    for(int i=STRING_TYPE;i<=ZSET_TYPE ;i++ )
    {
        int meta_type = 0;
        switch ( i )
        {
        case STRING_TYPE:
            meta_type = STRING_TYPE;
            break;
        case LIST_TYPE:
            meta_type = LIST_META;
            break;
        case HASH_TYPE:
            meta_type = HASH_META;
            break;
        case SET_TYPE:
            meta_type = SET_META;
            break;
        case ZSET_TYPE:
            meta_type = ZSET_META;
            break;
        default:
            break;
        }
        string sKey = kv_encode::EncodeMetaKey( key,key_type );
        string sValue ;
        int ret = KvGet(sKey, sValue );
        if ( !sValue.empty() )
        {
            key_type = i;
            return true;
        }
    }
    return false;
}
int Database::ExpireKey( const string& key,const int32_t new_ttl )
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //获取类型
    int type=0;
    if( !KeyType(key ,type) ){
        
        return KVCLIENT_SYS_FAIL;
    }
    //对类型进行删除,更新ttl，异步删除
    string str_meta_key =kv_encode::EncodeMetaKey( key,type );
    string str_meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( str_meta_value.empty() ){
       
        ERROR_LOG("meta not exist.");
        return KVCLIENT_META_NOTEXIST;
    }
    uint32_t ttl=0;
    uint64_t length = 0 ;
    switch (type)
    {
    case STRING_TYPE:
        m_rocksdb->Delete( rocksdb::WriteOptions(), vec_cf[1],key  );
        break;
    case HASH_TYPE:
        kv_encode::DecodeHashMetaValue(str_meta_value , ttl, length );
        ttl=new_ttl;
        str_meta_value = kv_encode::EncodeHashMetaValue(ttl , length );
        m_rocksdb->Put(rocksdb::WriteOptions(), vec_cf[1],str_meta_key , str_meta_value );
        break;
    case LIST_TYPE:
        {uint64_t list_head=0;uint64_t list_tail=0;uint64_t list_next_seq=0;
        kv_encode::DecodeListMetaValue(str_meta_value , ttl, length ,list_head , list_tail , list_next_seq );
        ttl=new_ttl;
        str_meta_value = kv_encode::EncodeListMetaValue(ttl , length , list_head,list_tail,list_next_seq );
        m_rocksdb->Put(rocksdb::WriteOptions(), vec_cf[1],str_meta_key , str_meta_value );
        break;}
    case SET_TYPE:
        kv_encode::DecodeSetMetaValue(str_meta_value , ttl, length );
        ttl=new_ttl;
        str_meta_value = kv_encode::EncodeSetMetaValue(ttl , length );
        m_rocksdb->Put(rocksdb::WriteOptions(), vec_cf[1],str_meta_key , str_meta_value );
        break;
    case ZSET_TYPE:
        kv_encode::DecodeZSetMetaValue(str_meta_value , ttl, length );
        ttl=new_ttl;
        str_meta_value = kv_encode::EncodeZSetMetaValue(ttl , length );
        m_rocksdb->Put(rocksdb::WriteOptions(), vec_cf[1],str_meta_key , str_meta_value );
        break;
    
    default:
        break;
    }
    INFO_LOG("DelKey ok");
    return KVCLIENT_OK;
}
int Database::DelKey(  const string& key)
{
    const int ttl= Time::GetTimestampSec() -1 ;
    return ExpireKey(key , ttl );
}

///list========
int Database::ListLpush(const ::phxkv::ListRequest* request,std::string list_key,const int type)
{
    //先把待插入解节点排好序，在附加到原头节点的前
    INFO_LOG("ListLpush start.");
    if (!m_bHasInit)
    {
        ERROR_LOG("ListLpush no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //从meta中取出应该分配的序号
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    rocksdb::Status oStatus;
    //读出meta状态
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=1;

    if( !list_meta_value.empty() ){
        kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    }else{
        if( 1 == type ){
            delete txn;
            ERROR_LOG("ListLpushx KVCLIENT_META_NOTEXIST");
            return KVCLIENT_META_NOTEXIST;
        }
    }
    int field_size = request->field_size();
    uint64_t pre_seq=0; 
    uint64_t new_haead_seq= list_next_seq;
   
    for(int k= field_size-1 ;k>=0 ;k--  ){
        
        string field = request->field(  k ) ;
        std::string new_node_key = "";
        std::string new_node_value ="";
    
        new_node_key = kv_encode::EncodeListFieldKey(list_key , list_next_seq);//把新节点放进去
        if( 0== k ){  //待插入的是最后一个
            new_node_value =kv_encode::EncodeListFieldValue(pre_seq, list_head, field );
        }else{
            new_node_value =kv_encode::EncodeListFieldValue(pre_seq, list_next_seq+1, field );
        }
        txn->Put(vec_cf[1], new_node_key, new_node_value );
        INFO_LOG("value=%s=pre=%d=next=%d=cur=%d", field.c_str() ,pre_seq, list_next_seq+1, list_next_seq  );
        pre_seq = list_next_seq;//当前节点成为了前一个
        list_next_seq++;
    }
    //更新原来的头节点信息
    if(0 != list_head ){
        string src_head_key = kv_encode::EncodeListFieldKey(list_key , list_head );
         string src_head_value ="";
        txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], src_head_key, &src_head_value);
        uint64_t src_pre_seq=0;uint64_t src_next_seq=0;std::string src_value="";
        kv_encode::DecodeListFieldValue(src_head_value ,src_pre_seq,src_next_seq , src_value );
        src_head_value = kv_encode::EncodeListFieldValue(pre_seq , src_next_seq, src_value );

        txn->Put(vec_cf[1], src_head_key, src_head_value );
        INFO_LOG("head value=pre=%d=next=%d=cur=%d", pre_seq, src_next_seq, list_head  );
    }else{
        list_tail =pre_seq;
    } 
    //更新meta
    list_meta_value = kv_encode::EncodeListMetaValue(ttl,length+field_size,new_haead_seq,list_tail,list_next_seq );//更新元数据
    txn->Put(vec_cf[1], list_meta_key, list_meta_value);

    oStatus= txn->Commit();
    if( !oStatus.ok() ){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ListLpush error.");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ListLpush ok.");
    return KVCLIENT_OK;
}
int Database::ListLpushx(const ::phxkv::ListRequest* request,std::string list_key)
{
    //先把待插入解节点排好序，在附加到原头节点的前
    INFO_LOG("ListLpushx start.");
    return ListLpush(request ,list_key , 1 );
}

int Database::ListLpop(const ::phxkv::ListRequest* request,std::string list_key,std::string& ret_value)
{
    INFO_LOG("ListLpop start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //从meta中取出表头
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    if( list_meta_value.empty() ){
        delete txn;
        ERROR_LOG("ListLpop meta_value empty");
       return KVCLIENT_META_NOTEXIST;
    } 
    
    //读取meta，找到表头
    std::string value="";
     uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=0;
    kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );

    INFO_LOG("ListLpop start11 length=%d list_head=%d ,list_tail=%d ,list_next_seq=%d data=%d",length, 
            list_head , list_tail , list_next_seq ,list_meta_value.length());
    //读旧表头的序号
    string field_key = kv_encode::EncodeListFieldKey(list_key , list_head);//旧节点的信息
    string field_value="";
    
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
   
    uint64_t pre_seq=0;uint64_t next_seq=0; //读旧表头的值
    kv_encode::DecodeListFieldValue(field_value,pre_seq, next_seq,value );//返回解码之后的
    ret_value = value;//用来返回
    //删除表头
    txn->Delete(vec_cf[1], field_key);
    //设置新表头的前一个节点
    length-=1;
    if(0 == length ){
        txn->Delete(vec_cf[1],list_meta_key);
    }else{
        list_head = next_seq;//成为表头
        //更新meta
        list_meta_value = kv_encode::EncodeListMetaValue(ttl,length,list_head,list_tail,list_next_seq );//更新元数据
        txn->Put(vec_cf[1], list_meta_key, list_meta_value);

        //更新节点
        field_key = kv_encode::EncodeListFieldKey(list_key ,next_seq );
        txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
         
        kv_encode::DecodeListFieldValue( field_value,pre_seq ,next_seq, value );//读到新头的next序号
        //新节点的下一个节点
    
        field_value = kv_encode::EncodeListFieldValue(0, next_seq,value );
        txn->Put( vec_cf[1], field_key, field_value);
    }
    rocksdb::Status oStatus= txn->Commit();
    if( !oStatus.ok() ){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ListLpop error");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ListLpop ok");
    return KVCLIENT_OK;
}
int Database::ListLength(const ::phxkv::ListRequest* request,::phxkv::ListResponse* response,::string list_key,uint64_t& length)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //从meta中取出应该分配的序号
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    if( list_meta_value.empty() ){
       return KVCLIENT_META_NOTEXIST;
    } 
    //执行push
    uint32_t ttl=0;;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=0;
    kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    if( HasOutTime(ttl) ){
        length = 0;
        return KVCLIENT_META_NOTEXIST;
    }
      
    return KVCLIENT_OK;
}
int Database::ListRpop(  const ::phxkv::ListRequest* request, std::string list_key ,std::string& ret_value)//弹出表尾，更新表尾
{
    INFO_LOG("ListRpop start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //从meta中取出表头
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    std::string value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    if( list_meta_value.empty() ){
        delete txn;
        ERROR_LOG("ListRpop meta_value empty");
       return KVCLIENT_META_NOTEXIST;
    } 
    
    //读取meta，找到表头
     uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=0;
    kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
     
    INFO_LOG("ListRpop start11 length=%d list_head=%d ,list_tail=%d ,list_next_seq=%d data=%d",length, 
            list_head , list_tail , list_next_seq ,list_meta_value.length());
    //读旧表尾的序号
    string field_key = kv_encode::EncodeListFieldKey(list_key , list_tail);//旧节点的信息
    string field_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
   
    uint64_t pre_seq=0;uint64_t next_seq=0; //读旧表尾的值
    kv_encode::DecodeListFieldValue(field_value,pre_seq, next_seq,value );//返回解码之后的
    ret_value = value;
    //删除表尾
    txn->Delete(vec_cf[1], field_key);
    //设置新表头的前一个节点
    length-=1;
    if(0 == length ){
        txn->Delete(vec_cf[1],list_meta_key);
    }else{
        list_tail = pre_seq;//成为表尾
        //更新meta
        list_meta_value = kv_encode::EncodeListMetaValue(ttl,length,list_head,list_tail,list_next_seq );//更新元数据
        txn->Put(vec_cf[1], list_meta_key, list_meta_value);

        //更新节点
        field_key = kv_encode::EncodeListFieldKey(list_key ,list_tail );
        txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
         
        kv_encode::DecodeListFieldValue( field_value,pre_seq ,next_seq, value );//读到新头的next序号
        //新节点的下一个节点
    
        field_value = kv_encode::EncodeListFieldValue(pre_seq, 0,value );
        txn->Put( vec_cf[1], field_key, field_value);
    }
    rocksdb::Status oStatus= txn->Commit();
    if( !oStatus.ok() ){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ListRpop error");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ListRpop ok=%s" , ret_value.c_str() );
    return KVCLIENT_OK;
}
int Database::ListRpopLpush(  const ::phxkv::ListRequest* request,const std::string list_dest,std::string &ret_value )//尾部进行头插
{
    INFO_LOG("ListRpopLpush =%s ==%s" ,request->src_list().c_str(), list_dest.c_str() );
    // 将列表 source 中的最后一个元素(尾元素)弹出，并返回给客户端。
    // 将 source 弹出的元素插入到列表 destination ，作为 destination 列表的的头元素。
    int ret = ListRpop(request , request->src_list() , ret_value );
    if( KVCLIENT_OK == ret ){
        phxkv::ListRequest t_request;
        t_request.CopyFrom( *request );
        t_request.add_field()->assign(ret_value.data()  ,ret_value.length() );
        return ListLpush( &t_request , list_dest);
    }else{
        return ret ;
    }
}
int Database::ListIndex(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key)//插入到指定位置
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //从meta中取出应该分配的序号
    int index = request->index();
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    if( list_meta_value.empty() ){
        delete txn;
        ERROR_LOG("ListIndex KVCLIENT_META_NOTEXIST");
       return KVCLIENT_META_NOTEXIST;
    } 
    //存在，执行操作
    uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=0;
    kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    if(index >= length ){
        delete txn;
        ERROR_LOG("ListIndex KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }
    uint64_t cur_seq=list_head;//从头开始
    INFO_LOG("ListLength list_head=%d ", list_head);
    uint64_t pre_seq=0;uint64_t next_seq=0;string value="";
    while(index-->=0 ){

       string field_key = kv_encode::EncodeListFieldKey(list_key , cur_seq);//节点的信息
       string field_value="";
       txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
       
       kv_encode::DecodeListFieldValue(field_value,pre_seq, next_seq,value );
       cur_seq = next_seq;
    }
    std::string* add_field = response->add_field();
    add_field->assign(value.data(), value.size() );
    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ListIndex KVCLIENT_PARAM_ERROR");
        return KVCLIENT_ROCKSDB_ERR;
    }
     delete txn;
    return KVCLIENT_OK;
}
int Database::ListInsert(  const ::phxkv::ListRequest* request, std::string list_key )//插入到指定位置
{
    INFO_LOG("ListInsert start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if( 0==request->field_size()  ){
         ERROR_LOG("ListInsert KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }
    //从meta中取出应该分配的序号 
    const int pos_flag = request->pos_flag();
    const std::string pivit  = request->pivot();
    const std::string insert_value = request->field(0);
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    if( list_meta_value.empty() ){
        delete txn;
       return KVCLIENT_META_NOTEXIST;
    } 
    //存在，执行操作
    uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=0;
    kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    INFO_LOG("ListInsert start1");
    uint64_t cur_seq=list_head;//与三个节点相关

    uint64_t pre_seq=0;uint64_t next_seq=0;
    string field_key = "";std::string field_value="";string value="";
    while( cur_seq!=0 ){ //找到目标节点
        
       field_key = kv_encode::EncodeListFieldKey(list_key , cur_seq);//节点的信息
       txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
       
       kv_encode::DecodeListFieldValue(field_value,pre_seq, next_seq,value );
       if( value == pivit){
           INFO_LOG("ListInsert find ok");
           break;
       }
       cur_seq = next_seq;
    }
    if(value != pivit ){//没找到
        delete txn;
        ERROR_LOG("ListInsert KVCLIENT_KEY_EXIST.");
        return KVCLIENT_KEY_NOTEXIST;   
    }
    INFO_LOG("ListInsert start22");
    if(1== pos_flag ){ //后面加
        //更新目标节点
        INFO_LOG("ListInsert start23");
        field_key = kv_encode::EncodeListFieldKey(list_key ,cur_seq);
        field_value = kv_encode::EncodeListFieldValue(pre_seq,list_next_seq,pivit );
        txn->Put(vec_cf[1] ,field_key ,field_value );
        //加入新节点
        field_key = kv_encode::EncodeListFieldKey(list_key ,list_next_seq);
        field_value = kv_encode::EncodeListFieldValue(cur_seq,next_seq,insert_value );
        txn->Put(vec_cf[1] ,field_key ,field_value );
        //读取目标节点的下一个,如果有
        if( next_seq!= 0 ){
            INFO_LOG("ListInsert start24");
            field_key= kv_encode::EncodeListFieldKey(list_key ,next_seq );
            txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1],field_key , &field_value );
            string _value="";
            kv_encode::DecodeListFieldValue(field_value , pre_seq , next_seq,_value );

            field_value = kv_encode::EncodeListFieldValue(list_next_seq,next_seq,_value );
            txn->Put(vec_cf[1] ,field_key ,field_value );
        }else{
            INFO_LOG("ListInsert start25");
            list_tail = list_next_seq;//自己成为尾节点
        }
        //更新meta    
        list_meta_value = kv_encode::EncodeListMetaValue(ttl,length+1,list_head , list_tail,list_next_seq+1);
        txn->Put(vec_cf[1] ,list_meta_key ,list_meta_value );                                  
    }else if(0 == pos_flag ){   //前面加
        
    }
    
    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        return KVCLIENT_ROCKSDB_ERR;
    }
     delete txn;
     INFO_LOG("ListInsert ok");
    return KVCLIENT_OK;
}
int Database::ListRange(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    int start = request->start();//从0开始
    int end = request->end();
    //查询表长
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction * txn = m_rocksdb->BeginTransaction(write_options);
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=1;
    if( !list_meta_value.empty() ){

        kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    }else{
        ERROR_LOG("ListRange KVCLIENT_META_NOTEXIST");
        return KVCLIENT_META_NOTEXIST;
    }
    start = (start>=0)?(start) :(start+ length) ;
    end = (end>=0)?(end) :(end+ length) ;
    if ( end<0 || start > end || start >= length  ) {
        ERROR_LOG("ListRange KVCLIENT_PARAM_ERROR start = %d end=%d length=%d",start ,end );
        
        return KVCLIENT_PARAM_ERROR;
    }
    if (start < 0) {
        start = 0;
    }
    if (end >= length) {
        end = length - 1;
    }
    //表头
    int index = start;
    uint64_t cur_seq=list_head;
    uint64_t pre_seq=0;uint64_t next_seq=0;string value="";
    while(index-- >0 ){

       string field_key = kv_encode::EncodeListFieldKey(list_key , cur_seq);//节点的信息
       string field_value="";
       txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
       
       kv_encode::DecodeListFieldValue(field_value,pre_seq, next_seq,value );
       cur_seq = next_seq;
    }
    while (start <= end )
    {
        std::string cur_key = kv_encode::EncodeListFieldKey(list_key , cur_seq);//节点的信息
       string cur_value="";
       txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], cur_key, &cur_value);
       
       kv_encode::DecodeListFieldValue(cur_value,pre_seq, next_seq,value );
       cur_seq = next_seq;
       start++;
       std::string * field = response->add_field();
       field->assign( value.data(),value.length() );
    }
    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ListRange KVCLIENT_BUSY");
        return KVCLIENT_BUSY;
    }
     delete txn;
     ERROR_LOG("ListRange KVCLIENT_OK");
     return KVCLIENT_OK;
}
int Database::ListRemRight(  const ::phxkv::ListRequest* request, std::string list_key  )
{
    INFO_LOG("ListRemRight start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    int count = abs( request->count() );
    const string & des_value = request->field(0);
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    if( list_meta_value.empty() ){
        delete txn;
        INFO_LOG("ListRem KVCLIENT_META_NOTEXIST");
       return KVCLIENT_META_NOTEXIST;
    } 
    //存在，执行操作
    uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=0;
    kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    
    uint64_t cur_seq=list_tail;//从表尾部开始
    uint64_t pre_seq=0;uint64_t next_seq=0;string value="";
    int num=0;
    while(num < count && cur_seq != 0 ){

       string field_key = kv_encode::EncodeListFieldKey(list_key , cur_seq);//当前读到的节点的信息
       string field_value="";
       txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
       kv_encode::DecodeListFieldValue(field_value,pre_seq, next_seq,value );
       cur_seq = pre_seq;
       
       if(value == des_value){
           txn->Delete( vec_cf[1] ,field_key );//删掉当前
           length--;
           //更新前一个节点的next
           std::string cur_key = "";
           std::string cur_value="";
           uint64_t cur_pre_seq=0;uint64_t cur_next_seq=0;std::string cur_field_value="";
           if(pre_seq != 0 ){ 
               cur_key = kv_encode::EncodeListFieldKey(list_key ,pre_seq );//当前点的上一个
                txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1],cur_key , &cur_value );                
                kv_encode::DecodeListFieldValue(cur_value , cur_pre_seq ,cur_next_seq , cur_field_value );
                cur_value = kv_encode::EncodeListFieldValue(cur_pre_seq , next_seq,cur_field_value );

                txn->Put(vec_cf[1] ,cur_key ,cur_value );  //更新上一个
                //更新下一个
                if( next_seq!= 0  ){
                    cur_key = kv_encode::EncodeListFieldKey(list_key ,next_seq );
                    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1],cur_key , &cur_value );
                    kv_encode::DecodeListFieldValue(cur_value , cur_pre_seq ,cur_next_seq , cur_field_value );
                    cur_value = kv_encode::EncodeListFieldValue(pre_seq , cur_next_seq,cur_field_value );
                    txn->Put(vec_cf[1] ,cur_key ,cur_value );  //更新上一个
                }else{
                    list_tail = pre_seq;//更新尾
                }
           }else{   //把头删了
                
                if(next_seq != 0 ){ //有下一个
                    cur_key = kv_encode::EncodeListFieldKey(list_key ,next_seq );//当前点的下一个
                    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1],cur_key , &cur_value );
                    kv_encode::DecodeListFieldValue(cur_value , cur_pre_seq ,cur_next_seq , cur_field_value );
                    cur_value = kv_encode::EncodeListFieldValue(pre_seq , cur_next_seq,cur_field_value );
                    txn->Put(vec_cf[1] ,cur_key ,cur_value );  //更新
                    list_head = next_seq;//更新头
                }else{ // 没有了不管
                    ERROR_LOG("length=0 ?=%d",length );
                }
           }
           num++;
       }
    }
    //更新元数据
    list_meta_value = kv_encode::EncodeListMetaValue(ttl,length ,list_head,list_tail,list_next_seq );
    txn->Put(vec_cf[1] ,list_meta_key , list_meta_value );  
    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        INFO_LOG("ListRem KVCLIENT_ROCKSDB_ERR");
        return KVCLIENT_ROCKSDB_ERR;
    }
     delete txn;
     INFO_LOG("ListRem ok");
    return KVCLIENT_OK;
}
int Database::ListRem(  const ::phxkv::ListRequest* request, std::string list_key  )//删除N个等值元素
{
    // count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count 。
    // count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值。
    // count = 0 : 移除表中所有与 value 相等的值。
    INFO_LOG("ListRem start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    int count = request->count();
    if( count < 0 ){
        return ListRemRight( request, list_key );
    }
    const string & des_value = request->field(0);
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    if( list_meta_value.empty() ){
        delete txn;
        INFO_LOG("ListRem KVCLIENT_META_NOTEXIST");
       return KVCLIENT_META_NOTEXIST;
    } 
    //存在，执行操作
    uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=0;
    kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    
    uint64_t cur_seq=list_head;
    uint64_t pre_seq=0;uint64_t next_seq=0;string value="";
    int num=0;
    if( 0 == count ){
        count = length;
    }
    while(num < count && cur_seq != 0 ){

       string field_key = kv_encode::EncodeListFieldKey(list_key , cur_seq);//当前读到的节点的信息
       string field_value="";
       txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
       kv_encode::DecodeListFieldValue(field_value,pre_seq, next_seq,value );
       cur_seq = next_seq;
       
       if(value == des_value){
           txn->Delete( vec_cf[1] ,field_key );//删掉当前
           length--;
           //更新前一个节点的next
           std::string cur_key = "";
           std::string cur_value="";
           uint64_t cur_pre_seq=0;uint64_t cur_next_seq=0;std::string cur_field_value="";
           if(pre_seq != 0 ){ 
               cur_key = kv_encode::EncodeListFieldKey(list_key ,pre_seq );//当前点的上一个
                txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1],cur_key , &cur_value );                
                kv_encode::DecodeListFieldValue(cur_value , cur_pre_seq ,cur_next_seq , cur_field_value );
                cur_value = kv_encode::EncodeListFieldValue(cur_pre_seq , next_seq,cur_field_value );

                txn->Put(vec_cf[1] ,cur_key ,cur_value );  //更新上一个
                //更新下一个
                if( next_seq!= 0  ){
                    cur_key = kv_encode::EncodeListFieldKey(list_key ,next_seq );
                    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1],cur_key , &cur_value );
                    kv_encode::DecodeListFieldValue(cur_value , cur_pre_seq ,cur_next_seq , cur_field_value );
                    cur_value = kv_encode::EncodeListFieldValue(pre_seq , cur_next_seq,cur_field_value );
                    txn->Put(vec_cf[1] ,cur_key ,cur_value );  //更新上一个
                }else{
                    list_tail = pre_seq;//更新尾
                }
           }else{   //把头删了
                
                if(next_seq != 0 ){ //有下一个
                    cur_key = kv_encode::EncodeListFieldKey(list_key ,next_seq );//当前点的下一个
                    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1],cur_key , &cur_value );
                    kv_encode::DecodeListFieldValue(cur_value , cur_pre_seq ,cur_next_seq , cur_field_value );
                    cur_value = kv_encode::EncodeListFieldValue(pre_seq , cur_next_seq,cur_field_value );
                    txn->Put(vec_cf[1] ,cur_key ,cur_value );  //更新
                    list_head = next_seq;//更新头
                }else{ // 没有了不管
                    ERROR_LOG("length=0 ?=%d",length );
                }
           }
           num++;
       }
    }
    //更新元数据
    list_meta_value = kv_encode::EncodeListMetaValue(ttl,length ,list_head,list_tail,list_next_seq );
    txn->Put(vec_cf[1] ,list_meta_key , list_meta_value );  
    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        INFO_LOG("ListRem KVCLIENT_ROCKSDB_ERR");
        return KVCLIENT_ROCKSDB_ERR;
    }
     delete txn;
     INFO_LOG("ListRem ok");
    return KVCLIENT_OK;
}
int Database::ListSet(  const ::phxkv::ListRequest* request, std::string list_key )//根据下表进行更新元素
{
    INFO_LOG("ListSet start.");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if( 0 ==request->field_size() ){
        ERROR_LOG("ListSet KVCLIENT_PARAM_ERROR");
       return KVCLIENT_PARAM_ERROR;
    }
    int index = request->index();
    const string new_value = request->field(0);

    //从meta中取出应该分配的序号
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    if( list_meta_value.empty() ){
        delete txn;
        ERROR_LOG("ListSet KVCLIENT_META_NOTEXIST");
       return KVCLIENT_META_NOTEXIST;
    } 
    //存在，执行操作
    uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=0;
    kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    if(index >=  length ){
         ERROR_LOG("ListSet KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;//
    }
    uint64_t cur_seq=list_head;
    uint64_t pre_seq=0;uint64_t next_seq=0;string value="";string field_key ="";string field_value="";
    while(index-- >= 0 ){

       field_key= kv_encode::EncodeListFieldKey(list_key , cur_seq);//节点的信息
       txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);
       kv_encode::DecodeListFieldValue(field_value,pre_seq, next_seq,value );
       cur_seq = next_seq;
    }
    field_value = kv_encode::EncodeListFieldValue( pre_seq ,next_seq,new_value );
    txn->Put(vec_cf[1], field_key, field_value );
    INFO_LOG("field_key size=%d=%d ",field_key.size(),field_value.size()  );
    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ListSet KVCLIENT_ROCKSDB_ERR.");
        return KVCLIENT_ROCKSDB_ERR;
    }
     delete txn;
     INFO_LOG("ListSet ok.");
    return KVCLIENT_OK;

}
int Database::ListTtim(  const ::phxkv::ListRequest* request, std::string list_key )//保留指定区间元素，其余删除
{
    INFO_LOG("ListTtim start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    int start = request->start();//区间保留
    int end = request->end();
    //查询表长
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=1;
    if( !list_meta_value.empty() ){

        kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    }else{
        delete txn;
        ERROR_LOG("ListTtim KVCLIENT_META_NOTEXIST");
        return KVCLIENT_META_NOTEXIST;
    }
    start = (start>=0)?(start) :(start+ length) ;
    end = (end>=0)?(end) :(end+ length) ;
    if ( end<0 || start > end || start >= length  ) {
        ERROR_LOG("start = %d end=%d length=%d",start ,end );
        return KVCLIENT_PARAM_ERROR;
    }
    if (start < 0) {
        start = 0;
    }
    if (end >= length) {
        end = length - 1;
    }
    //表头
    INFO_LOG("tetst start=%d===%d===%d" , start,end,length );
    int index = 0;
    uint64_t cur_seq=list_head;
    uint64_t new_head_next=0;//新的头节点的后一个序号      .[x x x x ]....... 区间内的不动，只更改区间外的pre_seq or next_seq
    std::string head_value="";//新的头节点的后value
    uint64_t new_tail_pre=0;//新尾节点的上一个序号
    std::string tail_value="";//新尾节点的value
    while( cur_seq!=0 ){//到表尾为止
        
        //需要删除
        string field_key = kv_encode::EncodeListFieldKey(list_key , cur_seq);
        string field_value="";
        txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value);

        uint64_t pre_seq=0;uint64_t next_seq=0;string value="";
        kv_encode::DecodeListFieldValue(field_value,pre_seq, next_seq,value );
        if(index <start || index > end ){
            txn->Delete(vec_cf[1],field_key);
            length--;
        }
        
       if( index == start ){   //更新头
            new_head_next = next_seq;
            list_head = cur_seq;
            head_value = value;
       }
       if( index == end ){
           new_tail_pre = pre_seq;
           list_tail =cur_seq;
           tail_value = value;
       }   
        index++;
        cur_seq = next_seq;
    }
    //更新头，尾部
    string new_head_key = kv_encode::EncodeListFieldKey(list_key , list_head );
    std::string new_head_value = kv_encode::EncodeListFieldValue(0,new_head_next,head_value );
    txn->Put(vec_cf[ 1 ], new_head_key ,new_head_value);

    string new_tail_key = kv_encode::EncodeListFieldKey(list_key , list_tail );
    std::string new_tail_value = kv_encode::EncodeListFieldValue( new_tail_pre ,0,tail_value );
    txn->Put(vec_cf[ 1 ], new_tail_key ,new_tail_value);
    //更新meta
    
    list_meta_value = kv_encode::EncodeListMetaValue(ttl, length ,list_head ,list_tail , list_next_seq );
    txn->Put(vec_cf[ 1 ], list_meta_key ,list_meta_value);
    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ListTtim KVCLIENT_ROCKSDB_ERR");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ListTtim ok");
    return KVCLIENT_OK;
}
int Database::ListRpush(  const ::phxkv::ListRequest* request, std::string list_key,const int type)//尾插法
{
    //先把待插入解节点排好序，在附加到原头节点的后面
    INFO_LOG("ListRpush start.");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //从meta中取出应该分配的序号
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    rocksdb::Status oStatus;
    //读出meta状态
    std::string list_meta_key = kv_encode::EncodeMetaKey( list_key,LIST_META  );
    std::string list_meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], list_meta_key, &list_meta_value);
    uint32_t ttl=0;uint64_t length=0;uint64_t list_head=0; uint64_t list_tail=0;uint64_t list_next_seq=1;

    if( !list_meta_value.empty() ){
        kv_encode::DecodeListMetaValue(list_meta_value, ttl,length,list_head,list_tail,list_next_seq );
    }else{
        if( 1 == type ){
            delete txn;
            ERROR_LOG("ListRpushx KVCLIENT_META_NOTEXIST");
            return KVCLIENT_META_NOTEXIST;
        }
    }
    
    int field_size = request->field_size();
    uint64_t pre_seq=list_tail; 
    uint64_t src_head_next=list_next_seq;//新插入的表的头，
    for(int k= 0 ;k<= field_size-1 ; k++ ){
        
        string field = request->field(  k ) ;
        std::string new_node_key = "";
        std::string new_node_value ="";
    
        new_node_key = kv_encode::EncodeListFieldKey(list_key , list_next_seq);//把新节点放进去
        if( field_size-1 == k ){  
            new_node_value =kv_encode::EncodeListFieldValue(pre_seq, 0, field );
        }else{
            new_node_value =kv_encode::EncodeListFieldValue(pre_seq, list_next_seq+1, field );
        }
        txn->Put(vec_cf[1], new_node_key, new_node_value );
        pre_seq = list_next_seq;//当前节点成为了前一个
        list_next_seq++;
    }
    
    //更新原来的尾节点信息
    if(0 != list_tail ){
        string src_tail_key = kv_encode::EncodeListFieldKey(list_key , list_tail );
         string src_tail_value ="";
        txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], src_tail_key, &src_tail_value);
        uint64_t src_pre_seq=0;uint64_t src_next_seq=0;std::string src_value="";
        kv_encode::DecodeListFieldValue(src_tail_value ,src_pre_seq,src_next_seq , src_value );
        src_tail_value = kv_encode::EncodeListFieldValue(src_pre_seq , src_head_next , src_value );//下一个节点是新的头

        txn->Put(vec_cf[1], src_tail_key, src_tail_value );
    }else{
        list_head =src_head_next;
    } 
    //更新meta
    list_meta_value = kv_encode::EncodeListMetaValue(ttl,length+field_size,list_head ,pre_seq,list_next_seq );//更新元数据
    txn->Put(vec_cf[1], list_meta_key, list_meta_value);

    oStatus= txn->Commit();
    if( !oStatus.ok() ){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ListLpush error.");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ListLpush ok.");
    return KVCLIENT_OK;
}
int Database::ListRpushx(  const ::phxkv::ListRequest* request, std::string list_key )//链表存在时，才执行尾部插入
{
    INFO_LOG("ListRpushx start.");
    return ListRpush(  request, list_key,1 );//尾插法
}

int Database::SAdd(  const ::phxkv::SetRequest* request,const std::string & set_key )
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    int set_size = 0;//更新后的长度
    rocksdb::Status oStatus;
    for(int k=0;k< request->field_size();k++ ){
        string field_key = request->field(k);
        string field_value="";
        oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value );
        
        txn->Put(vec_cf[1] ,field_key, "" );
        set_size+= oStatus.ok() ? 0 : 1 ;
    }
    std::string _meta_key = kv_encode::EncodeMetaKey( set_key,SET_META  );
    std::string _meta_value="";
    
    uint32_t ttl=0;uint64_t length=0;
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], _meta_key, &_meta_value);
    if( !_meta_value.empty() ){
        kv_encode::DecodeSetMetaValue(_meta_value,ttl ,length );
        set_size += length ;
    } 
    //存在
    _meta_value = kv_encode::EncodeSetMetaValue(ttl, set_size );
    txn->Put( vec_cf[1], _meta_key, _meta_value);

    oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("SAdd ok.");
    return KVCLIENT_OK;
}
int Database::SRem(  const ::phxkv::SetRequest* request, const std::string & set_key)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    int set_size = 0;//更新后的长度
    rocksdb::Status oStatus;
    for(int k=0;k< request->field_size();k++ ){
        string field_key = request->field(k);
        string field_value="";
        oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value );

        if( oStatus.ok() ){
            txn->Delete(vec_cf[1] ,field_key );
            set_size++;
        }
    }
    std::string _meta_key = kv_encode::EncodeMetaKey( set_key,SET_META  );
    std::string _meta_value="";
    
    uint32_t ttl=0;uint64_t length=0;
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], _meta_key, &_meta_value);
    if( !_meta_value.empty() ){
        kv_encode::DecodeSetMetaValue(_meta_value,ttl ,length );
        length -= set_size ;
    } 
    //存在
    _meta_value = kv_encode::EncodeSetMetaValue(ttl, length );
    if( 0 ==length ){
        txn->Delete( vec_cf[1], _meta_key);
    }else{
        txn->Put( vec_cf[1], _meta_key, _meta_value);
    }

    oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        ERROR_LOG("SRem error");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("SRem ok");
    return KVCLIENT_OK;
}
int Database::SCard(  const ::phxkv::SetRequest* request,uint64_t & length,const std::string & set_key)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    
    std::string _meta_key = kv_encode::EncodeMetaKey( set_key,SET_META  );
    std::string _meta_value="";
    uint32_t ttl=0;
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], _meta_key, &_meta_value);
    if( !_meta_value.empty() ){
        kv_encode::DecodeSetMetaValue(_meta_value,ttl ,length );
    } 
    
    INFO_LOG("SCard ok");
    return KVCLIENT_OK;
}
int Database::SMembers(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string & set_key)
{
    INFO_LOG("SMembers start=%s",set_key.c_str() );
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::set<std::string> set_result;
    int ret = Members(set_key ,set_result );//解码后的
    if(set_result.empty() ){
        return ret;
    }
    std::set<std::string>::iterator iter= set_result.begin();
    for( ; iter!= set_result.end();iter++ ){
        std::string field_key = *iter;
        response->add_field()->append(field_key.data(), field_key.length() );
    }
    INFO_LOG("SMembers ok size=%d",set_result.size() );
    return KVCLIENT_OK;
}
int Database::Members(const std::string & set_key , set<std::string>& resust)
{
    INFO_LOG("Members start...");
    string str_meta_key =kv_encode::EncodeMetaKey( set_key ,SET_META);
    string str_meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( str_meta_value.empty() ){
        ERROR_LOG("meta not exist.");
        return KVCLIENT_META_NOTEXIST;
    }
    uint32_t ttl=0;
    uint64_t length =0;
    kv_encode::DecodeSetMetaValue(str_meta_value,ttl ,length );
    
    if( HasOutTime(ttl) ){
        ERROR_LOG("hash del has ttl");
        return KVCLIENT_KEY_NOTEXIST;
    }

    std::string dbkey;
    std::string key_start = set_key;
    key_start = kv_encode::EncodeKey(key_start,SET_TYPE);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    it = m_rocksdb->NewIterator(iterate_options, vec_cf[1] );
    it->Seek(key_start);
    while (it->Valid()) {
        string key = it->key().ToString();
        if ( (it->key())[0] != SET_TYPE) {  //类型相等
            break;
       }
        string field_key = "";
       kv_encode::DecodeSetFieldKey(key, dbkey ,field_key);
       if (dbkey == set_key) {//加个验证
           dbkey="";
           resust.insert( field_key);//解码，直接返回
       } else {
           INFO_LOG("length error key size=%d dbkey size=%d",set_key.size(),dbkey.size() );
           break;
       }
       it->Next();
    }
    delete it;
    INFO_LOG("Members ok");
    return KVCLIENT_OK;
}
int Database::DeleteKey(const std::string & set_key)
{
    INFO_LOG("DeleteKey start...");
    string str_meta_key =kv_encode::EncodeMetaKey( set_key ,SET_META);
    string str_meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( str_meta_value.empty() ){
        ERROR_LOG("meta not exist.");
        return KVCLIENT_META_NOTEXIST;
    }
    uint32_t ttl=0;
    uint64_t length =0;
    kv_encode::DecodeSetMetaValue(str_meta_value,ttl ,length );

    std::string dbkey;
    std::string key_start = set_key;
    key_start = kv_encode::EncodeKey(key_start,SET_TYPE);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    it = m_rocksdb->NewIterator(iterate_options, vec_cf[1] );
    it->Seek(key_start);
    while (it->Valid()) {
        string key = it->key().ToString();
        string value = it->value().ToString();
       if ( (it->key())[0] != HASH_TYPE) {  //类型相等
            break;
       }
       kv_encode::DecodeKey(key, dbkey);
       if (dbkey == set_key) {//加个验证
           dbkey="";
           
       } else {
           INFO_LOG("length error key size=%d dbkey size=%d",set_key.size(),dbkey.size() );
           break;
       }
       it->Next();
    }
    delete it;
    INFO_LOG("Members ok");
    return KVCLIENT_OK;
}
int Database::SUnionStore(  const ::phxkv::SetRequest* request,const std::string & set_key)
{
    INFO_LOG("SUnionStore start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
     if(request->src_set_size()==0  ){
        ERROR_LOG("SInterStore KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }

    std::set<std::string> result_;
    for(int k=0;k< request->src_set().size();k++ ){
        std::string set_t = request->src_set().at(k);
        Members( set_t,result_ );//解码
     }
    if( 0 == result_.size() ){
        ERROR_LOG("SUnionStore KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    
    rocksdb::Status oStatus;
    //删除原集合的旧数据
    std::string dbkey;
    std::string key_start = set_key;
    key_start = kv_encode::EncodeKey(key_start,SET_TYPE);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    it = m_rocksdb->NewIterator(iterate_options, vec_cf[1] );
    it->Seek(key_start);
    while (it->Valid()) {
        string key = it->key().ToString();
        if ( (it->key())[0] != SET_TYPE) {  //类型相等
            break;
       }
       kv_encode::DecodeKey(key, dbkey);
       if (dbkey == set_key) {//加个验证
           dbkey="";
            txn->Delete(vec_cf[1], key );
       } else {
           INFO_LOG("length error key size=%d dbkey size=%d",set_key.size(),dbkey.size() );
           break;
       }
       it->Next();
    }
    delete it;

    std::set<std::string>::iterator iter = result_.begin();

    uint64_t size =result_.size();
    for(;iter!=result_.end() ; iter++ ){
        string field_key = *iter;
        field_key = kv_encode::EncodeSetFieldKey(set_key ,field_key ); 
        txn->Put(vec_cf[1] ,field_key, "" );//直接写
    }
    
    std::string _meta_key = kv_encode::EncodeMetaKey( set_key,SET_META  );
    std::string _meta_value="";
    txn->GetForUpdate( rocksdb::ReadOptions(),vec_cf[1], _meta_key,&_meta_value );
     
    uint32_t ttl=0;uint64_t length=0;  
    if( !_meta_value.empty() ){
        kv_encode::DecodeSetMetaValue(_meta_value,ttl ,length );
    } 
    _meta_value = kv_encode::EncodeSetMetaValue(ttl, size );
    txn->Put( vec_cf[1], _meta_key, _meta_value);
    oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        INFO_LOG("SUnionStore error");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
     INFO_LOG("SUnionStore ok");
    return KVCLIENT_OK;
}
int Database::SUnion(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response )
{
    INFO_LOG("SUnion start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if(request->src_set_size()==0  ){
        ERROR_LOG("SInterStore KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }
    std::set<std::string> result_;
    for(int k=0;k< request->src_set().size();k++ ){
        std::string set_t = request->src_set().at(k);
        Members( set_t,result_ );
     }

    std::set<std::string>::iterator iter=result_.begin();
    for( ;iter!=result_.end() ; iter++ ){
        std::string field = *iter;
        response->add_field()->assign( field.data() ,field.size() );
    }
    INFO_LOG("SUnion ok");
    return KVCLIENT_OK;

}
int Database::SInterStore(  const ::phxkv::SetRequest* request,  const std::string & set_key)
{
    INFO_LOG("SInterStore start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if(request->src_set_size()==0  ){
        ERROR_LOG("SInterStore KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    rocksdb::Status oStatus;

    std::map<std::string,uint8_t> map_data_cnt;
    std::set<std::string> src_result_;
    Members( request->src_set().at(0),src_result_ );
    
    for(int k=1;k< request->src_set().size();k++ ){
        std::set<std::string> result_;
        std::string set_t = request->src_set().at(k);
        Members( set_t,result_ );
        
        std::set<std::string>::iterator iter= result_.begin();
        for(  ;iter!= result_.end();iter++ ){
            std::string key_t = *iter;
            map_data_cnt[ key_t ]++;
        }
    }
    //删除原集合的旧数据
    std::string dbkey;
    std::string key_start = set_key;
    key_start = kv_encode::EncodeKey(key_start,SET_TYPE);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    it = m_rocksdb->NewIterator(iterate_options, vec_cf[1] );
    it->Seek(key_start);
    while (it->Valid()) {
        string key = it->key().ToString();
        if ( (it->key())[0] != SET_TYPE) {  //类型相等
            break;
       }

       kv_encode::DecodeKey(key, dbkey);
       if (dbkey == set_key) {//加个验证
           dbkey="";
            txn->Delete(vec_cf[1], key );
       } else {
           INFO_LOG("length error key size=%d dbkey size=%d",set_key.size(),dbkey.size() );
           break;
       }
       it->Next();
    }
    delete it;

    int set_size = 0;
    std::set<std::string>::iterator iter= src_result_.begin();
    for(;iter!=src_result_.end() ; iter++ ){
        string field_key = *iter;
        if( map_data_cnt.count( field_key ) > 0 && map_data_cnt[field_key]== request->src_set().size()-1 ){
            
            string field_value="";
            field_key = kv_encode::EncodeSetFieldKey(set_key , field_key  );
            oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value );
            set_size++;
            txn->Put(vec_cf[1] ,field_key, "" );
        }
    }
    std::string _meta_key = kv_encode::EncodeMetaKey( set_key,SET_META  );
    std::string _meta_value="";
    
    uint32_t ttl=0;
    //存在
    _meta_value = kv_encode::EncodeSetMetaValue(ttl, set_size );
    txn->Put( vec_cf[1], _meta_key, _meta_value);

    oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
    }
    delete txn;
    return KVCLIENT_OK;
}

int Database::SInter(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response)
{
    INFO_LOG("SInter start");//自己有，别人也都有
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if(request->src_set_size()==0  ){
        ERROR_LOG("SInterStore KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }

    std::map<std::string,uint8_t> map_data_cnt;
    std::set<std::string> src_result_;
    Members( request->src_set().at(0),src_result_ );//本身
   
    std::set<std::string>::iterator iter;;//
    for(int k=1;k< request->src_set().size();k++ ){
        std::set<std::string> result_;//其他
        std::string set_t = request->src_set().at(k);
        Members( set_t,result_ );
        
        iter= result_.begin();
        for(  ;iter!= result_.end();iter++ ){
            std::string key_t = *iter;
            map_data_cnt[ key_t ]++;
        }
    }

    iter=src_result_.begin();
    for( ;iter!=src_result_.end() ; iter++ ){
        std::string field = *iter ;
        if( map_data_cnt.count(field)>0  && map_data_cnt[field] == request->src_set().size()-1 ){
            response->add_field()->assign( field.data() ,field.size() );
        }
    }
    return KVCLIENT_OK;
}
int Database::SDiffStore(  const ::phxkv::SetRequest* request, const std::string & set_key)
{
    //自己有，其他都没有
    INFO_LOG("SDiff start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if(request->src_set_size()==0  ){
        ERROR_LOG("SInterStore KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }

    std::set<std::string> result_;
    for(int k=1;k< request->src_set().size();k++ ){
        std::string set_t = request->src_set().at(k);
        Members( set_t,result_ );
    }

    std::set<std::string> src_result_;
    Members( request->src_set().at(0),src_result_ );

    rocksdb::WriteOptions write_options;
    rocksdb::Status oStatus;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务

    //删除原集合的旧数据
    std::string dbkey;
    std::string key_start = set_key;
    key_start = kv_encode::EncodeKey(key_start,SET_TYPE);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    it = m_rocksdb->NewIterator(iterate_options, vec_cf[1] );
    it->Seek(key_start);
    while (it->Valid()) {
        string key = it->key().ToString();
        if ( (it->key())[0] != SET_TYPE) {  //类型相等
            break;
       }
       kv_encode::DecodeKey(key, dbkey);
       if (dbkey == set_key) {//加个验证
           dbkey="";
            txn->Delete(vec_cf[1], key );
       } else {
           INFO_LOG("length error key size=%d dbkey size=%d",set_key.size(),dbkey.size() );
           break;
       }
       it->Next();
    }
    delete it;

    uint64_t length=0;;//更新后的长度
    std::set<std::string>::iterator iter=src_result_.begin();
    for( ;iter!=src_result_.end() ; iter++ ){
        std::string field = *iter;
        if( result_.count( field )<= 0 ){
            string field_key = kv_encode::EncodeSetFieldKey(set_key , field  );
            txn->Put(vec_cf[1] ,field_key, "" );
            length++;
        }
    }

    std::string _meta_key = kv_encode::EncodeMetaKey( set_key,SET_META );
    std::string _meta_value="";
    
    uint32_t ttl=0;
    _meta_value = kv_encode::EncodeSetMetaValue(ttl, length );
    txn->Put( vec_cf[1], _meta_key, _meta_value);

    oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        return KVCLIENT_BUSY;
    }
    delete txn;
    return KVCLIENT_OK; 
}

int Database::SDiff(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response  )
{
    //自己有，其他都没有
    INFO_LOG("SDiff start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if(request->src_set_size()==0  ){
        ERROR_LOG("SInterStore KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }
    std::set<std::string> result_;
    for(int k=1;k< request->src_set().size();k++ ){
        std::string set_t = request->src_set().at(k);
        Members( set_t,result_ );
    }

    std::set<std::string> src_result_;
    Members( request->src_set().at(0),src_result_ );

    std::set<std::string>::iterator iter=src_result_.begin();
    for( ;iter!=src_result_.end() ; iter++ ){
        std::string field = *iter;
        if( result_.count( field )<= 0 ){
            response->add_field()->assign( field.data() ,field.size() );
        }
    }
    return KVCLIENT_OK;
}

int Database::SIsMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string & set_key)
{
    INFO_LOG("SIsMember start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if( 0 == request->field_size()  ){
        ERROR_LOG("SInterStore KVCLIENT_PARAM_ERROR");
        return KVCLIENT_PARAM_ERROR;
    }
    string key= request->field(0);
    string value="";
    rocksdb::Status oStatus = m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], key, &value);
    if( !oStatus.ok() ){
        ERROR_LOG("ket not exist.");
        return KVCLIENT_KEY_NOTEXIST;
    }
    
    string str_meta_key =kv_encode::EncodeMetaKey( set_key,SET_META );
    string str_meta_value ="";
    oStatus = m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( str_meta_value.empty() ){
        ERROR_LOG("meta not exist.");
        return KVCLIENT_META_NOTEXIST;
    }
    uint32_t ttl=0;
    uint64_t length =0;
    kv_encode::DecodeHashMetaValue(str_meta_value,ttl ,length );
    
    if( HasOutTime(ttl) ){
        ERROR_LOG("hash del has ttl");
        return KVCLIENT_KEY_NOTEXIST;
    }
    INFO_LOG("SIsMember ok");
    return KVCLIENT_OK;
}
int Database::SPop(  const ::phxkv::SetRequest* request, const std::string & set_key, std::string &ret_key)
{
    INFO_LOG("SPop start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //取出长度，随机一个值
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    rocksdb::Status oStatus;
    std::string _meta_key = kv_encode::EncodeMetaKey( set_key,SET_META  );
    std::string _meta_value="";
    uint32_t ttl=0;
    uint64_t length =0;
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], _meta_key, &_meta_value);
    if( !_meta_value.empty() ){
        kv_encode::DecodeSetMetaValue(_meta_value,ttl ,length );
    }else{
        delete txn;
        ERROR_LOG("SPop meta not exist.");
        return KVCLIENT_META_NOTEXIST;
    } 
    if( HasOutTime(ttl) ){
        ERROR_LOG("HasOutTime.");
        return KVCLIENT_KEY_NOTEXIST;
    }
    srand( length );
    int k = rand() % length;

    std::string dbkey;
    std::string key_start = set_key;
    key_start = kv_encode::EncodeKey(key_start,SET_TYPE);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    it = m_rocksdb->NewIterator(iterate_options, vec_cf[1] );
    it->Seek(key_start);
    while (it->Valid() && k--> 0 ) {
        
       it->Next();
    }
    std::string rand_key= it->key().ToString();
    delete it;
    kv_encode::DecodeKey(rand_key, dbkey);
    //删除key
    if (dbkey == set_key) {//加个验证
        ret_key.assign(rand_key.data() ,rand_key.length() );
        txn->Delete(vec_cf[1] , rand_key );
    } else {
        INFO_LOG("length error key size=%d dbkey size=%d",set_key.size(),dbkey.size() );
    }
    //更新length
    length--;
    if( 0 == length ){
       txn->Delete(vec_cf[1] , _meta_key);
    }else{
         _meta_value = kv_encode::EncodeSetMetaValue(ttl ,length );
         txn->Put( vec_cf[1] , _meta_key, _meta_value);
    }
    oStatus = txn->Commit();
    if( !oStatus.ok() ){
        txn->Rollback();
        delete txn;
        ERROR_LOG("SPop Commit ok");
    }
    delete txn;
    INFO_LOG("SPop ok");
    return KVCLIENT_OK;
}

int Database::SRandMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string & set_key )
{
    INFO_LOG("HashGet start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    //取出长度，随机一个值
    uint64_t length =0;
    int ret = SCard(request ,length , set_key );
    if(0== length ){
        return KVCLIENT_META_NOTEXIST;
    }
    srand( length );
    int k = rand() % length;

    std::string dbkey;
    std::string key_start = set_key;
    key_start = kv_encode::EncodeKey(key_start,SET_TYPE);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    it = m_rocksdb->NewIterator(iterate_options, vec_cf[1] );
    it->Seek(key_start);
    while (it->Valid() && k--  > 0 ) {
        
       it->Next();
    }
    std::string rand_key= it->key().ToString();
    kv_encode::DecodeKey(rand_key, dbkey);
    if (dbkey == set_key) {//加个验证
        response->add_field()->assign(rand_key.data() ,rand_key.length() );
    } else {
        INFO_LOG("length error key size=%d dbkey size=%d",set_key.size(),dbkey.size() );
    }
    delete it;
    INFO_LOG("SRandMember ok");
    return KVCLIENT_OK;
}
//集合A移到集合B
int Database::SMove( const ::phxkv::SetRequest* request, const std::string & set_key )
{
    INFO_LOG("SMove start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    const std::string & field_key = request->field(0);
    const std::string & src_key = request->src_set(0);
    string value="";

    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    rocksdb::Status oStatus;
    //删除集合
    std::string _meta_key = kv_encode::EncodeMetaKey( src_key,SET_META );
    std::string _meta_value = "";
    oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], _meta_key, &_meta_value);
    if( _meta_value.empty() ){
        ERROR_LOG("ket not exist.");
        return KVCLIENT_KEY_NOTEXIST;
    }
    std::string _field = kv_encode::EncodeSetFieldKey(src_key ,field_key );
    txn->Delete(vec_cf[1] , _field ); 

    uint32_t ttl=0;uint64_t length = 0;
    kv_encode::DecodeSetMetaValue( _meta_value,ttl ,length );
    length-=1;
    _meta_value = kv_encode::EncodeSetMetaValue( ttl ,length );
    if(0 == length ){
        txn->Delete(vec_cf[1] , _meta_key );
    }else{
        txn->Put( vec_cf[1] ,_meta_key, _meta_value);
    }
    
    //增加集合B
    _field = kv_encode::EncodeSetFieldKey(set_key ,field_key );
    
    std::string field_value="";
    oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], _field, &field_value);
    if( oStatus.IsNotFound()){

        txn->Put(vec_cf[1] , _field,"" ); //新增进去
        _meta_key = kv_encode::EncodeMetaKey( set_key,SET_META );
        _meta_value = "";
        oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], _meta_key, &_meta_value);
        ttl=0;
        length=0;
        if( !_meta_value.empty() ){
            kv_encode::DecodeSetMetaValue( _meta_value,ttl ,length );
        }
        length+=1;
        _meta_value = kv_encode::EncodeSetMetaValue( ttl ,length );
        txn->Put( vec_cf[1] ,_meta_key, _meta_value);
    }
    oStatus =  txn->Commit();
    if( !oStatus.ok() ){
        txn->Rollback();
        delete txn;
        ERROR_LOG("SMove error");
         return KVCLIENT_ROCKSDB_ERR;
    }
    INFO_LOG("SMove ok");
    return KVCLIENT_OK;
}


//=============zset=============================
int Database::ZAdd( const ::phxkv::ZsetRequest* request,const std::string & zset_key)
{
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    int set_size = 0;
    rocksdb::Status oStatus;
    for(int k=0;k< request->field_key_size();k++ )
    {
        string field_key = request->field_key(k).field_key();
        string field_value="";
        oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value );
         if( oStatus.IsNotFound() ){
             set_size++;
             field_value = request->field_key(k).field_value();
             txn->Put(vec_cf[1] ,field_key, field_value );

             field_key = request->field_score(k).field_key();//存一下score到key上
             field_value = request->field_score(k).field_value();
             txn->Put(vec_cf[1] ,field_key, field_value );
         }
    }
    std::string _meta_key = kv_encode::EncodeMetaKey( zset_key,ZSET_META  );
    std::string _meta_value="";
    
    uint32_t ttl=0;uint64_t length=0;
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], _meta_key, &_meta_value);
    if( !_meta_value.empty() ){
        kv_encode::DecodeSetMetaValue(_meta_value,ttl ,length );
        set_size += length ;
    } 
    //存在
    _meta_value = kv_encode::EncodeSetMetaValue(ttl, set_size );
    txn->Put( vec_cf[1], _meta_key, _meta_value);

    oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ZADD KVCLIENT_ROCKSDB_ERR ");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ZADD OK ");
    return KVCLIENT_OK;
}
int Database::ZCard( const ::phxkv::ZsetRequest* request,uint64_t & length ,const std::string & zset_key)//元素数量
{
    INFO_LOG("ZCard start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    
    std::string _meta_key = kv_encode::EncodeMetaKey( zset_key,ZSET_META  );
    std::string _meta_value="";
    uint32_t ttl=0;
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], _meta_key, &_meta_value);
    if( !_meta_value.empty() ){
        kv_encode::DecodeSetMetaValue(_meta_value,ttl ,length );
    } 
    INFO_LOG("ZCard ok");
    return KVCLIENT_OK;
}
int Database::ZCount( const ::phxkv::ZsetRequest* request, uint64_t& length,const std::string & zset_key)//有序集合中，在区间内的数量
{
    //返回有序集 key 中， score 值在 min 和 max 之间(默认包括 score 值等于 min 或 max )的成员的数量。
    INFO_LOG("ZCount start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    std::string meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0; 
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    if( HasOutTime( ttl ) ){
        ERROR_LOG("ZCount HasOutTime ");
        return KVCLIENT_META_NOTEXIST;
    }
    int64_t min_score= request->min() ;
    int64_t max_score = request->max() ;
    if(min_score <= ZSET_SCORE_MIN ){
        min_score = ZSET_SCORE_MIN;
    }
    if(max_score >= ZSET_SCORE_MAX){
        max_score = ZSET_SCORE_MAX;
    }
    
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE);

    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
    length = 0;
    while ( it->Valid()   ) {
        INFO_LOG("ZCount start1");
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }
        std::string field_key="";
        __int64_t score_out=0;
        kv_encode::DecodeZSetScoreKey(it->key().ToString() ,field_key , score_out);
        INFO_LOG("ZCount start12=%d" ,score_out );
        
        if(score_out >= min_score && score_out <= max_score ){
            length++;
        }
        it->Next();
    }
    delete it;
    INFO_LOG("ZCount ok length=%ld" , length );
    return KVCLIENT_OK;
}
int Database::ZRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)//有序集合区间内的成员
{
    INFO_LOG("ZRange start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    std::string meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;uint64_t length =0;
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    if( HasOutTime( ttl ) ){
        INFO_LOG("ZRange HasOutTime");
        return KVCLIENT_META_NOTEXIST;
    }
    int64_t start_=request->start_pos();
    int64_t end_ = request->end_pos();

    start_ =  start_>=0 ? start_:(length+start_);
    end_ = end_>=0 ? end_ :(length + end_ );
    if(start_ <0){
       start_ = 0;
    }
    if(end_ > length-1){
       end_ = length-1;
    }
    INFO_LOG("param start=%lu end=%lu" , start_ , end_ );
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE);//从小往大找,优化
    
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
     int pos=0;
     while ( it->Valid() && pos++ < start_)
     {
         it->Next();
     }
     
    while ( it->Valid() && start_<= end_  ) {
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }
        __int64_t score_out=0;
        std::string field_key="";
        kv_encode::DecodeZSetScoreKey(it->key().ToString() ,field_key , score_out);
       
        if(score_out >= ZSET_SCORE_MIN && score_out <= ZSET_SCORE_MAX ){
            
            phxkv::ZsetField * field = response->add_field();
            field->set_field_key( field_key.c_str() );
            field->set_field_value( kv_encode::EncodeZSetFieldValue(score_out) );
            it->Next();
             start_++;
        }else{
            break;
        }
    }
    delete it;
    INFO_LOG("ZRange ok");
    return KVCLIENT_OK;
}
int Database::ZIncrby( const ::phxkv::ZsetRequest* request,  const std::string & zset_key,std::string & ret_key)//为score增加或减少
{
    INFO_LOG("ZIncrby start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if( 0 == request->field_key_size() ){
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
    std::string meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;uint64_t length =0;
    if( !meta_value.empty() ){

        kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    }

    std::string field_key =request->field_key(0).field_key();
    std::string field_value ="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value );

    if( !field_value.empty() ){ 

        txn->Delete(vec_cf[1], field_key );//旧的 key 删除

        int64_t score=0;
        kv_encode::DecodeZSetFieldValue(field_value , score);
        field_key = kv_encode::EncodeZSetScoreKey(zset_key , score );
        
        txn->Delete(vec_cf[1], field_key );//旧的score  删除
    }else{
        //更新长度
        length += 1;
    }//key部分
    __int64_t score=0;
    score+=request->incrscore();
    field_key = request->field_key(0).field_key();
    ret_key = kv_encode::EncodeZSetFieldValue( score );
    txn->Put(vec_cf[1], field_key,ret_key );//新添加的key

    //
    field_key = kv_encode::EncodeZSetScoreKey(zset_key,score );
    kv_encode::DecodeZSetFieldKey( request->field_key(0).field_key(),field_value );
    txn->Put(vec_cf[1], field_key,field_value);//score部分

    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ZUnionStore KVCLIENT_ROCKSDB_ERR ");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ZIncrby start");
    return KVCLIENT_OK;
}
int Database::ZUnionStore( const ::phxkv::ZsetRequest* request,const std::string & zset_key)//并集不输出
{
    INFO_LOG("ZUnionStore start");
    //并集，score累加
    std::map<std::string , ZsetStruct * > resust;
    for(int k=0;k<request->src_set_size();k++ ){
        std::string t_key = request->src_set( k );
        MemBers( t_key, resust);
    }
    if( 0 == resust.size() ){

        return KVCLIENT_OK;
    }
    
    //清空旧key
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
     
    txn->Delete(vec_cf[1] ,meta_key );
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE);//从小往大找,优化
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
     
    while ( it->Valid()  ) {
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }
        std::string field_key = it->key().ToString();
        __int64_t score_out=0;
        std::string _key="";
        kv_encode::DecodeZSetScoreKey(field_key , _key ,score_out);
       
        if(score_out >= ZSET_SCORE_MIN && score_out <= ZSET_SCORE_MAX ){
            
             txn->Delete(vec_cf[1] ,field_key );//删除对应的key
            //删除对应的 score 
             field_key = kv_encode::EncodeZSetScoreKey( zset_key ,score_out );
             txn->Delete(vec_cf[1] ,field_key );
        }
        it->Next();
    }
    //插入
    std::map<std::string , ZsetStruct * >::iterator iter= resust.begin();
    for( ;iter!=resust.end() ;iter++ ){

       std::string field_key = kv_encode::EncodeZSetFieldKey(zset_key , iter->first );
       std::string field_value = kv_encode::EncodeZSetFieldValue(iter->second->n_score );
       txn->Put(vec_cf[1] ,field_key ,field_value );

       field_key = kv_encode::EncodeZSetScoreKey(zset_key , iter->second->n_score );
       field_value = iter->first;
       txn->Put(vec_cf[1] ,field_key ,field_value);
       delete iter->second;
    
    }
    std::string meta_value = kv_encode::EncodeZSetMetaValue( 0 , resust.size() );
    txn->Put(vec_cf[1] ,meta_key ,meta_value );
    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ZUnionStore KVCLIENT_ROCKSDB_ERR ");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ZUnionStore ok ");
    return KVCLIENT_OK;
}
int Database::MemBers(const std::string & zse_key, std::map<std::string , ZsetStruct * > &resust )
{
    INFO_LOG("Members start...");
    string str_meta_key =kv_encode::EncodeMetaKey( zse_key ,ZSET_META);
    string str_meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], str_meta_key, &str_meta_value);
    if( str_meta_value.empty() ){
        ERROR_LOG("meta not exist.");
        return KVCLIENT_META_NOTEXIST;
    }
    uint32_t ttl=0;
    uint64_t length =0;
    kv_encode::DecodeZSetMetaValue(str_meta_value,ttl ,length );
    
    if( HasOutTime(ttl) ){
        ERROR_LOG(" has ttl");
        return KVCLIENT_KEY_NOTEXIST;
    }

    std::string dbkey;
    std::string key_start = zse_key;
    key_start = kv_encode::EncodeKey(key_start,ZSET_TYPE);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    it = m_rocksdb->NewIterator(iterate_options, vec_cf[1] );
    it->Seek(key_start);
    while (it->Valid()) {
        string key = it->key().ToString();
        if ( (it->key())[0] != ZSET_TYPE) {  //类型相等
            break;
       }
       string field_key = "";
       kv_encode::DecodeZSetFieldKey(key ,field_key);
        //加个验证
        int64_t int64_score=0;
        kv_encode::DecodeZSetFieldValue(it->value().ToString() ,int64_score );

        if( resust.count( field_key ) == 0 ){
            ZsetStruct * field = new ZsetStruct();
            field->str_member=field_key ;
            //解码score
            field->data_size = 1;
            field->n_score = int64_score ;
            resust.insert( make_pair(field_key , field ) );//解码，直接返回
        }else{
            resust[field_key]->n_score+= int64_score;//解码，直接返回
            resust[field_key]->data_size++;
        }
       it->Next();
    }
    delete it;
    INFO_LOG("Members ok");
    return KVCLIENT_OK;
}
int Database::ZInterStore( const ::phxkv::ZsetRequest* request,const std::string & zset_key)//交集不输出
{
    INFO_LOG("ZInterStore start");
    //并集，score累加
    std::map<std::string , ZsetStruct * > resust;
    
    
    for(int k=0;k<request->src_set_size();k++ ){
        std::string t_key = request->src_set( k );
        MemBers( t_key, resust);
    }
    if( 0 == resust.size() ){

        return KVCLIENT_OK;
    }
    
    //清空旧key
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
     
    txn->Delete(vec_cf[1] ,meta_key );
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE);//从小往大找,优化
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
     
    while ( it->Valid()  ) {
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }
        std::string field_key = it->key().ToString();
        __int64_t score_out=0;
        std::string _key = "";
        kv_encode::DecodeZSetScoreKey(field_key ,_key, score_out);
       
        if(score_out >= ZSET_SCORE_MIN && score_out <= ZSET_SCORE_MAX ){
            
             txn->Delete(vec_cf[1] ,field_key );//删除对应的key
            //删除对应的 score 
             field_key = kv_encode::EncodeZSetScoreKey( zset_key ,score_out );
             txn->Delete(vec_cf[1] ,field_key );
        }
        it->Next();
    }
    //插入
    int64_t size = 0;
    std::map<std::string , ZsetStruct * >::iterator iter= resust.begin();
    for( ;iter!=resust.end() ;iter++ ){
        if( iter->second->data_size != request->src_set_size() ){
            continue;
        }
       std::string field_key = kv_encode::EncodeZSetFieldKey(zset_key , iter->first );
       std::string field_value = kv_encode::EncodeZSetFieldValue(iter->second->n_score );
       txn->Put(vec_cf[1] ,field_key ,field_value );

       field_key = kv_encode::EncodeZSetScoreKey(zset_key , iter->second->n_score );
       field_value = iter->first;
       txn->Put(vec_cf[1] ,field_key ,field_value);
       delete iter->second;
       size++;
    }
    std::string meta_value = kv_encode::EncodeZSetMetaValue( 0 , size );
    txn->Put(vec_cf[1] ,meta_key ,meta_value );
    rocksdb::Status oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        ERROR_LOG("ZInterStore KVCLIENT_ROCKSDB_ERR ");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ZInterStore ok ");
    return KVCLIENT_OK;
}
int Database::ZRangebyscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)
{
    //返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。有序集成员按 score 值递增(从小到大)次序排列。
    // 可选的 LIMIT 参数指定返回结果的数量及区间
    INFO_LOG("ZRangebyscore start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    std::string meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0; uint64_t length=0;
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    if( HasOutTime( ttl ) ){
        ERROR_LOG("ZRangebyscore HasOutTime ");
        return KVCLIENT_META_NOTEXIST;
    }
    int64_t min_score=request->min() ;
    int64_t max_score = request->max() ;
    if(min_score <= ZSET_SCORE_MIN ){
        min_score = ZSET_SCORE_MIN;
    }
    if(max_score >= ZSET_SCORE_MAX){
        max_score = ZSET_SCORE_MAX;
    }
    int64_t offset =request->offset();//偏移量
    if(offset < 0 ){
        offset = 0;
    }
    int64_t count = request->count();
    if( count < 0 ){
        count = length;//读取所有
    }
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE);

    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
    length = 0;
    uint64_t temp_offset=0;
    uint64_t temp_count=0;
    while ( it->Valid() &&  temp_count< count  ) {
        INFO_LOG("ZRangebyscore start1");
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }
        std::string field_key="";
        __int64_t score_out=0;
        kv_encode::DecodeZSetScoreKey(it->key().ToString() , field_key ,score_out);
        INFO_LOG("ZCount start12=%d" ,score_out );
        if(score_out >= min_score && score_out <= max_score ){
            //根据score获取的field
            if( temp_offset >= offset ){

                phxkv::ZsetField * field = response->add_field();
                field->set_field_key( field_key );
                field->set_field_value( kv_encode::EncodeZSetFieldValue( score_out ) );

                temp_count++;
            }else{  
                temp_offset++;
            }
        }else{
            it->Next();
        }
    }
    delete it;
    INFO_LOG("ZRangebyscore ok length=%d" , length );
    return KVCLIENT_OK;
}
int Database::ZRem( const ::phxkv::ZsetRequest* request ,const std::string & zset_key)//移除有序集 key 中的一个或多个成员
{
    INFO_LOG("ZRem  start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if( 0 == request->field_key_size() ){
        ERROR_LOG("ZRem KVCLIENT_PARAM_ERROR");
        return KVCLIENT_SYS_FAIL;
    }
    rocksdb::WriteOptions write_options;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);//开启事务
    int set_size = 0;//更新后的长度
    std::set<string> set_;//防止重复
    rocksdb::Status oStatus;
    for(int k=0;k< request->field_key_size();k++ ){
        string field_key = request->field_key(k).field_key();
        string field_value="";
        oStatus = txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value );
         if( oStatus.ok() && set_.count( field_key )<=0 ){
             set_size++;
             txn->Delete(vec_cf[1] ,field_key );//删除对应的key

            //此处需要解码成int64
             int64_t int64_score = 0;
             kv_encode::DecodeZSetFieldValue( field_value ,int64_score );
            //删除对应的 score 
             field_key = kv_encode::EncodeZSetScoreKey( zset_key ,int64_score );
             txn->Delete(vec_cf[1] ,field_key );
         }
    }
    std::string _meta_key = kv_encode::EncodeMetaKey( zset_key,ZSET_META  );
    std::string _meta_value="";
    
    uint32_t ttl=0;uint64_t length=0;
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], _meta_key, &_meta_value);
    if( !_meta_value.empty() ){
        kv_encode::DecodeSetMetaValue(_meta_value,ttl ,length );
        set_size = length - set_size ;
    } 
    //更新长度
    //存在
    _meta_value = kv_encode::EncodeZSetMetaValue(ttl, set_size );
    txn->Put( vec_cf[1], _meta_key, _meta_value);

    oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        INFO_LOG("ZRem  KVCLIENT_ROCKSDB_ERR");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ZRem  ok");
    return KVCLIENT_OK;
}
int Database::ZRank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key)//返回member的排名，
{
    INFO_LOG("ZRank start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if( 0 == request->field_key_size() ){
        INFO_LOG("ZRank start11");
        return KVCLIENT_PARAM_ERROR;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    std::string meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;uint64_t length =0;
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    if( HasOutTime( ttl ) ){
        ERROR_LOG("ZRank HasOutTime");
        return KVCLIENT_META_NOTEXIST;
    }
    
    std::string member = request->field_key(0).field_key();
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE );
    
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
    
    while ( it->Valid()   ) {
    
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
           INFO_LOG("ZRank start23=%d", (it->key())[0] );
            break;
       }
        __int64_t score = 0;
        std::string field_key="";
        kv_encode::DecodeZSetScoreKey(it->key().ToString(), field_key ,score  );
        
        if(score >= ZSET_SCORE_MIN && score <= ZSET_SCORE_MAX ){
            rank_member++;
            if( member ==field_key ){
                break;
            }
            it->Next();
        }else{
            break;
        }
    }
    delete it;
    INFO_LOG("ZRank ok");
    return KVCLIENT_OK;
}
int Database::ZRevrank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key)//member逆序排名
{
    INFO_LOG("ZRevrank start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    if( 0 == request->field_key_size() ){
        INFO_LOG("ZRank start11");
        return KVCLIENT_PARAM_ERROR;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    std::string meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;uint64_t length =0;
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    if( HasOutTime( ttl ) ){
        ERROR_LOG("ZRank HasOutTime");
        return KVCLIENT_META_NOTEXIST;
    }
    
    std::string member = request->field_key(0).field_key();
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE );
    
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
    
    while ( it->Valid()   ) {
    
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
           INFO_LOG("ZRank start23=%d", (it->key())[0] );
            break;
       }
        __int64_t score = 0;
        std::string field_key="";
        kv_encode::DecodeZSetScoreKey(it->key().ToString(),field_key, score  );
        
        if(score >= ZSET_SCORE_MIN && score <= ZSET_SCORE_MAX ){
            rank_member++;
            if( member ==field_key ){
                break;
            }
            it->Next();
        }else{
            break;
        }
    }
    delete it;
    rank_member = length - rank_member + 1 ;
    INFO_LOG("ZRevrank ok");
    return KVCLIENT_OK;
}
int Database::ZScore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)//member的score
{
    INFO_LOG("ZScore start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    std::string meta_value="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;uint64_t length =0;
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    if( HasOutTime( ttl ) ){
        ERROR_LOG("ZScore KVCLIENT_META_NOTEXIST");
        return KVCLIENT_META_NOTEXIST;
    }
    std::string field_key= request->field_key(0).field_key();//参数需要拼接
    std::string field_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], field_key, &field_value );
    if( field_value.empty() ){
        ERROR_LOG("ZScore KVCLIENT_KEY_NOTEXIST");
        return KVCLIENT_KEY_NOTEXIST;
    }
    __int64_t score_out=0;
    kv_encode::DecodeZSetFieldValue(field_value , score_out);
    response->set_mem_score( score_out );
     
    return KVCLIENT_OK;
}
int Database::ZREVRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)//指定区间成员，逆序输出
{
    INFO_LOG("ZREVRange start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    std::string meta_value ="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;uint64_t length =0;
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    if( HasOutTime( ttl ) ){
        INFO_LOG("ZREVRange HasOutTime");
        return KVCLIENT_META_NOTEXIST;
    }
    int64_t start_=request->start_pos();
    int64_t end_ = request->end_pos();

    start_ =  start_>=0 ? start_:(length+start_);
    end_ = end_>=0 ? end_ :(length + end_ );
    if(start_ <0){
       start_ = 0;
    }
    if(end_ > length-1){
       end_ = length-1;
    }
    int64_t start = start_;
    int64_t end= end_;
    start_ = length - end -1;
    end_ = length - start -1 ;
    INFO_LOG("ZREVRange param start=%lu end=%lu" , start_ , end_ );
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE);//从小往大找,优化
    
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
     int pos=0;
     while ( it->Valid() && pos++ < start_)
     {
         it->Next();
     }
    std::vector<std::string> vec_key;
    std::vector<__int64_t> vec_score;
    while ( it->Valid() && start_<= end_  ) {
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }
        __int64_t score_out=0;
        std::string field_key="";
        
        kv_encode::DecodeZSetScoreKey(it->key().ToString() ,field_key ,  score_out);
       
        if(score_out >= ZSET_SCORE_MIN && score_out <= ZSET_SCORE_MAX ){
            vec_key.push_back( field_key );
            vec_score.push_back( score_out);
            it->Next();
             start_++;
        }else{
            break;
        }
    }
    for(int i=vec_key.size()-1;i>=0 ;i-- ){
        phxkv::ZsetField * field = response->add_field();
        field->set_field_key( vec_key[i] );
        field->set_field_value( kv_encode::EncodeZSetFieldValue(vec_score[i] ) );
    }
    delete it;
    INFO_LOG("ZREVRange ok");
    return KVCLIENT_OK;
}
int Database::ZREVRangebylscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)
{
    //返回有序集 key 中， score 值介于 max 和 min 之间(默认包括等于 max 或 min )的所有的成员。有序集成员按 score 值递减(从大到小)的次序排列
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    std::string meta_value="";
    m_rocksdb->Get(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;uint64_t length =0;
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    if( HasOutTime( ttl ) ){
        return KVCLIENT_META_NOTEXIST;
    }
    int64_t min_score=request->min();
    int64_t max_score = request->max();
    if(min_score <= ZSET_SCORE_MIN ){
        min_score = ZSET_SCORE_MIN;
    }
    if(max_score >= ZSET_SCORE_MAX){
        max_score = ZSET_SCORE_MAX;
    }
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE);
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    std::vector<std::string> vec_key;
    std::vector<__int64_t> vec_score;

    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options , vec_cf[1]);
    it->Seek(key_start);
     
    while ( it->Valid()   ) {
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }
        __int64_t score_out=0;
         std::string field_key="";
        kv_encode::DecodeZSetScoreKey(it->key().ToString() , field_key ,score_out);
       
        if(score_out >= min_score && score_out <= max_score ){
            vec_key.push_back(field_key);
            vec_score.push_back(score_out);
        }
        it->Next();   
    }
    for(int i=vec_key.size()-1;i>=0 ;i-- ){
        phxkv::ZsetField * field = response->add_field();
        field->set_field_key( vec_key[i] );
        field->set_field_value( kv_encode::EncodeZSetFieldValue(vec_score[i] ) );
    }
    
    delete it;
    return KVCLIENT_OK;
}
int Database::ZRemrangebyrank( const ::phxkv::ZsetRequest* request, const std::string & zset_key)
{
    //移除有序集 key 中，指定排名(rank)区间内的所有成员。从0开始
    INFO_LOG("ZRemrangebyrank start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    rocksdb::WriteOptions write_options;
    rocksdb::Status oStatus;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
    std::string meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;uint64_t length =0;
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    
    int64_t start_=request->start_pos();
    int64_t end_ = request->end_pos();

    start_ =  start_>=0 ? start_:(length+start_);
    end_ = end_>=0 ? end_ :(length + end_ );
    if(start_ <0){
       start_ = 0;
    }
    if(end_ > length-1){
       end_ = length-1;
    }
   const int64_t start = start_;
   const int64_t end = end_;
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE);//从小往大找,优化
    
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
     int64_t pos=0;
     while ( it->Valid() && pos++ < start_ )
     {
         if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }
         it->Next();//找到起点位置
     }
     
    while ( it->Valid() && start_<= end_  ) {
        //解码得分
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }

        int64_t db_score =0;
        std::string field_key = it->key().ToString();
        std::string _key ="";
        kv_encode::DecodeZSetScoreKey( field_key , _key ,db_score );//取出score
        INFO_LOG("db_score=%ld", db_score );
        start_++;
        if(db_score >= ZSET_SCORE_MIN && db_score <= ZSET_SCORE_MAX ){
            
            txn->Delete(vec_cf[1],field_key );//删除score
            std::string field_value = it->value().ToString();//通过score结构的value部分，拼接，
             
            field_key = kv_encode::EncodeZSetFieldKey( zset_key ,field_value );
            txn->Delete(vec_cf[1],field_key );//删除key
        }
        it->Next();
    }
    
    //更新长度
    //存在
    meta_value = kv_encode::EncodeZSetMetaValue(ttl, length - (end - start) -1 );
    txn->Put( vec_cf[1], meta_key, meta_value);
    delete it;
    oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        INFO_LOG("ZRem  KVCLIENT_ROCKSDB_ERR");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ZRemrangebyrank ok");
    return KVCLIENT_OK;
}
int Database::ZRemrangebyscore( const ::phxkv::ZsetRequest* request, const std::string & zset_key)
{
    ////移除有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员
    INFO_LOG("ZRemrangebyscore start");
    if (!m_bHasInit)
    {
        ERROR_LOG("no init yet");
        return KVCLIENT_SYS_FAIL;
    }
    std::string meta_key = kv_encode::EncodeMetaKey(zset_key , ZSET_META);
    rocksdb::WriteOptions write_options;
    rocksdb::Status oStatus;
    rocksdb::Transaction* txn = m_rocksdb->BeginTransaction(write_options);
    std::string meta_value="";
    txn->GetForUpdate(rocksdb::ReadOptions(),vec_cf[1], meta_key, &meta_value);
    uint32_t ttl=0;uint64_t length =0;
    kv_encode::DecodeZSetMetaValue(meta_value , ttl ,length );
    
    int64_t start_=request->min();
    int64_t end_ = request->max();

    if( start_<= ZSET_SCORE_MIN ){
        start_= ZSET_SCORE_MIN;
    }
    if( end_ >= ZSET_SCORE_MAX ){
        end_ = ZSET_SCORE_MAX;
    }
    std::string key_start = kv_encode::EncodeKey(zset_key , ZSET_SCORE);//从小往大找,优化
    
    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;
    rocksdb::Iterator *it = m_rocksdb->NewIterator(read_options, vec_cf[1]);
    it->Seek(key_start);
    int64_t size = 0;
    while ( it->Valid()  ) {
        if ( (it->key())[0] != ZSET_SCORE) {  //类型相等
            break;
        }

        int64_t db_score =0;
        std::string _key ="";
        std::string field_key = it->key().ToString();
        kv_encode::DecodeZSetScoreKey( field_key , _key , db_score );//取出score
        INFO_LOG("db_score=%ld", db_score );
        
        if(db_score >= start_ && db_score <= end_  ){
            
            txn->Delete(vec_cf[1],field_key );//删除score
            std::string field_value = it->value().ToString();//通过score结构的value部分，拼接，
             
            field_key = kv_encode::EncodeZSetFieldKey( zset_key ,field_value );
            txn->Delete(vec_cf[1],field_key );//删除key
            size++;
        }
        it->Next();
    }
    
    //更新长度
    //存在
    INFO_LOG("length==%ld===%ld",length,size);
    meta_value = kv_encode::EncodeZSetMetaValue(ttl, length - size );
    txn->Put( vec_cf[1], meta_key, meta_value);
    delete it;
    oStatus = txn->Commit();
    if( !oStatus.ok()){
        txn->Rollback();
        delete txn;
        INFO_LOG("ZRem  KVCLIENT_ROCKSDB_ERR");
        return KVCLIENT_ROCKSDB_ERR;
    }
    delete txn;
    INFO_LOG("ZRemrangebyscore ok");
    return KVCLIENT_OK;
}
////////////////////////////////////////////////////

MultiDatabase :: MultiDatabase()
{
}

MultiDatabase :: ~MultiDatabase()
{
    for (auto & poDB : m_vecDBList)
    {
        delete poDB;
    }
}

int MultiDatabase :: Init(const std::string & sDBPath, const int iGroupCount)
{
      
    if (access(sDBPath.c_str(), F_OK) == -1)
    {
        ERROR_LOG("DBPath not exist or no limit to open, %s", sDBPath.c_str());
        return -1;
    }

    if (iGroupCount < 1 || iGroupCount > 100000)
    {
        ERROR_LOG("Groupcount wrong %d", iGroupCount);
        return -2;
    }

    std::string sNewDBPath = sDBPath;

    if (sDBPath[sDBPath.size() - 1] != '/')
    {
        sNewDBPath += '/';
    }
    
    
    char sGroupDBPath[512] = {0};
    const int GroupId = 0;
    snprintf(sGroupDBPath, sizeof(sGroupDBPath), "%sg%d", sNewDBPath.c_str(), GroupId);

    Database * poDB = new Database();
    assert(poDB != nullptr);
    m_vecDBList.push_back(poDB); 

    if (poDB->Init(sGroupDBPath, iGroupCount ) != 0)
    {
        return -1;
    }
     
    INFO_LOG("OK, DBPath %s groupcount %d", sDBPath.c_str(), iGroupCount);

    return 0;
}

const std::string MultiDatabase :: GetLogStorageDirPath(const int iGroupIdx)
{
    return m_vecDBList[0]->GetDBPath();
}

int MultiDatabase :: Get(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue)
{
    return m_vecDBList[0]->Get(iGroupIdx,llInstanceID, sValue);
}

int MultiDatabase :: Put(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue)
{
    
    return m_vecDBList[0]->Put(oWriteOptions,iGroupIdx, llInstanceID, sValue);
}

int MultiDatabase :: Del(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID)
{
    return m_vecDBList[0]->Del(iGroupIdx,oWriteOptions, llInstanceID);
}

int MultiDatabase :: ForceDel(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID)
{
    return m_vecDBList[0]->ForceDel(iGroupIdx,oWriteOptions, llInstanceID);
}

int MultiDatabase :: GetMaxInstanceID(const int iGroupIdx, uint64_t & llInstanceID)
{
    return m_vecDBList[0]->GetMaxInstanceID(iGroupIdx,llInstanceID);
}
    
int MultiDatabase :: SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llMinInstanceID)
{

    return m_vecDBList[0]->SetMinChosenInstanceID(iGroupIdx,oWriteOptions, llMinInstanceID);
}

int MultiDatabase :: GetMinChosenInstanceID(const int iGroupIdx, uint64_t & llMinInstanceID)
{
    return m_vecDBList[0]->GetMinChosenInstanceID(iGroupIdx , llMinInstanceID);
}

int MultiDatabase :: ClearAllLog(const int iGroupCnt)
{
    return m_vecDBList[0]->ClearAllLog( iGroupCnt );
}

int MultiDatabase :: SetSystemVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer)
{
    return m_vecDBList[0]->SetSystemVariables(oWriteOptions,iGroupIdx, sBuffer);
}

int MultiDatabase :: GetSystemVariables(const int iGroupIdx, std::string & sBuffer)
{

    return m_vecDBList[0]->GetSystemVariables(iGroupIdx,sBuffer);
}

int MultiDatabase :: SetMasterVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer)
{
    return m_vecDBList[0]->SetMasterVariables(oWriteOptions, iGroupIdx,sBuffer);
}

int MultiDatabase :: GetMasterVariables(const int iGroupIdx, std::string & sBuffer)
{

    return m_vecDBList[0]->GetMasterVariables(iGroupIdx , sBuffer);
}

 bool MultiDatabase :: CreteCheckPoint(std::string &backpath)//备份到这个路径下
 {

     return m_vecDBList[0]->CreatCheckPoint( backpath );
 }

//新增
int MultiDatabase :: KvGet(const std::string & sKey, std::string & sValue )
{
    return m_vecDBList[0]->KvGet(   sKey, sValue);
}
 

int MultiDatabase :: KvSet(const std::string & sKey, const std::string & sValue )
{
    return m_vecDBList[0]->KvSet(   sKey, sValue);
}
int MultiDatabase :: KvDel(const std::string & sKey )
{
    return m_vecDBList[0]->KvDel(   sKey );;
}

int MultiDatabase :: KvGetCheckpointInstanceID(const uint16_t iGroupIdx,uint64_t & llCheckpointInstanceID)
{
    return m_vecDBList[0]->KvGetCheckpointInstanceID( iGroupIdx,  llCheckpointInstanceID );
}
int MultiDatabase :: KvSetCheckpointInstanceID(const uint16_t iGroupIdx,const uint64_t llCheckpointInstanceID)
{
    return m_vecDBList[0]->KvSetCheckpointInstanceID(  iGroupIdx, llCheckpointInstanceID );
}

void MultiDatabase :: DumpRocksDBStats()
{
    return m_vecDBList[0]->DumpRocksDBStats(  );
}

bool MultiDatabase :: KvBatchGet(const ::phxkv::KvBatchGetRequest* request, ::phxkv::KvBatchGetResponse* response)
{
    return m_vecDBList[0]->KvBatchGet(request, response );
}

bool MultiDatabase :: KvBatchSet(const phxkv::KvBatchPutRequest request)
{
    return m_vecDBList[0]->KvBatchSet(request );
}


int MultiDatabase :: HashDel(const ::phxkv::HashRequest* request,std::string hash_key)
{
    return m_vecDBList[0]->HashDel(request,hash_key );
}
int MultiDatabase :: HashSet(const ::phxkv::HashRequest* request,std::string hash_key )
{
    return m_vecDBList[0]->HashSet(request,hash_key );
}
int MultiDatabase ::HashGet(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response ,string hash_key)
{
    return m_vecDBList[0]->HashGet(request , response ,hash_key);
}
int MultiDatabase ::HashGetAll(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response,std::string hash_key )
{
    return m_vecDBList[0]->HashGetAll(request , response ,hash_key);
}
int MultiDatabase ::HashExist(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key )
{
    return m_vecDBList[0]->HashExist(request , response,hash_key);
}
int MultiDatabase ::HashIncrByInt(const ::phxkv::HashRequest* request,std::string hash_key,std::string & ret_value)
{
    return m_vecDBList[0]->HashIncrByInt(request ,hash_key,ret_value);
}
int MultiDatabase ::HashIncrByFloat(const ::phxkv::HashRequest* request,std::string hash_key,std::string & ret_value)
{
    return m_vecDBList[0]->HashIncrByFloat(request ,hash_key,ret_value);
}
int MultiDatabase ::HashKeys( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key)
{
    return m_vecDBList[0]->HashKeys(request , response,hash_key);
} 
int MultiDatabase ::HashLen(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response ,std::string hash_key,uint64_t &length )
{
    return m_vecDBList[0]->HashLen(request , response,hash_key,length);
}
int MultiDatabase ::HashMget(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key )
{
    return m_vecDBList[0]->HashMget(request , response,hash_key);
}
int MultiDatabase ::HashMset(const ::phxkv::HashRequest* request ,std::string hash_key )
{
    return m_vecDBList[0]->HashMset( request ,hash_key);
}
int MultiDatabase ::HashSetNx(const ::phxkv::HashRequest* request ,std::string hash_key )
{
    return m_vecDBList[0]->HashSetNx(request,hash_key);
}
int MultiDatabase ::HashValues( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key)
{
    return m_vecDBList[0]->HashValues(request , response,hash_key);
}

int MultiDatabase::DelKey(const string& key)
{
    return m_vecDBList[0]->DelKey(  key);
}
int MultiDatabase::ExpireKey( const string& key,const int ttl)
{
    return m_vecDBList[0]->ExpireKey( key,ttl);
}
//=============list=================
int MultiDatabase::ListLpush(const ::phxkv::ListRequest* request,std::string list_key)
{
    return m_vecDBList[0]->ListLpush(request ,list_key);
}
int MultiDatabase::ListLpushx(const ::phxkv::ListRequest* request,std::string list_key)
{
    return m_vecDBList[0]->ListLpushx(request ,list_key);
}
int MultiDatabase::ListLpop(const ::phxkv::ListRequest* request,std::string list_key,std::string& value)
{
    return m_vecDBList[0]->ListLpop(request ,list_key ,value );
}
int MultiDatabase::ListLength(const ::phxkv::ListRequest* request,::phxkv::ListResponse* response,std::string list_key,uint64_t& length)
{
    return m_vecDBList[0]->ListLength(request , response,list_key ,length );
}
int MultiDatabase::ListRpop(  const ::phxkv::ListRequest* request, std::string list_key,std::string& value )//弹出表尾
{
    return m_vecDBList[0]->ListRpop(request ,list_key,value);
}
int MultiDatabase::ListRpopLpush(  const ::phxkv::ListRequest* request,std::string list_key ,std::string &ret_value)//尾部进行头插
{
    return m_vecDBList[0]->ListRpopLpush(request ,list_key,ret_value);
}
int MultiDatabase::ListIndex(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key)//插入到指定位置
{
    return m_vecDBList[0]->ListIndex(request , response,list_key);
}
int MultiDatabase::ListInsert(  const ::phxkv::ListRequest* request, std::string list_key )//插入到指定位置
{
    return m_vecDBList[0]->ListInsert(request ,list_key);
}
int MultiDatabase::ListRange(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key)
{
    return m_vecDBList[0]->ListRange(request , response,list_key);
}
int MultiDatabase::ListRem(  const ::phxkv::ListRequest* request, std::string list_key  )//删除N个等值元素
{
    return m_vecDBList[0]->ListRem(request ,list_key  );
}
int MultiDatabase::ListSet(  const ::phxkv::ListRequest* request, std::string list_key )//根据下表进行更新元素
{
    return m_vecDBList[0]->ListSet(request ,list_key);
}
int MultiDatabase::ListTtim(  const ::phxkv::ListRequest* request, std::string list_key )//保留指定区间元素，其余删除
{
    return m_vecDBList[0]->ListTtim(request ,list_key);
}
int MultiDatabase::ListRpush(  const ::phxkv::ListRequest* request, std::string list_key )//尾插法
{
    return m_vecDBList[0]->ListRpush(request ,list_key);
}
int MultiDatabase::ListRpushx(  const ::phxkv::ListRequest* request, std::string list_key )//链表存在时，才执行尾部插入
{
    return m_vecDBList[0]->ListRpushx(request ,list_key);
}

//=============set===================
int MultiDatabase::SAdd(  const ::phxkv::SetRequest* request,const std::string & set_key )
{
    return m_vecDBList[0]->SAdd(request , set_key);
}
int MultiDatabase::SRem(  const ::phxkv::SetRequest* request, const std::string & set_key)
{
    return m_vecDBList[0]->SRem(request , set_key);
}
int MultiDatabase::SCard(  const ::phxkv::SetRequest* request, uint64_t & length,const std::string & set_key)
{
    return m_vecDBList[0]->SCard(request ,length, set_key);
}
 
int MultiDatabase::SMembers(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string & set_key)
{
    return m_vecDBList[0]->SMembers(request ,response, set_key);
}
int MultiDatabase::SUnionStore(  const ::phxkv::SetRequest* request,const std::string & set_key)
{
    return m_vecDBList[0]->SUnionStore(request , set_key);
}
int MultiDatabase::SUnion(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response )
{
    return m_vecDBList[0]->SUnion(request , response);
}
int MultiDatabase::SInterStore(  const ::phxkv::SetRequest* request, const std::string & set_key)
{
    return m_vecDBList[0]->SInterStore(request , set_key);
}
int MultiDatabase::SInter(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response )
{
    return m_vecDBList[0]->SInter(request , response);
}
int MultiDatabase::SDiffStore(  const ::phxkv::SetRequest* request,const std::string & set_key)
{
    return m_vecDBList[0]->SDiffStore(request , set_key);
}
int MultiDatabase::SDiff(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response )
{
    return m_vecDBList[0]->SDiff(request , response);
}
int MultiDatabase::SIsMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string & set_key)
{   
    return m_vecDBList[0]->SIsMember(request , response,set_key );
}
int MultiDatabase::SPop(  const ::phxkv::SetRequest* request, const std::string & set_key , std::string &ret_key)
{
    return m_vecDBList[0]->SPop(request ,set_key , ret_key);
}
int MultiDatabase::SRandMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string & set_key)
{
    return m_vecDBList[0]->SRandMember(request , response,set_key);
}
int MultiDatabase::SMove(  const ::phxkv::SetRequest* request, const std::string & set_key)
{
    return m_vecDBList[0]->SMove(request , set_key);
}

int MultiDatabase::ZAdd( const ::phxkv::ZsetRequest* request,const std::string & zset_key)//一个或多个
{
    return m_vecDBList[0]->ZAdd(request , zset_key);
}
int MultiDatabase::ZCard( const ::phxkv::ZsetRequest* request, uint64_t & length,const std::string & zset_key)//元素数量
{
    return m_vecDBList[0]->ZCard(request ,length, zset_key);
}
int MultiDatabase::ZCount( const ::phxkv::ZsetRequest* request, uint64_t& length,const std::string & zset_key)//有序集合中，在区间内的数量
{
    return m_vecDBList[0]->ZCount(request ,length, zset_key);
}
int MultiDatabase::ZRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)//有序集合区间内的成员
{
    return m_vecDBList[0]->ZRange(request , response,zset_key);
}
int MultiDatabase::ZIncrby( const ::phxkv::ZsetRequest* request,  const std::string & zset_key,std::string & ret_key)//为score增加或减少
{
    return m_vecDBList[0]->ZIncrby(request , zset_key,ret_key );
}
int MultiDatabase::ZUnionStore( const ::phxkv::ZsetRequest*request ,const std::string & zset_key)//并集不输出
{
    return m_vecDBList[0]->ZUnionStore(request , zset_key);
}
int MultiDatabase::ZInterStore( const ::phxkv::ZsetRequest* request,const std::string & zset_key)//交集不输出
{
    return m_vecDBList[0]->ZInterStore(request , zset_key);
}
int MultiDatabase::ZRangebyscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)
{
    return m_vecDBList[0]->ZRangebyscore(request , response,zset_key);
}
int MultiDatabase::ZRem( const ::phxkv::ZsetRequest* request ,const std::string & zset_key)//移除有序集 key 中的一个或多个成员
{
    return m_vecDBList[0]->ZRem(request , zset_key);
}
int MultiDatabase::MultiDatabase::ZRank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key)//返回member的排名，
{
    return m_vecDBList[0]->ZRank(request ,rank_member, zset_key);
}
int MultiDatabase::ZRevrank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key)//member逆序排名
{
    return m_vecDBList[0]->ZRevrank(request , rank_member,zset_key);
}
int MultiDatabase::ZScore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)//member的score
{
    return m_vecDBList[0]->ZScore(request ,response, zset_key);
}
int MultiDatabase::ZREVRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)//指定区间成员，逆序输出
{
    return m_vecDBList[0]->ZREVRange(request ,response, zset_key);
}
int MultiDatabase::ZREVRangebylscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)
{
    return m_vecDBList[0]->ZREVRangebylscore(request , response,zset_key);
}
int MultiDatabase::ZRemrangebyrank( const ::phxkv::ZsetRequest* request, const std::string & zset_key)
{
    return m_vecDBList[0]->ZRemrangebyrank(request , zset_key);
}
int MultiDatabase::ZRemrangebyscore( const ::phxkv::ZsetRequest* request, const std::string & zset_key)
{
    return m_vecDBList[0]->ZRemrangebyscore(request , zset_key);
}

}


