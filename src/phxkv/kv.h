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

#include <mutex>
#include "rocksdb/db.h"
#include <string>
#include "metrics.h"
#include "util.h"
#include "phxpaxos/storage.h"
#include "phxkv.pb.h"

namespace phxkv
{

enum KVClientRet
{
    KVCLIENT_SYS_FAIL = -1,//
    makeKVCLIENT_OK = 0,
    KVCLIENT_KEY_NOTEXIST = 1,
    KVCLIENT_BUSY=2,
    KVCLIENT_PARAM_ERROR=3,
    KVCLIENT_META_NOTEXIST = 4,
    KVCLIENT_ROCKSDB_ERR = 5,//rocksdb执行出错
    KVCLIENT_KEY_EXIST=6,
};

//#define KV_CHECKPOINT_KEY ((uint64_t)-1)

class KVClient
{
public:
    KVClient();
    ~KVClient();

    bool Init(phxpaxos::LogStorage * logStorage);

    static KVClient * Instance();

    int Get(const std::string & sKey, std::string & sValue );

    int Set(const std::string & sKey, const std::string & sValue  );

    int Del(const std::string & sKey );

    int GetCheckpointInstanceID(const int iGroupIdx,uint64_t & llCheckpointInstanceID);

    int SetCheckpointInstanceID(const int iGroupIdx,const uint64_t llCheckpointInstanceID);

    void DumpRocksdbStatus();

    int BatchGet(const KvBatchGetRequest* request, KvBatchGetResponse* response );

    int batchSet(const KvBatchPutRequest* request);
    bool CreateCheckpoint( const std::string checkpoint_dir );

    //hash相关
    int HashDel(const ::phxkv::HashRequest* request ,std::string hash_key);
    int HashSet( const ::phxkv::HashRequest* request ,std::string hash_key);
    int HashGet( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key);
    int HashGetAll(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response  ,std::string hash_key );
    int HashExist(const ::phxkv::HashRequest* request, ::phxkv::HashResponse* response,std::string hash_key );
    int HashIncrByInt(const ::phxkv::HashRequest* request ,std::string hash_key,std::string & ret_value);
    int HashIncrByFloat(const ::phxkv::HashRequest* request ,std::string hash_key,std::string & ret_value);
    int HashKeys( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key);
    int HashLen(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response ,std::string hash_key ,uint64_t &length);
    int HashMget(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response ,std::string hash_key);
    int HashMset(const ::phxkv::HashRequest* request ,std::string hash_key );
    int HashSetNx(const ::phxkv::HashRequest* request  ,std::string hash_key);
    int HashValues( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key);
    //=============list==========================
    int ListLpush(const ::phxkv::ListRequest* request,std::string list_key);
    int ListLpushx(const ::phxkv::ListRequest* request,std::string list_key);
    int ListLpop(const ::phxkv::ListRequest* request,std::string list_key,std::string& value);
    int ListLength(const ::phxkv::ListRequest* request,::phxkv::ListResponse* response,std::string list_key,uint64_t& length);
    int ListRpop(  const ::phxkv::ListRequest* request, std::string list_key,std::string& value );//弹出表尾
    int ListRpopLpush(  const ::phxkv::ListRequest* request,std::string list_key ,std::string &ret_value);//尾部进行头插
    int ListIndex(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key);//插入到指定位置
    int ListInsert(  const ::phxkv::ListRequest* request, std::string list_key );//插入到指定位置
    int ListRange(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key);
    int ListRem(  const ::phxkv::ListRequest* request, std::string list_key  );//删除N个等值元素
    int ListSet(  const ::phxkv::ListRequest* request, std::string list_key );//根据下表进行更新元素
    int ListTtim(  const ::phxkv::ListRequest* request, std::string list_key );//保留指定区间元素，其余删除
    int ListRpush(  const ::phxkv::ListRequest* request, std::string list_key );//尾插法
    int ListRpushx(  const ::phxkv::ListRequest* request, std::string list_key );//链表存在时，才执行尾部插入
    //============set相关==========================
    int SAdd(  const ::phxkv::SetRequest* request, std::string set_key);
    int SRem(  const ::phxkv::SetRequest* request,  std::string set_key );
    int SCard(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, std::string set_key,uint64_t & length);
    int SMembers(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, std::string set_key);
    int SUnionStore(  const ::phxkv::SetRequest* request,std::string set_key );
    int SUnion(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response );
    int SInterStore(  const ::phxkv::SetRequest* request, std::string set_key );
    int SInter(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response );
    int SDiffStore(  const ::phxkv::SetRequest* request, std::string set_key );
    int SDiff(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response );
    int SIsMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, std::string set_key);
    int SPop(  const ::phxkv::SetRequest* request, std::string set_key,std::string & ret_key );
    int SRandMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, std::string set_key);
    int SMove(  const ::phxkv::SetRequest* request,std::string set_key );

    //============zset相关==========================
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
    //=========删除============
    int DelKey(const std::string& key);
    int ExpireKey(const std::string& key,const int ttl);
    
private:
    phxpaxos::LogStorage * m_poLogStorage;

    //监控
    std::shared_ptr<TimerMetric> KvSet_Rocksdb_Time;//记录 rocksdb kvset 耗时
    std::shared_ptr<TimerMetric> BatchSet_Rocksdb_Time;//记录 rocksdb batchset 耗时
};
    
}
