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

#include <string>
#include <typeinfo>
#include "rocksdb/utilities/transaction_db.h"
#include <inttypes.h>
#include "phxkv.pb.h"
enum KVClientRet  //操作rocksdb的返回状态值
{
    KVCLIENT_SYS_FAIL = -1,//
    KVCLIENT_OK = 0,
    KVCLIENT_KEY_NOTEXIST = 1,
    KVCLIENT_BUSY=2,
    KVCLIENT_PARAM_ERROR=3,
    KVCLIENT_META_NOTEXIST = 4,
    KVCLIENT_ROCKSDB_ERR = 5,//rocksdb执行出错
    KVCLIENT_KEY_EXIST=6,
};

namespace phxpaxos
{

//Paxoslib need to storage many datas, if you want to storage datas yourself,
//you must implememt all function in class LogStorage, and make sure that observe the writeoptions.

class WriteOptions
{
public:
    WriteOptions() : bSync(true) { }
    bool bSync;
};

class LogStorage
{
public:
    virtual ~LogStorage() {}

    virtual const std::string GetLogStorageDirPath(const int iGroupIdx) = 0;

    virtual int Get(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue) = 0;

    virtual int Put(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue) = 0;

    virtual int Del(const WriteOptions & oWriteOptions, int iGroupIdx, const uint64_t llInstanceID) = 0;

    virtual int GetMaxInstanceID(const int iGroupIdx, uint64_t & llInstanceID) = 0;

    virtual int SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llMinInstanceID) = 0;

    virtual int GetMinChosenInstanceID(const int iGroupIdx, uint64_t & llMinInstanceID) = 0;

    virtual int ClearAllLog(const int iGroupIdx) = 0;

    virtual int SetSystemVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer) = 0;

    virtual int GetSystemVariables(const int iGroupIdx, std::string & sBuffer) = 0;

    virtual int SetMasterVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer) = 0;

    virtual int GetMasterVariables(const int iGroupIdx, std::string & sBuffer) = 0;

    //新增
    virtual int KvGet(const std::string & sKey, std::string & sValue )= 0;;

    virtual int KvSet(const std::string & sKey, const std::string & sValue )= 0;;

    virtual int KvDel(const std::string & sKey )= 0;;

    virtual bool KvBatchGet(const ::phxkv::KvBatchGetRequest* request, ::phxkv::KvBatchGetResponse* response)=0;

    virtual bool KvBatchSet(const ::phxkv::KvBatchPutRequest request)=0;;

    virtual int KvGetCheckpointInstanceID(const uint16_t iGroupIdx,uint64_t & llCheckpointInstanceID)= 0;;

    virtual int KvSetCheckpointInstanceID(const uint16_t iGroupIdx,const uint64_t llCheckpointInstanceID)= 0;;

    virtual void DumpRocksDBStats()= 0 ;

    virtual rocksdb::TransactionDB * GetDbObj()=0;
    virtual rocksdb::ColumnFamilyHandle* GetCfData()=0;

   virtual bool CreateCheckpoint( const std::string checkpoint_dir )=0;

    //hash相关
    virtual int HashDel(const ::phxkv::HashRequest* request ,std::string hash_key)=0;
    virtual int HashSet( const ::phxkv::HashRequest* request ,std::string hash_key)=0;
    virtual int HashGet( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key)=0;
    virtual int HashGetAll(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response ,std::string hash_key  )=0;
    virtual int HashExist(const ::phxkv::HashRequest* request, ::phxkv::HashResponse* response ,std::string hash_key)=0;
    virtual int HashIncrByInt(const ::phxkv::HashRequest* request ,std::string hash_key,std::string & ret_value)=0;
    virtual int HashIncrByFloat(const ::phxkv::HashRequest* request ,std::string hash_key,std::string & ret_value)=0;
    virtual int HashKeys( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key)=0;
    virtual int HashLen(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response  ,std::string hash_key,uint64_t &length)=0;//返回长度
    virtual int HashMget(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response ,std::string hash_key)=0;
    virtual int HashMset(const ::phxkv::HashRequest* request ,std::string hash_key )=0;
    virtual int HashSetNx(const ::phxkv::HashRequest* request ,std::string hash_key )=0;
    virtual int HashValues( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key)=0;

    //key相关
    virtual int DelKey(const std::string& key)=0;
    virtual int ExpireKey(const std::string& key,const int ttl)=0;

    //list相关
    virtual int ListLpush(const ::phxkv::ListRequest* request,std::string list_key)=0;
    virtual int ListLpushx(const ::phxkv::ListRequest* request,std::string list_key)=0;
    virtual int ListLpop(const ::phxkv::ListRequest* request,std::string list_key,std::string& value)=0;
    virtual int ListLength(const ::phxkv::ListRequest* request,::phxkv::ListResponse* response,std::string list_key,uint64_t& length)=0;
    virtual int ListRpop(  const ::phxkv::ListRequest* request, std::string list_key,std::string& value )=0;//弹出表尾
    virtual int ListRpopLpush(  const ::phxkv::ListRequest* request,std::string list_key,std::string &ret_value )=0;//尾部进行头插
    virtual int ListIndex(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key)=0;//插入到指定位置
    virtual int ListInsert(  const ::phxkv::ListRequest* request, std::string list_key )=0;//插入到指定位置
    virtual int ListRange(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key)=0;
    virtual int ListRem(  const ::phxkv::ListRequest* request, std::string list_key )=0;;//删除N个等值元素
    virtual int ListSet(  const ::phxkv::ListRequest* request, std::string list_key )=0;;//根据下表进行更新元素
    virtual int ListTtim(  const ::phxkv::ListRequest* request, std::string list_key )=0;;//保留指定区间元素，其余删除
    virtual int ListRpush(  const ::phxkv::ListRequest* request, std::string list_key )=0;;//尾插法
    virtual int ListRpushx(  const ::phxkv::ListRequest* request, std::string list_key )=0;;//链表存在时，才执行尾部插入

    //set相关
    virtual int SAdd(  const ::phxkv::SetRequest* request, const std::string& set_keyy)=0;
    virtual int SRem(  const ::phxkv::SetRequest* request,  const std::string& set_key )=0;
    virtual int SCard(  const ::phxkv::SetRequest* request, uint64_t & length, const std::string& set_key)=0;
    virtual int SMembers(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, const std::string& set_key)=0;
    virtual int SUnionStore(  const ::phxkv::SetRequest* request,const std::string& set_key )=0;
    virtual int SUnion(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response )=0;
    virtual int SInterStore(  const ::phxkv::SetRequest* request, const std::string& set_key )=0;
    virtual int SInter(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response )=0;
    virtual int SDiffStore(  const ::phxkv::SetRequest* request, const std::string& set_key )=0;
    virtual int SDiff(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response  )=0;
    virtual int SIsMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response,const std::string& set_key)=0;
    virtual int SPop(  const ::phxkv::SetRequest* request, const std::string& set_key , std::string &ret_key)=0;
    virtual int SRandMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, const std::string& set_key)=0;
    virtual int SMove(  const ::phxkv::SetRequest* request,const std::string& set_key )=0;

    //zset相关
    virtual int ZAdd( const ::phxkv::ZsetRequest* request,const std::string & zset_key)=0;;//一个或多个
    virtual int ZCard( const ::phxkv::ZsetRequest* request, uint64_t & length,const std::string & zset_key)=0;;//元素数量
    virtual int ZCount( const ::phxkv::ZsetRequest* request, uint64_t& length,const std::string & zset_key)=0;;//有序集合中，在区间内的数量
    virtual int ZRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)=0;;//有序集合区间内的成员
    virtual int ZIncrby( const ::phxkv::ZsetRequest* request,  const std::string & zset_key,std::string & ret_key)=0;;//为score增加或减少
    virtual int ZUnionStore( const ::phxkv::ZsetRequest* ,const std::string & zset_key)=0;;//并集不输出
    virtual int ZInterStore( const ::phxkv::ZsetRequest* request,const std::string & zset_key)=0;;//交集不输出
    virtual int ZRangebyscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)=0;;
    virtual int ZRem( const ::phxkv::ZsetRequest* request ,const std::string & zset_key)=0;;//移除有序集 key 中的一个或多个成员
    virtual int ZRank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key)=0;;//返回member的排名，
    virtual int ZRevrank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key)=0;;//member逆序排名
    virtual int ZScore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)=0;;//member的score
    virtual int ZREVRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)=0;;//指定区间成员，逆序输出
    virtual int ZREVRangebylscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)=0;;
    virtual int ZRemrangebyrank( const ::phxkv::ZsetRequest* request, const std::string & zset_key)=0;
    virtual int ZRemrangebyscore( const ::phxkv::ZsetRequest* request, const std::string & zset_key)=0;;
};

}
