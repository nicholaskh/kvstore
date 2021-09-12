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

#include "kv.h"
#include "phxkv.pb.h"
#include "log.h"

//using namespace phxpaxos;
using namespace std;
#define TTL_LENGTH ( sizeof( uint8_t )+sizeof(uint64_t ) )
namespace phxkv
{

KVClient :: KVClient()
{
    KvSet_Rocksdb_Time.reset( g_pmetrics->NewTimer("write_kv_rocksdb_cost", "", {{"op", "kvrocksdb"}}));;
    BatchSet_Rocksdb_Time.reset( g_pmetrics->NewTimer("write_batchkv_rocksdb_cost", "", {{"op", "batch_rocksdb"}}));;
}

KVClient :: ~KVClient()
{
}

bool KVClient :: Init(phxpaxos::LogStorage * logStorage )
{
    m_poLogStorage = logStorage;
    return true;
}

KVClient * KVClient :: Instance()
{
    static KVClient oKVClient;
    return &oKVClient;
}

int KVClient :: Get(const std::string & sKey, std::string & sValue )
{
    int ret =  m_poLogStorage->KvGet(  sKey, sValue);
    return ret;
}

int KVClient :: Set(const std::string & sKey, const std::string & sValue  )
{
     
    auto start = std::chrono::system_clock::now();
    //值加上过期时间
    std::string db_value=sValue;
    int ret = m_poLogStorage->KvSet(  sKey, db_value);
    auto end = std::chrono::system_clock::now();

    std::chrono::duration<double> elapsed_seconds = end - start;
    KvSet_Rocksdb_Time->Observe(elapsed_seconds.count());

    return ret;
}

int KVClient :: Del(const std::string & sKey )
{
    
    return m_poLogStorage->KvDel(  sKey);
}

int KVClient :: GetCheckpointInstanceID(const int iGroupIdx,uint64_t & llCheckpointInstanceID)
{

    return m_poLogStorage->KvGetCheckpointInstanceID( iGroupIdx,llCheckpointInstanceID );
}

int KVClient :: SetCheckpointInstanceID(const int iGroupIdx,const uint64_t llCheckpointInstanceID)
{

    return m_poLogStorage->KvSetCheckpointInstanceID( iGroupIdx, llCheckpointInstanceID );
     
}

void KVClient :: DumpRocksdbStatus()
{
    DEBUG_LOG("start DumpRocksdbStatus");
    return m_poLogStorage->DumpRocksDBStats(  );
}


int KVClient :: BatchGet(const ::phxkv::KvBatchGetRequest* request, ::phxkv::KvBatchGetResponse* response )
{
    DEBUG_LOG("KVClient :: BatchGet");
    bool ret = m_poLogStorage->KvBatchGet(  request,  response );

    for (int i = 0; i < response->values_size(); i++)
    {
        auto v = response->mutable_values( i );
        if( KVCLIENT_OK == v->ret()  ){  //对查询结果进行过滤

            const std::string db_value = v->value();
            
            v->set_value( db_value );
        }
    }

    return KVCLIENT_OK;
}

int KVClient :: batchSet(const KvBatchPutRequest* request )
{
    //值加上过期时间
    DEBUG_LOG("batchSet...");
    auto start = std::chrono::system_clock::now();
    phxkv::KvBatchPutRequest put_request;
    put_request.set_operator_( request->operator_() );

    for( int i=0;i< request->subs_size();i++ ){

        phxkv::KvBatchPutSubRequest* sub = put_request.add_subs();

        string key = request->subs( i ).key();
        string value = request->subs( i ).value();

        sub->set_key( key );
        sub->set_value( value );
    }
    bool ret = m_poLogStorage->KvBatchSet(  put_request );
    DEBUG_LOG("batchSet finish..%d.", ret);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    BatchSet_Rocksdb_Time->Observe(elapsed_seconds.count());

    return KVCLIENT_OK;                    
}

bool KVClient :: CreateCheckpoint( const std::string checkpoint_dir )
{
    return m_poLogStorage->CreateCheckpoint(  checkpoint_dir );
}

int KVClient :: HashDel(const ::phxkv::HashRequest* request ,std::string hash_key)
{
    return m_poLogStorage->HashDel(  request ,hash_key);
}
int KVClient :: HashGet(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,string hash_key)
{
    return m_poLogStorage->HashGet(  request ,response,hash_key );
}
int KVClient :: HashSet(const ::phxkv::HashRequest* request,std::string hash_key )
{
    return m_poLogStorage->HashSet(  request ,hash_key);
}
int KVClient :: HashExist(const ::phxkv::HashRequest* request, ::phxkv::HashResponse* response,std::string hash_key)
{
    return m_poLogStorage->HashExist(  request ,response ,hash_key);
}
int KVClient :: HashIncrByInt(const ::phxkv::HashRequest* request,std::string hash_key ,std::string & ret_value)
{
    return m_poLogStorage->HashIncrByInt(request ,hash_key,ret_value );
}
int KVClient :: HashIncrByFloat(const ::phxkv::HashRequest* request ,std::string hash_key,std::string & ret_value)
{
    return m_poLogStorage->HashIncrByFloat(request ,hash_key ,ret_value);
}
int KVClient :: HashKeys( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key)
{
    return m_poLogStorage->HashKeys(request , response,hash_key );
}
int KVClient :: HashLen(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response ,std::string hash_key ,uint64_t &length)
{
    return m_poLogStorage->HashLen(request,response ,hash_key, length);
}
int KVClient :: HashMget(const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key )
{
    return m_poLogStorage->HashMget(request,response,hash_key );
}
int KVClient :: HashMset(const ::phxkv::HashRequest* request ,std::string hash_key )
{
    return m_poLogStorage->HashMset(request ,hash_key);
}
int KVClient :: HashSetNx(const ::phxkv::HashRequest* request  ,std::string hash_key)
{
    return m_poLogStorage->HashSetNx(request ,hash_key);
}
int KVClient :: HashValues( const ::phxkv::HashRequest* request ,::phxkv::HashResponse* response,std::string hash_key)
{
    return m_poLogStorage->HashValues(request,response,hash_key );
}
int KVClient :: HashGetAll(const ::phxkv::HashRequest* request,::phxkv::HashResponse* response ,std::string hash_key  )
{
    return m_poLogStorage->HashGetAll(request , response,hash_key );
}

int KVClient::DelKey(const std::string& key)
{
    return m_poLogStorage->DelKey( key );
}
int KVClient::ExpireKey(const std::string& key,const int ttl)
{
    return m_poLogStorage->ExpireKey( key, ttl );
}
//=================list=============
int KVClient::ListLpush(const ::phxkv::ListRequest* request,std::string list_key)
{
    return m_poLogStorage->ListLpush(request ,list_key );
}
int KVClient::ListLpushx(const ::phxkv::ListRequest* request,std::string list_key)
{
    return m_poLogStorage->ListLpushx(request ,list_key );
}
int KVClient::ListLpop(const ::phxkv::ListRequest* request,std::string list_key,std::string& value)
{
    return m_poLogStorage->ListLpop(request ,list_key ,value);
}
int KVClient::ListLength(const ::phxkv::ListRequest* request,::phxkv::ListResponse* response,std::string list_key,uint64_t& length)
{
    return m_poLogStorage->ListLength(request ,response, list_key ,length );
}
int KVClient::ListRpop(  const ::phxkv::ListRequest* request, std::string list_key,std::string& value )//弹出表尾
{
    return m_poLogStorage->ListRpop(request ,list_key ,value);
}
int KVClient::ListRpopLpush(  const ::phxkv::ListRequest* request,std::string list_key,std::string &ret_value )//尾部进行头插
{
    return m_poLogStorage->ListRpopLpush(request ,list_key,ret_value );
}
int KVClient::ListIndex(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key)//插入到指定位置
{
    return m_poLogStorage->ListIndex(request ,response, list_key );
}
int KVClient::ListInsert(  const ::phxkv::ListRequest* request, std::string list_key )//插入到指定位置
{
    return m_poLogStorage->ListInsert(request ,list_key );
}
int KVClient::ListRange(  const ::phxkv::ListRequest* request, ::phxkv::ListResponse* response, std::string list_key)
{
    return m_poLogStorage->ListRange(request ,response,list_key );
}
int KVClient::ListRem(  const ::phxkv::ListRequest* request, std::string list_key  )//删除N个等值元素
{
    return m_poLogStorage->ListRem(request ,list_key  );
}
int KVClient::ListSet(  const ::phxkv::ListRequest* request, std::string list_key )//根据下表进行更新元素
{
    return m_poLogStorage->ListSet(request ,list_key );
}
int KVClient::ListTtim(  const ::phxkv::ListRequest* request, std::string list_key )//保留指定区间元素，其余删除
{
    return m_poLogStorage->ListTtim(request ,list_key );
}
int KVClient::ListRpush(  const ::phxkv::ListRequest* request, std::string list_key )//尾插法
{
    return m_poLogStorage->ListRpush(request ,list_key );
}
int KVClient::ListRpushx(  const ::phxkv::ListRequest* request, std::string list_key )//链表存在时，才执行尾部插入
{
    return m_poLogStorage->ListRpushx(request ,list_key );
}
//=================set==============
int KVClient::SAdd(  const ::phxkv::SetRequest* request, std::string set_key)
{
    return m_poLogStorage->SAdd(request , set_key );
}
int KVClient::SRem(  const ::phxkv::SetRequest* request,  std::string set_key )
{
    return m_poLogStorage->SRem(request ,set_key );
}
int KVClient::SCard(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, std::string set_key,uint64_t & length)
{
    return m_poLogStorage->SCard(request , length , set_key );
}
int KVClient::SMembers(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, std::string set_key)
{
    return m_poLogStorage->SMembers(request , response,set_key );
}
int KVClient::SUnionStore(  const ::phxkv::SetRequest* request,std::string set_key )
{
    return m_poLogStorage->SUnionStore(request ,set_key );
}
int KVClient::SUnion(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response )
{
    return m_poLogStorage->SUnion(request , response );
}
int KVClient::SInterStore(  const ::phxkv::SetRequest* request, std::string set_key )
{
    return m_poLogStorage->SInterStore(request ,set_key );
}
int KVClient::SInter(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response )
{
    return m_poLogStorage->SInter(request , response );
}
int KVClient::SDiffStore(  const ::phxkv::SetRequest* request, std::string set_key )
{
    return m_poLogStorage->SDiffStore(request , set_key );
}
int KVClient::SDiff(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response )
{
    return m_poLogStorage->SDiff(request , response );
}
int KVClient::SIsMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, std::string set_key)
{
    return m_poLogStorage->SIsMember(request , response,set_key );
}
int KVClient::SPop(  const ::phxkv::SetRequest* request, std::string set_key,std::string & ret_key )
{
    return m_poLogStorage->SPop(request ,set_key , ret_key);
}
int KVClient::SRandMember(  const ::phxkv::SetRequest* request, ::phxkv::SetResponse* response, std::string set_key)
{
    return m_poLogStorage->SRandMember(request , response,set_key );
}
int KVClient::SMove(  const ::phxkv::SetRequest* request,std::string set_key )
{
    return m_poLogStorage->SMove(request ,set_key );
}

int KVClient::ZAdd( const ::phxkv::ZsetRequest* request,const std::string & zset_key)//一个或多个
{
    return m_poLogStorage->ZAdd(request ,zset_key );
}
int KVClient::ZCard( const ::phxkv::ZsetRequest* request, uint64_t & length,const std::string & zset_key)//元素数量
{
    return m_poLogStorage->ZCard(request ,length,zset_key );
}
int KVClient::ZCount( const ::phxkv::ZsetRequest* request, uint64_t& length,const std::string & zset_key)//有序集合中，在区间内的数量
{
    return m_poLogStorage->ZCount(request ,length,zset_key );
}
int KVClient::ZRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)//有序集合区间内的成员
{
    return m_poLogStorage->ZRange(request ,response,zset_key );
}
int KVClient::ZIncrby( const ::phxkv::ZsetRequest* request,  const std::string & zset_key,std::string& ret_value)//为score增加或减少
{
    return m_poLogStorage->ZIncrby(request ,zset_key ,ret_value);
}
int KVClient::ZUnionStore( const ::phxkv::ZsetRequest* request,const std::string & zset_key)//并集不输出
{
    return m_poLogStorage->ZUnionStore(request ,zset_key );
}
int KVClient::ZInterStore( const ::phxkv::ZsetRequest* request,const std::string & zset_key)//交集不输出
{
    return m_poLogStorage->ZInterStore(request ,zset_key );
}
int KVClient::ZRangebyscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)
{
    return m_poLogStorage->ZRangebyscore(request ,response,zset_key );
}
int KVClient::ZRem( const ::phxkv::ZsetRequest* request ,const std::string & zset_key)//移除有序集 key 中的一个或多个成员
{
    return m_poLogStorage->ZRem(request ,zset_key );
}
int KVClient::ZRank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key)//返回member的排名，
{
    return m_poLogStorage->ZRank(request ,rank_member,zset_key );
}
int KVClient::ZRevrank( const ::phxkv::ZsetRequest* request, uint64_t & rank_member,const std::string & zset_key)//member逆序排名
{
    return m_poLogStorage->ZRevrank(request ,rank_member,zset_key );
}

int KVClient::ZScore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)//member的score
{
    return m_poLogStorage->ZScore(request ,response,zset_key );
}
int KVClient::ZREVRange( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)//指定区间成员，逆序输出
{
    return m_poLogStorage->ZREVRange(request ,response,zset_key );
}
int KVClient::ZREVRangebylscore( const ::phxkv::ZsetRequest* request, ::phxkv::ZsetResponse* response,const std::string & zset_key)
{
    return m_poLogStorage->ZREVRangebylscore(request ,response, zset_key );
}
int KVClient::ZRemrangebyrank( const ::phxkv::ZsetRequest* request, const std::string & zset_key)
{
    return m_poLogStorage->ZRemrangebyrank(request ,zset_key );
}
int KVClient::ZRemrangebyscore( const ::phxkv::ZsetRequest* request, const std::string & zset_key)
{
    return m_poLogStorage->ZRemrangebyscore(request ,zset_key );
}

}

