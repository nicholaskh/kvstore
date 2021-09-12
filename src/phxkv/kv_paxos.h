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

#include "phxpaxos/node.h"
#include "kvsm.h"
#include <string>
#include <vector>
#include "log.h"
#include "def.h"
#include "metrics.h"
#include <memory.h>
#include "kv_encode.h"
namespace phxkv
{

class PhxKV
{
public:
    PhxKV(const phxpaxos::NodeInfo & oMyNode, const phxpaxos::NodeInfoList & vecNodeList,
            const std::string & sKVDBPath, const std::string & sPaxosLogPath);
    ~PhxKV();

    int RunPaxos();
        phxpaxos::NodeInfo GetMasterByGroup( int groupid);//通过groupid 获取
        const bool IsMaster(const int  iGroupId);
        const bool DropMaster(const int  iGroupId);

PhxKVStatus Put(const int groupid, const std::string & sKey, const std::string & sValue );

PhxKVStatus GetLocal(const std::string & sKey, std::string & sValue );
//批量读写
    PhxKVStatus BatchPut(const int groupid,const std::string & sKey,const KvBatchPutRequest & data);
    PhxKVStatus BatchGet(const KvBatchGetRequest* request, KvBatchGetResponse* response );     
    //删除key
    PhxKVStatus DelKey(const int groupid, const std::string & sKey  );
    //过期key
    PhxKVStatus ExpireKey(const int groupid, const std::string & sKey,const uint64_t ttl);

void DumpRocksdbStatus();
    void DumpClusterStatus();

    phxpaxos::Node * GetPaxosNode();
private:
     int KVPropose(const int groupid ,const std::string & sKey, const std::string & sPaxosValue, PhxKVSMCtx & oPhxKVSMCtx);
    int KVBatchPropose(const int groupid ,const std::string & sKey,const std::string & sPaxosValue, PhxKVSMCtx & oPhxKVSMCtx);

public:
    //====================hash相关============================
    int HashDel(const ::phxkv::Request* request );
    int HashGet( const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashSet( const ::phxkv::Request* request );
    int HashGetAll(const ::phxkv::Request* request, ::phxkv::Response* response );
    int HashExist(const ::phxkv::Request* request, ::phxkv::Response* response );
    int HashIncrByInt( const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashIncrByFloat( const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashKeys( const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashLen(const ::phxkv::Request* request, ::phxkv::Response* response );
    int HashMget( const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashMset( const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashSetNx(const ::phxkv::Request* request );
    int HashValues(const ::phxkv::Request* request, ::phxkv::Response* response );
    //=============list=================
    int ListLpush(const ::phxkv::Request* request );
    int ListLpushx(const ::phxkv::Request* request );
    int ListLpop(const ::phxkv::Request* request ,::phxkv::Response* response);
    int ListLength(const ::phxkv::Request* request,::phxkv::Response* response );
    int ListRpop(  const ::phxkv::Request* request ,::phxkv::Response* response );//弹出表尾
    int ListRpopLpush(  const ::phxkv::Request* request ,::phxkv::Response* response );//尾部进行头插
    int ListIndex(  const ::phxkv::Request* request, ::phxkv::Response* response );//插入到指定位置
    int ListInsert(  const ::phxkv::Request* request  );//插入到指定位置
    int ListRange(  const ::phxkv::Request* request, ::phxkv::Response* response );
    int ListRem(  const ::phxkv::Request* request  );//删除N个等值元素
    int ListSet(  const ::phxkv::Request* request  );//根据下表进行更新元素
    int ListTtim(  const ::phxkv::Request* request  );//保留指定区间元素，其余删除
    int ListRpush(  const ::phxkv::Request* request  );//尾插法
    int ListRpushx(  const ::phxkv::Request* request );//链表存在时，才执行尾部插入
    //========================set相关=====================================
    int SAdd(  const ::phxkv::Request* request);
    int SRem(  const ::phxkv::Request* request);
    int SCard(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SMembers(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SUnionStore(  const ::phxkv::Request* request );
    int SUnion(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SInterStore(  const ::phxkv::Request* request );
    int SInter(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SDiffStore(  const ::phxkv::Request* request );
    int SDiff(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SIsMember(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SPop(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SRandMember(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SMove(  const ::phxkv::Request* request);
    //==========================zset相关====================================
    int ZAdd( const ::phxkv::Request* request );//一个或多个
    int ZCard( const ::phxkv::Request* request, ::phxkv::Response* response);//元素数量
    int ZCount( const ::phxkv::Request* request, ::phxkv::Response* response);//有序集合中，在区间内的数量
    int ZRange( const ::phxkv::Request* request, ::phxkv::Response* response);//有序集合区间内的成员
    int ZIncrby( const ::phxkv::Request* request, ::phxkv::Response* response);//为score增加或减少
    int ZUnionStore( const ::phxkv::Request* request );//并集不输出
    int ZInterStore( const ::phxkv::Request* request );//交集不输出
    int ZRangebyscore( const ::phxkv::Request* request, ::phxkv::Response* response);
    int ZRem( const ::phxkv::Request* request );//移除有序集 key 中的一个或多个成员
    int ZRank( const ::phxkv::Request* request, ::phxkv::Response* response);//返回member的排名，
    int ZRevrank( const ::phxkv::Request* request, ::phxkv::Response* response);//member逆序排名
    int ZScore( const ::phxkv::Request* request, ::phxkv::Response* response);//member的score
    int ZREVRange( const ::phxkv::Request* request, ::phxkv::Response* response);//指定区间成员，逆序输出
    int ZREVRangebylscore( const ::phxkv::Request* request, ::phxkv::Response* response);
    int ZRemrangebyrank( const ::phxkv::Request* request );
    int ZRemrangebyscore( const ::phxkv::Request* request );

private:
    phxpaxos::NodeInfo m_oMyNode;
    phxpaxos::NodeInfoList m_vecNodeList;
    std::string m_sKVDBPath;
    std::string m_sPaxosLogPath;

    int m_iGroupCount;
    phxpaxos::Node * m_poPaxosNode;
    PhxKVSM m_oPhxKVSM;
    std::shared_ptr<Guage> oNode_Master;//是否有master
};
    
}


