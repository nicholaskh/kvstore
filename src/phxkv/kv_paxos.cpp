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

#include "kv_paxos.h"
#include <assert.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "sysconfig.h"
using namespace phxpaxos;
using namespace std;

namespace phxkv
{

PhxKV :: PhxKV(const phxpaxos::NodeInfo & oMyNode, const phxpaxos::NodeInfoList & vecNodeList,
        const std::string & sKVDBPath, const std::string & sPaxosLogPath)
    : m_oMyNode(oMyNode), m_vecNodeList(vecNodeList), 
    m_sKVDBPath(sKVDBPath), m_sPaxosLogPath(sPaxosLogPath), 
    m_poPaxosNode(nullptr), m_oPhxKVSM(sKVDBPath)
{
    //only show you how to use multi paxos group, you can set as 1, 2, or any other number.
    //not too large.
    m_iGroupCount = CConfig::Get()->paxosnodes.group_cnt;
    std::map<std::string, std::string> labels = {{"db", "0"}};
    oNode_Master.reset(g_pmetrics->NewGuage("CLUSTER_MASTER_VALID", "", labels ));
}

PhxKV :: ~PhxKV()
{
    delete m_poPaxosNode;
}

phxpaxos::NodeInfo PhxKV :: GetMasterByGroup( int groupid)
{
    return m_poPaxosNode->GetMaster(groupid);
}

const bool PhxKV :: IsMaster(const int  iGroupId)
{
    return m_poPaxosNode->IsIMMaster(iGroupId);
}
const bool PhxKV :: DropMaster(const int  iGroupId)
{
    return m_poPaxosNode->DropMaster(iGroupId);
}


int PhxKV :: RunPaxos()
{

    Options oOptions;
    oOptions.bUseCheckpointReplayer = true;
    oOptions.sLogStoragePath = m_sPaxosLogPath;

    //this groupcount means run paxos group count.
    //every paxos group is independent, there are no any communicate between any 2 paxos group.
    oOptions.iGroupCount = m_iGroupCount;

    oOptions.oMyNode = m_oMyNode;
    oOptions.vecNodeInfoList = m_vecNodeList;

    //because all group share state machine(kv), so every group have same state machine.
    //just for split key to different paxos group, to upgrate performance.
    for (int iGroupIdx = 0; iGroupIdx < m_iGroupCount; iGroupIdx++)
    {
        GroupSMInfo oSMInfo;
        oSMInfo.iGroupIdx = iGroupIdx;
        oSMInfo.vecSMList.push_back(&m_oPhxKVSM);
        oSMInfo.bIsUseMaster = true;

        oOptions.vecGroupSMInfoList.push_back(oSMInfo);
    }

    //set logfunc
    //oOptions.pLogFunc = LOGGER->GetLogFunc();
    oOptions.bUseBatchPropose = true;
    oOptions.bSync = true;
    oOptions.iSyncInterval = 200;
    LogStorage * pStorage;
    int ret = Node::RunNode(oOptions, m_poPaxosNode ,pStorage,m_oPhxKVSM  );
    if (ret != 0)
    {
        ERROR_LOG("run paxos fail, ret %d", ret);
        exit(0);
        return ret;
    }
    for(int index=0;index< m_iGroupCount;index++ ){

        m_poPaxosNode->SetBatchDelayTimeMs(index, 1 );//batch time
        m_poPaxosNode->SetBatchCount(index, 100);
    }
     
    INFO_LOG("run paxos ok\n");
    return 0;
}


int PhxKV :: KVPropose(const int groupid ,const std::string & sKey, const std::string & sPaxosValue, PhxKVSMCtx & oPhxKVSMCtx)
{
    int iGroupIdx =groupid; 
    SMCtx oCtx;
    //smid must same to PhxKVSM.SMID().
    oCtx.m_iSMID = 1;
    oCtx.m_pCtx = (void *)&oPhxKVSMCtx;

    uint64_t llInstanceID = 0;
    int ret = m_poPaxosNode->Propose(iGroupIdx, sPaxosValue, llInstanceID, &oCtx);
    if (ret != 0)
    {
        ERROR_LOG("paxos propose fail, key %s groupidx %d ret %d",sPaxosValue.c_str(), iGroupIdx, ret);
        return ret;
    }else{
        INFO_LOG("paxos propose ok, key %s groupidx %d ret %d",sPaxosValue.c_str(), iGroupIdx, ret);
    }

    return 0;
}
int PhxKV :: KVBatchPropose(const int groupid ,const std::string & sKey,const std::string & sPaxosValue, PhxKVSMCtx & oPhxKVSMCtx)
{
    int iGroupIdx =groupid;
    SMCtx oCtx;
    //smid must same to PhxKVSM.SMID().
    oCtx.m_iSMID = 1;
    oCtx.m_pCtx = (void *)&oPhxKVSMCtx;

    uint64_t llInstanceID = 0;
    uint32_t iBatchIndex = 0;
    
    int ret = m_poPaxosNode->BatchPropose(iGroupIdx, sPaxosValue, llInstanceID,iBatchIndex, &oCtx);
    if (ret != 0)
    {
        ERROR_LOG("paxos BatchPropose fail, key %s groupidx %d ret %d",sPaxosValue.c_str(), iGroupIdx, ret);
        return ret;
    }else{
        INFO_LOG("paxos BatchPropose ok, groupidx %d ret %d",iGroupIdx, ret);
    }

    return 0;
}

PhxKVStatus PhxKV :: Put(const int groupid, const std::string & sKey, const std::string & sValue  )
{
    string sPaxosValue;
    bool bSucc = PhxKVSM::MakeSetOpValue(sKey, sValue,sPaxosValue);
    if (!bSucc)
    {
        ERROR_LOG("MakeSetOpValue error");
        return FAIL;
    }

    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid, sKey, sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("KVBatchPropose error.");
        return PhxKVStatus::FAIL;
    }

    if (oPhxKVSMCtx.iExecuteRet == KVCLIENT_OK)
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("KVBatchPropose error.");
        return PhxKVStatus::FAIL;
    }
}
PhxKVStatus PhxKV :: GetLocal(const std::string & sKey, std::string & sValue)
{
    int ret = m_oPhxKVSM.GetKVClient()->Get(sKey, sValue );
    if (ret == KVCLIENT_OK)
    {
        return PhxKVStatus::SUCC;
    }
    else if (ret == KVCLIENT_KEY_NOTEXIST)
    {
        return PhxKVStatus::KEY_NOTEXIST; 
    }
    else
    {
        return PhxKVStatus::FAIL;
    }
}
PhxKVStatus PhxKV :: BatchPut(const int groupid,const std::string & sKey,const ::phxkv::KvBatchPutRequest & data)
{
    string sPaxosValue;
    bool bSucc = PhxKVSM::MakeBatchSetOpValue(data, sPaxosValue);
    if (!bSucc)
    {
        return FAIL;
    }

    PhxKVSMCtx oPhxKVSMCtx;
    
    int ret = KVBatchPropose( groupid,sKey, sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        return FAIL;
    }

    if (oPhxKVSMCtx.iExecuteRet == KVCLIENT_OK)
    {
        return SUCC;
    }
    else
    {
        return FAIL;
    }
}
PhxKVStatus PhxKV :: BatchGet(const ::phxkv::KvBatchGetRequest* request, ::phxkv::KvBatchGetResponse* response )
{
    DEBUG_LOG("PhxKV :: BatchGet");
    int ret = m_oPhxKVSM.GetKVClient()->BatchGet(request, response );
    if (ret == KVCLIENT_OK)
    {
        return SUCC;
    }
    else if (ret == KVCLIENT_KEY_NOTEXIST)
    {
        return KEY_NOTEXIST; 
    }
    else
    {
        return FAIL;
    }
}

phxpaxos::Node * PhxKV ::GetPaxosNode()
{
    return m_poPaxosNode;
}

void PhxKV :: DumpRocksdbStatus()
{
    return m_oPhxKVSM.GetKVClient()->DumpRocksdbStatus( );
}

void PhxKV :: DumpClusterStatus()
{
    int goup_cnt = CConfig::Get()->paxosnodes.group_cnt;
    int master_cnt=0;
    for(int k=0;k< goup_cnt;k++ ){

        bool flag  = m_poPaxosNode->IsIMMaster( k );
        if( flag ){
            master_cnt++;
        }
    }
    DEBUG_LOG("master cnt %d  "  ,master_cnt ); 
    oNode_Master->SetValue( master_cnt  );
     
}


int PhxKV :: HashDel(const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_HASH_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("HashDel SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("HashDel KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//处理SM的返回值
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("HashDel KVBatchPropose error.iExecuteRet=%d " , oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV :: HashGet( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string hash_key = request->key();
    phxkv::HashRequest hash_request = request->hash_req();
    phxkv::HashResponse* hash_response = response->mutable_hash_response();
    return m_oPhxKVSM.GetKVClient()->HashGet(&hash_request, hash_response,hash_key );
}
int PhxKV :: HashSet( const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_HASH_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("HashSet SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)//0代表提案成功
    {
        ERROR_LOG("HashSet KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        INFO_LOG("HashSet KVBatchPropose ok");
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("HashSet KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}

int PhxKV ::HashGetAll(const ::phxkv::Request* request, ::phxkv::Response* response )
{
    string hash_key = request->key();
    phxkv::HashRequest hash_request = request->hash_req();
    phxkv::HashResponse* hash_response = response->mutable_hash_response();
     return m_oPhxKVSM.GetKVClient()->HashGetAll(&hash_request, hash_response,hash_key );
}
int PhxKV ::HashExist(const ::phxkv::Request* request, ::phxkv::Response* response )
{
    string hash_key = request->key();
    phxkv::HashRequest hash_request = request->hash_req();
    phxkv::HashResponse* hash_response = response->mutable_hash_response();
    return m_oPhxKVSM.GetKVClient()->HashExist(&hash_request, hash_response,hash_key );
}
int PhxKV ::HashIncrByInt( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_HASH_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("HashIncrByInt SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("HashIncrByInt KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)
    {
        response->mutable_hash_response()->add_field()->set_field_value( oPhxKVSMCtx.sReadValue );
        INFO_LOG("HashIncrByInt KVBatchPropose ok");
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("HashIncrByInt KVBatchPropose error.=%d" , oPhxKVSMCtx.iExecuteRet );
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV ::HashIncrByFloat( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_HASH_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("HashIncrByFloat SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("HashIncrByFloat KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        INFO_LOG("HashIncrByFloat KVBatchPropose ok");
        response->mutable_hash_response()->add_field()->set_field_value( oPhxKVSMCtx.sReadValue );
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("HashIncrByFloat KVBatchPropose error.ret=%d", oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV ::HashKeys( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string hash_key = request->key();
    phxkv::HashRequest hash_request = request->hash_req();
    phxkv::HashResponse* hash_response = response->mutable_hash_response();
    return m_oPhxKVSM.GetKVClient()->HashKeys(&hash_request, hash_response ,hash_key);
}
int PhxKV ::HashLen(const ::phxkv::Request* request, ::phxkv::Response* response )
{
    string hash_key = request->key();
    phxkv::HashRequest hash_request = request->hash_req();
    phxkv::HashResponse* hash_response = response->mutable_hash_response();
    uint64_t length=0;
    int ret =  m_oPhxKVSM.GetKVClient()->HashLen(&hash_request, hash_response ,hash_key , length);
    response->set_length( length );
    return ret;
}
int PhxKV ::HashMget( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string hash_key = request->key();
    phxkv::HashRequest hash_request = request->hash_req();
    phxkv::HashResponse* hash_response = response->mutable_hash_response();
    return m_oPhxKVSM.GetKVClient()->HashMget(&hash_request, hash_response ,hash_key);
}
int PhxKV ::HashMset( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_HASH_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("HashMset SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("HashMset KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("HashMset KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
    
}
int PhxKV ::HashSetNx(const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_HASH_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("HashSetNx SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("HashSetNx KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("HashSetNx KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV ::HashValues(const ::phxkv::Request* request, ::phxkv::Response* response )
{
    string hash_key = request->key();
    phxkv::HashRequest hash_request = request->hash_req();
    phxkv::HashResponse* hash_response = response->mutable_hash_response();
    return m_oPhxKVSM.GetKVClient()->HashValues(&hash_request, hash_response ,hash_key);
}

//=============list=================
int PhxKV::ListLpush(const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.key=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListLpushx(const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListLpop(const ::phxkv::Request* request ,::phxkv::Response* response)
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        response->mutable_list_response()->add_field()->assign( oPhxKVSMCtx.sReadValue );
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListLength(const ::phxkv::Request* request,::phxkv::Response* response )
{
    string list_key = request->key();
    phxkv::ListRequest list_request = request->list_req();
    phxkv::ListResponse* list_response = response->mutable_list_response();
    uint64_t length=0;
    int ret= m_oPhxKVSM.GetKVClient()->ListLength(&list_request, list_response ,list_key ,length );
    response->set_length( length );
    return ret;
}
int PhxKV::ListRpop(  const ::phxkv::Request* request ,::phxkv::Response* response )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        response->mutable_list_response()->add_field()->assign( oPhxKVSMCtx.sReadValue );
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListRpopLpush(  const ::phxkv::Request* request ,::phxkv::Response* response )//尾部进行头插
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        response->mutable_list_response()->add_field()->assign( oPhxKVSMCtx.sReadValue );
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListIndex(  const ::phxkv::Request* request, ::phxkv::Response* response )//插入到指定位置
{
    string list_key = request->key();
    phxkv::ListRequest list_request = request->list_req();
    phxkv::ListResponse* list_response = response->mutable_list_response();
    return m_oPhxKVSM.GetKVClient()->ListIndex(&list_request, list_response ,list_key  );
}
int PhxKV::ListInsert(  const ::phxkv::Request* request  )//插入到指定位置
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListRange(  const ::phxkv::Request* request, ::phxkv::Response* response )
{
    string list_key = request->key();
    phxkv::ListRequest list_request = request->list_req();
    phxkv::ListResponse* list_response = response->mutable_list_response();
     
    return m_oPhxKVSM.GetKVClient()->ListRange(&list_request, list_response ,list_key  );
}
int PhxKV::ListRem(  const ::phxkv::Request* request  )//删除N个等值元素
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListSet(  const ::phxkv::Request* request  )//根据下表进行更新元素
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListSet SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListSet KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListTtim(  const ::phxkv::Request* request  )//保留指定区间元素，其余删除
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListRpush(  const ::phxkv::Request* request  )//尾插法
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ListRpushx(  const ::phxkv::Request* request )//链表存在时，才执行尾部插入
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_LIST_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ListLpush SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0)
    {
        ERROR_LOG("ListLpush KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC)//
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ListLpush KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
//////////============set====================================
int PhxKV::SAdd(  const ::phxkv::Request* request)
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.CopyFrom( * request );
    sub_mesage.set_data_type( Request_req_type_SET_REQ );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("SAdd SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("SAdd KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SAdd KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::SRem(  const ::phxkv::Request* request)
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_SET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("SRem SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("SRem KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SRem KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::SCard(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string set_key = request->key();
    phxkv::SetRequest set_request = request->set_req();
    phxkv::SetResponse* set_response = response->mutable_set_response();
    uint64_t length=0;
    int ret=  m_oPhxKVSM.GetKVClient()->SCard(&set_request, set_response ,set_key ,length );
    response->set_length(length );
    return ret;
}
int PhxKV::SMembers(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string set_key = request->key();
    phxkv::SetRequest set_request = request->set_req();
    phxkv::SetResponse* set_response = response->mutable_set_response();
    return m_oPhxKVSM.GetKVClient()->SMembers(&set_request, set_response ,set_key);
}
int PhxKV::SUnionStore(  const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_SET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("SUnionStore SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("SUnionStore KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SUnionStore KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::SUnion(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string set_key = request->key();
    phxkv::SetRequest set_request = request->set_req();
    phxkv::SetResponse* set_response = response->mutable_set_response();
    return m_oPhxKVSM.GetKVClient()->SUnion(&set_request, set_response );
}
int PhxKV::SInterStore(  const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_SET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("SInterStore SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("SInterStore KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SInterStore KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::SInter(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string set_key = request->key();
    phxkv::SetRequest set_request = request->set_req();
    phxkv::SetResponse* set_response = response->mutable_set_response();
    return m_oPhxKVSM.GetKVClient()->SInter(&set_request, set_response );
}
int PhxKV::SDiffStore(  const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_SET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("SDiffStore SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("SDiffStore KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SDiffStore KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::SDiff(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string set_key = request->key();
    phxkv::SetRequest set_request = request->set_req();
    phxkv::SetResponse* set_response = response->mutable_set_response();
    return m_oPhxKVSM.GetKVClient()->SDiff(&set_request, set_response );
}
int PhxKV::SIsMember(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string set_key = request->key();
    phxkv::SetRequest set_request = request->set_req();
    phxkv::SetResponse* set_response = response->mutable_set_response();
    int ret = m_oPhxKVSM.GetKVClient()->SIsMember(&set_request, set_response ,set_key);
    if( KVCLIENT_OK == ret ){
        response->set_exist( true );
    }else{
        response->set_exist( false );
    }
    return ret;
}
int PhxKV::SPop(  const ::phxkv::Request* request, ::phxkv::Response* response )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_SET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("SPop SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("SPop KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        string ret_value = oPhxKVSMCtx.sReadValue;
        phxkv::SetResponse *res = response->mutable_set_response();
        res->add_field()->assign( ret_value.data() ,ret_value.length() );
        
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SPop KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::SRandMember(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string set_key = request->key();
    phxkv::SetRequest set_request = request->set_req();
    phxkv::SetResponse* set_response = response->mutable_set_response();
    return m_oPhxKVSM.GetKVClient()->SRandMember(&set_request, set_response ,set_key);
}
int PhxKV::SMove(  const ::phxkv::Request* request)
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_SET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("SMove SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("SMove KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SMove KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}

//===========================
int PhxKV::ZAdd( const ::phxkv::Request* request )//一个或多个
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_ZSET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ZAdd SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("ZAdd KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {   
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SPop KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ZCard( const ::phxkv::Request* request, ::phxkv::Response* response)//元素数量
{
    string zset_key = request->key();
    phxkv::ZsetRequest zset_request = request->zset_req();
   
    uint64_t length=0;
    int ret= m_oPhxKVSM.GetKVClient()->ZCard(&zset_request, length,zset_key );
    response->set_length( length );
    return ret;

}
int PhxKV::ZCount( const ::phxkv::Request* request, ::phxkv::Response* response)//有序集合中，在区间内的数量
{
    string zset_key = request->key();
    phxkv::ZsetRequest zset_request = request->zset_req();
     uint64_t length=0;
    int ret = m_oPhxKVSM.GetKVClient()->ZCount(&zset_request, length,zset_key );
    response->set_length(length );
    return ret;
}
int PhxKV::ZRange( const ::phxkv::Request* request, ::phxkv::Response* response)//有序集合区间内的成员
{
    string zset_key = request->key();
    phxkv::ZsetRequest zset_request = request->zset_req();
    phxkv::ZsetResponse* set_response = response->mutable_zset_response();
    return m_oPhxKVSM.GetKVClient()->ZRange(&zset_request, set_response,zset_key );
}
int PhxKV::ZIncrby( const ::phxkv::Request* request, ::phxkv::Response* response)//为score增加或减少
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_ZSET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ZAdd SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("ZAdd KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {   
        ZsetResponse * res=response->mutable_zset_response();
        int64_t score=0;
        kv_encode::DecodeZSetFieldValue(oPhxKVSMCtx.sReadValue ,score  );
        res->set_mem_score( score );
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SPop KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ZUnionStore( const ::phxkv::Request* request )//并集不输出
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_ZSET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ZAdd SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("ZAdd KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {   
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SPop KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ZInterStore( const ::phxkv::Request* request )//交集不输出
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_ZSET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ZInterStore SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("ZInterStore KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {   
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ZInterStore KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ZRangebyscore( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string zset_key = request->key();
    phxkv::ZsetRequest zset_request = request->zset_req();
    phxkv::ZsetResponse* set_response = response->mutable_zset_response();
    return m_oPhxKVSM.GetKVClient()->ZRangebyscore(&zset_request, set_response,zset_key );
}
int PhxKV::ZRem( const ::phxkv::Request* request )//移除有序集 key 中的一个或多个成员
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_ZSET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ZAdd SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("ZAdd KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {   
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SPop KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ZRank( const ::phxkv::Request* request, ::phxkv::Response* response)//返回member的排名，
{
    string zset_key = request->key();
    phxkv::ZsetRequest zset_request = request->zset_req();
    phxkv::ZsetResponse* zset_response = response->mutable_zset_response();
    uint64_t rank_member = 0;
    int ret = m_oPhxKVSM.GetKVClient()->ZRank(&zset_request, rank_member,zset_key );
    zset_response->set_mem_rank(rank_member );
    return ret;
}
int PhxKV::ZRevrank( const ::phxkv::Request* request, ::phxkv::Response* response)//member逆序排名
{
    string zset_key = request->key();
    phxkv::ZsetRequest zset_request = request->zset_req();
    phxkv::ZsetResponse* zset_response = response->mutable_zset_response();
    uint64_t  rank_member= 0 ;
    int ret = m_oPhxKVSM.GetKVClient()->ZRevrank(&zset_request, rank_member,zset_key );
    zset_response->set_mem_rank( rank_member );
    return ret;
}
int PhxKV::ZScore( const ::phxkv::Request* request, ::phxkv::Response* response)//member的score
{
    string zset_key = request->key();
    phxkv::ZsetRequest zset_request = request->zset_req();
    phxkv::ZsetResponse* set_response = response->mutable_zset_response();
    return m_oPhxKVSM.GetKVClient()->ZScore(&zset_request, set_response,zset_key );
}

int PhxKV::ZREVRange( const ::phxkv::Request* request, ::phxkv::Response* response)//指定区间成员，逆序输出
{
    string zset_key = request->key();
    phxkv::ZsetRequest zset_request = request->zset_req();
    phxkv::ZsetResponse* set_response = response->mutable_zset_response();
    return m_oPhxKVSM.GetKVClient()->ZREVRange(&zset_request, set_response,zset_key );
}
int PhxKV::ZREVRangebylscore( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    string zset_key = request->key();
    phxkv::ZsetRequest zset_request = request->zset_req();
    phxkv::ZsetResponse* set_response = response->mutable_zset_response();
    return m_oPhxKVSM.GetKVClient()->ZREVRangebylscore(&zset_request, set_response,zset_key );
}
int PhxKV::ZRemrangebyrank( const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_ZSET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ZRemrangebyscore SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("ZAdd KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {   
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SPop KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}
int PhxKV::ZRemrangebyscore( const ::phxkv::Request* request )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_ZSET_REQ );
    sub_mesage.CopyFrom( * request );
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ZRemrangebyscore SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    uint16_t groupid = request->groupid();
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("ZAdd KVBatchPropose error.hashkey=%s ", request->key().c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {   
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("SPop KVBatchPropose error.ret=%d",oPhxKVSMCtx.iExecuteRet);
        return oPhxKVSMCtx.iExecuteRet;
    }
}

//=============================================

PhxKVStatus PhxKV::DelKey(const int groupid, const std::string & sKey  )
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_DEL_KEY_REQ );
     
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("DelKey SerializeToString error.");
        return PhxKVStatus::FAIL;
    }
    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("DelKey KVBatchPropose error. =%s ", sKey.c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("DelKey KVBatchPropose error.");
        return PhxKVStatus::FAIL;;
    }
}
     
PhxKVStatus PhxKV::ExpireKey(const int groupid, const std::string & sKey,const uint64_t ttl)
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_RequestMsg );
    phxkv::Request& sub_mesage = *Message.mutable_requestopt();
    sub_mesage.set_data_type( Request_req_type_EXPIRE_REQ );
    
    string sPaxosValue;
    if( !Message.SerializeToString(&sPaxosValue) ){
        ERROR_LOG("ExpireKey SerializeToString error.");
        return PhxKVStatus::FAIL;
    }

    PhxKVSMCtx oPhxKVSMCtx;
    int ret = KVBatchPropose(groupid,"",   sPaxosValue, oPhxKVSMCtx);
    if (ret != 0) 
    {
        ERROR_LOG("ExpireKey KVBatchPropose error key=%s ",  sKey.c_str() );
        return PhxKVStatus::FAIL;
    }
    if (oPhxKVSMCtx.iExecuteRet == SUCC )
    {
        return PhxKVStatus::SUCC;
    }
    else
    {
        ERROR_LOG("ExpireKey KVBatchPropose error.");
        return PhxKVStatus::FAIL;;
    }
}

}

