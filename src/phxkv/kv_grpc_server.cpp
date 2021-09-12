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
#include <stdio.h>
#include <sstream>
#include "kv_grpc_server.h"
#include "util.h"
#include "kv_grpc_server.h"
#include "def.h"
#include "sysconfig.h"
#include "log.h"
#include <stdio.h>
#include <chrono>
#include <ctime>
#include "grpcpp/resource_quota.h"
#include "commdef.h"
//#include "commdef.h"
using grpc::ServerContext;
using grpc::Status;
using namespace std;
int global_cp = 0;
namespace phxkv
{


PhxKVServiceImpl :: PhxKVServiceImpl(const phxpaxos::NodeInfo & oMyNode, const phxpaxos::NodeInfoList & vecNodeList,
        const std::string & sKVDBPath, const std::string & sPaxosLogPath)
    : m_oPhxKV(oMyNode, vecNodeList, sKVDBPath, sPaxosLogPath)
{
    m_groupCnt =CConfig::Get()->paxosnodes.group_cnt;//32;
    initPaxosBucket();
    initTotalBucket();
    initGrpcBucket();
    kv_hash =NULL ;
    kv_list= NULL ;
    kv_set=NULL;
    kv_zset=NULL;
}
PhxKVServiceImpl::~PhxKVServiceImpl()
{
    if( kv_hash !=NULL){
        delete kv_hash ;
    }
    if( kv_list !=NULL){
        delete kv_list ;
    }
    if( kv_set !=NULL){
        delete kv_set ;
    }
    if( kv_zset !=NULL){
        delete kv_zset ;
    }
}
int PhxKVServiceImpl :: Init()
{
    m_oPhxKV.RunPaxos();
    kv_hash = new KvHash( m_oPhxKV );
    kv_list = new KvList( m_oPhxKV );
    kv_set = new KvSet( m_oPhxKV );
    kv_zset = new KvZset( m_oPhxKV );
    paxosNode =  m_oPhxKV.GetPaxosNode();
    return 0;
}
void PhxKVServiceImpl :: initPaxosBucket()
{
    Count_PaxosPrepare.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "Prepare" }}));
    vec_KvPaxosCount.push_back( Count_PaxosPrepare );
    Count_PaxosPrepareReply.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "PrepareReply" }}));
    vec_KvPaxosCount.push_back( Count_PaxosPrepareReply );
    Count_PaxosAccept.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "Accept" }}));
    vec_KvPaxosCount.push_back( Count_PaxosAccept );
    Count_PaxosAcceptReply.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "AcceptReply" }}));
    vec_KvPaxosCount.push_back( Count_PaxosAcceptReply );
    Count_PaxosLearner_AskforLearn.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "AskforLearn" }}));;
    vec_KvPaxosCount.push_back( Count_PaxosLearner_AskforLearn );
    Count_PaxosLearner_SendLearnValue.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "SendLearnValue" }}));;
    vec_KvPaxosCount.push_back( Count_PaxosLearner_SendLearnValue );
    Count_PaxosLearner_ProposerSendSuccess.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "ProposerSendSuccess" }}));;
    vec_KvPaxosCount.push_back( Count_PaxosLearner_ProposerSendSuccess );
    Count_PaxosProposal_SendNewValue.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "SendNewValue" }}));;
    vec_KvPaxosCount.push_back( Count_PaxosProposal_SendNewValue );
    Count_PaxosLearner_SendNowInstanceID.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "SendNowInstanceID" }}));;
    vec_KvPaxosCount.push_back( Count_PaxosLearner_SendNowInstanceID );
    Count_PaxosLearner_ComfirmAskforLearn.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "ComfirmAskforLearn" }}));;
    vec_KvPaxosCount.push_back( Count_PaxosLearner_ComfirmAskforLearn );
    Count_PaxosLearner_SendLearnValue_Ack.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "SendLearnValue_Ack" }}));;
    vec_KvPaxosCount.push_back( Count_PaxosLearner_SendLearnValue_Ack );
    Count_PaxosLearner_AskforCheckpoint.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "AskforCheckpoint" }}));;
    vec_KvPaxosCount.push_back( Count_PaxosLearner_AskforCheckpoint );
    Count_PaxosLearner_OnAskforCheckpoint.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "OnAskforCheckpoint" }}));;
    vec_KvPaxosCount.push_back( Count_PaxosLearner_OnAskforCheckpoint );
    Count_Checkpoint_SendFile.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "SendFile" }}));;
    vec_KvPaxosCount.push_back( Count_Checkpoint_SendFile );
    Count_Checkpoint_SendFile_Ack.reset(g_pmetrics->NewTimer("count_paxos_msg", "", {{"type", "SendFile_Ack" }}));;  // 15
    vec_KvPaxosCount.push_back( Count_Checkpoint_SendFile_Ack );
}
void PhxKVServiceImpl :: initGrpcBucket()
{
   
    for(int k=0;k< m_groupCnt;k++ ){
        stringstream ss;
        ss<<k;
         
        std::shared_ptr<TimerMetric> metric_ptr_ok( g_pmetrics->NewTimer("kvnode_server_grpc", "", {{"op", "kvput"}, {"ret", "ok"},{"groupid", ss.str() }}) ) ;
        vec_kvPutOkTimer.push_back( metric_ptr_ok );


        std::shared_ptr<TimerMetric> metric_ptr_error( g_pmetrics->NewTimer("kvnode_server_grpc", "", {{"op", "kvput"}, {"ret", "err"},{"groupid", ss.str() }}) ) ;
        vec_kvPutErrTimer.push_back( metric_ptr_error );
    }

    for(int k=0;k< m_groupCnt;k++ ){
        stringstream ss;
        ss<<k;
        std::shared_ptr<TimerMetric> metric_ptr_ok( g_pmetrics->NewTimer("kvnode_server_grpc", "", {{"op", "kvget"}, {"ret", "ok"},{"groupid", ss.str() }}) ) ;
        vec_kvGetOkTimer.push_back( metric_ptr_ok );

        std::shared_ptr<TimerMetric> metric_ptr_error( g_pmetrics->NewTimer("kvnode_server_grpc", "", {{"op", "kvget"}, {"ret", "err"},{"groupid", ss.str() }}) ) ;
        vec_kvGetErrTimer.push_back( metric_ptr_error );
    }
}
void PhxKVServiceImpl :: initTotalBucket()
{
    Total_KvPutOkTimer.reset( g_pmetrics->NewTimer("total_server_grpc", "", {{"op", "kvput"}, {"ret", "ok"} } ));;
    Total_KvPutErrTimer.reset( g_pmetrics->NewTimer("total_server_grpc", "", {{"op", "kvput"}, {"ret", "err"}} ));

    Total_KvGetOkTimer.reset( g_pmetrics->NewTimer("total_server_grpc", "", {{"op", "kvget"}, {"ret", "ok"} }));;
    Total_KvGetErrTimer.reset( g_pmetrics->NewTimer("total_server_grpc", "", {{"op", "kvget"}, {"ret", "err"}} ));;
}
Status PhxKVServiceImpl :: KvPut(ServerContext* context, const KVOperator * request, KVResponse * reply)
{
    if( request->groupid() >= CConfig::Get()->paxosnodes.group_cnt ){

        ERROR_LOG(" KvPut groupid error %d ", request->groupid() );
        reply->set_ret( -1 );
        return Status::OK;
    }
    if (!m_oPhxKV.IsMaster(request->groupid()))
    {
        reply->set_ret((int)PhxKVStatus::MASTER_REDIRECT);

        phxkv::GroupsMapMsg Msg;
        GetgroupList( Msg );

        phxkv::GroupsMapMsg* MapMsg = reply->mutable_submap();
        MapMsg->CopyFrom( Msg );

        INFO_LOG(" KvPut MASTER_REDIRECT ok  key %s ", request->key().c_str() );
        
        return Status::OK;
    }
    auto start = std::chrono::system_clock::now();
    PhxKVStatus status = m_oPhxKV.Put(request->groupid() , request->key(), request->value() );
     
    reply->set_ret((int)status);

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    if (status != PhxKVStatus::SUCC )
    {
        ERROR_LOG("KvPut ret %d, key %s groupid=%d", reply->ret(), request->key().c_str(),request->groupid() );
        vec_kvPutErrTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvPutErrTimer->Observe(elapsed_seconds.count());
    }else{
        INFO_LOG("KvPut ret %d, key %s groupid %d", reply->ret(), request->key().c_str(),request->groupid()  );
        vec_kvPutOkTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvPutOkTimer->Observe(elapsed_seconds.count());
    }
    
    return Status::OK;
}

Status PhxKVServiceImpl :: KvGetLocal(ServerContext* context, const KVOperator * request, KVResponse * reply)
{
    string sReadValue;
     
    auto start = std::chrono::system_clock::now();
    PhxKVStatus status = m_oPhxKV.GetLocal(request->key(), sReadValue);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;

    if (status == SUCC)
    {
        reply->set_data(sReadValue);
        vec_kvGetOkTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvGetOkTimer->Observe(elapsed_seconds.count());
    }
    else if (status == KEY_NOTEXIST)
    {
        vec_kvGetErrTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvGetErrTimer->Observe(elapsed_seconds.count());
    }

    reply->set_ret((int)status);
    
    INFO_LOG("GetLocal ret %d, key: %s value:%s ", reply->ret(), request->key().c_str() , sReadValue.c_str() );

    return Status::OK;
}

Status PhxKVServiceImpl :: KvGet(ServerContext* context, const KVOperator * request, KVResponse * reply)
{
     
    INFO_LOG(" KvGet.key %s",request->key().c_str() );
    if( request->groupid() >= CConfig::Get()->paxosnodes.group_cnt ){

        ERROR_LOG(" KvGet groupid error %d ", request->groupid() );
        reply->set_ret( -1 );
        return Status::OK;
    }
   if (!m_oPhxKV.IsMaster(request->groupid() )  )
    {
        reply->set_ret((int)MASTER_REDIRECT);
        
        phxkv::GroupsMapMsg Msg;
        GetgroupList( Msg );

        phxkv::GroupsMapMsg* MapMsg = reply->mutable_submap();
        MapMsg->CopyFrom( Msg );
        INFO_LOG(" KvGet  MASTER_REDIRECT ok  key %s ", request->key().c_str() );
        return Status::OK;
        
    }
    return KvGetLocal(context, request, reply);
}

Status PhxKVServiceImpl :: KvDelete(ServerContext* context, const KVOperator * request, KVResponse * reply)
{

    return Status::OK;
}

grpc::Status PhxKVServiceImpl :: KvBatchPut(::grpc::ServerContext* context, const ::phxkv::KvBatchPutRequest* request, ::phxkv::KvBatchPutResponse* reply)
{
    if( request->groupid() >= CConfig::Get()->paxosnodes.group_cnt ){

        ERROR_LOG(" KvBatchPut groupid error %d ", request->groupid() );
        reply->set_ret( -1 );
        return Status::OK;
    }

    int sub_size = request->subs_size();
    INFO_LOG("KvBatchPut size %d", sub_size );
    string first_key = request->subs(0).key();
    if (!m_oPhxKV.IsMaster( request->groupid() ) )
    {
        reply->set_ret((int)MASTER_REDIRECT);

        phxkv::GroupsMapMsg Msg;
        GetgroupList( Msg );

        phxkv::GroupsMapMsg* MapMsg = reply->mutable_submap();
        MapMsg->CopyFrom( Msg );

        INFO_LOG(" KvBatchPut  MASTER_REDIRECT ok firstkey %s ",  first_key.c_str() );
        
        return Status::OK;
    }
    auto start = std::chrono::system_clock::now();
    PhxKVStatus status = m_oPhxKV.BatchPut(request->groupid() , first_key, *request  );
     
    reply->set_ret((int)status);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    if (status != SUCC )
    {
        ERROR_LOG("KvBatchPut error ret %d, key size %d firstkey %s ", reply->ret(), sub_size, first_key.c_str());
        vec_kvPutErrTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvPutErrTimer->Observe(elapsed_seconds.count());
    }else{
        INFO_LOG("KvBatchPut ret %d, key size %d firstkey %s", reply->ret(), sub_size, first_key.c_str());
        vec_kvPutOkTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvPutOkTimer->Observe(elapsed_seconds.count());
    }

    return Status::OK;
}
grpc::Status PhxKVServiceImpl :: KvBatchGet(::grpc::ServerContext* context, const ::phxkv::KvBatchGetRequest* request, ::phxkv::KvBatchGetResponse* reply)
{
    if( request->groupid() >= CConfig::Get()->paxosnodes.group_cnt ){

        ERROR_LOG(" KvBatchGet groupid error %d ", request->groupid() );
        reply->set_ret( -1 );
        return Status::OK;
    }
    int sub_size = request->subs_size();
    INFO_LOG("KvBatchGet size %d", sub_size );
    string first_key = request->subs(0).key();
    if (!m_oPhxKV.IsMaster( request->groupid() ) )
    {
        reply->set_ret((int)MASTER_REDIRECT);

        phxkv::GroupsMapMsg Msg;
        GetgroupList( Msg );

        phxkv::GroupsMapMsg* MapMsg = reply->mutable_submap();
        MapMsg->CopyFrom( Msg );

        INFO_LOG(" KvBatchGet  MASTER_REDIRECT ok firstkey %s ", first_key.c_str() );
        
        return Status::OK;
    }
     
    auto start = std::chrono::system_clock::now();
    PhxKVStatus status = m_oPhxKV.BatchGet(request , reply );
     
    reply->set_ret((int)status);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    if (status != SUCC )
    {
        ERROR_LOG("KvBatchGet error ret %d, key size %d firstkey %s ", reply->ret(), sub_size, first_key.c_str());
        vec_kvGetErrTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvGetErrTimer->Observe(elapsed_seconds.count());
    }else{
        INFO_LOG("KvBatchGet ok ret %d, key size %d firstkey %s", reply->ret(), sub_size, first_key.c_str());
        vec_kvGetOkTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvGetOkTimer->Observe(elapsed_seconds.count());
    }

    return Status::OK;
}

grpc::Status PhxKVServiceImpl :: KvGetgroupList(::grpc::ServerContext* context, const ::phxkv::GroupListReq* request, ::phxkv::GroupListRes* response)
{
    const int group_cnt = request->groupcnt();
    int cong_group = CConfig::Get()->paxosnodes.group_cnt;
    if( group_cnt!= cong_group ){

        ERROR_LOG("group cnt not equal client %d : server %d" , group_cnt , cong_group);
        response->set_ret(-1);
        return Status::OK;
    }
    phxkv::GroupsMapMsg Msg;
    GetgroupList( Msg );

    phxkv::GroupsMapMsg* MapMsg = response->mutable_submap();
    MapMsg->CopyFrom( Msg );
    
    return Status::OK;
}

grpc::Status PhxKVServiceImpl :: GetgroupList(  ::phxkv::GroupsMapMsg& response)
{
    INFO_LOG("GetgroupList start.");
    int cong_group = CConfig::Get()->paxosnodes.group_cnt;
    for(int i=0;i<cong_group;i++  ){

        phxkv::GroupMsg* msg = response.add_subgroup();

        uint64_t llMasterNodeID = m_oPhxKV.GetMasterByGroup( i ).GetNodeID();
        if( llMasterNodeID== 0 ){
            msg->set_ret((int)NO_MASTER);
        }else{
            msg->set_groupid( i );
            msg->set_ret( 0 );
            phxpaxos::NodeInfo nodeinfo( llMasterNodeID );
            msg->set_masterip( nodeinfo.GetIP() );
            msg->set_masterport( CConfig::Get()->grpc.port );
            INFO_LOG("groupid %d ip %s port %d" ,i , nodeinfo.GetIP().c_str(),nodeinfo.GetPort( ) );
        }       
    }
    INFO_LOG("GetgroupList finish.");
    return Status::OK;
}



grpc::Status PhxKVServiceImpl :: KvDropMaster(::grpc::ServerContext* context, const ::phxkv::DropMastReq* request, ::phxkv::DropMastRes* response)
{
    const int cnt = request->cnt( );//丢的个数
    const int cong_group = CConfig::Get()->paxosnodes.group_cnt;//总个数

    int has_drop=0;
    for(int i=0;i< cong_group ;i++ ){

        if( paxosNode->IsIMMaster( i ) ){

            paxosNode->DropMaster(i );
            has_drop++;
            INFO_LOG("drop groupid=[%d] ok", i);
        }
        if( has_drop == cnt ){
           
            break;
        }
    }
    response->set_ret( has_drop );//返回丢掉的个数
    return Status::OK;
}


grpc::Status PhxKVServiceImpl :: KvBeMaster(::grpc::ServerContext* context, const ::phxkv::BemasterReq* request, ::phxkv::BemasterRes* response)
{
    int ret = -1;
    const int cong_group = CConfig::Get()->paxosnodes.group_cnt;//总个数
    const int cnt = cong_group/3 + 1;//抢这么多个
    int has_cnt=0;
    for(int i=0;i< cong_group; i++ ){

        if( !paxosNode->IsIMMaster( i ) ){

            paxosNode->BeMaster( i , true );
            has_cnt++;
        }
        if(has_cnt == cnt ){
            ret = 0;
            break;
        }
    }

    response->set_ret( ret );
    return Status::OK;
}

PhxKV& PhxKVServiceImpl :: GetPhxKV( )
{
    return m_oPhxKV;
}

bool PhxKVServiceImpl :: CheckGroupid(const uint32_t groupid)
{
    if( groupid >= CConfig::Get()->paxosnodes.group_cnt ){
        return false;
    }
    return true;
}

grpc::Status PhxKVServiceImpl :: HashOperate(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response)
{
    INFO_LOG("HashOperate start");
    const uint32_t groupid  = request->groupid();
    const string hash_key = request->key();
    if( !CheckGroupid( groupid )){
        ERROR_LOG(" HashOperate groupid hash_key %s ", hash_key.c_str() );
        response->set_ret_code( Response_enum_code_PARAM_ERROR );
        return Status::OK;
    }

    int data_type = request->data_type();//是不是hash
    INFO_LOG("data_type=%d",data_type );
    if(data_type != Request_req_type_HASH_REQ ){
        response->set_ret_code( Response_enum_code_PARAM_ERROR );
        return Status::OK;
    }
    kv_hash->HashMsg(request ,response );
    INFO_LOG("HashOperate end");
    return Status::OK;
}

grpc::Status PhxKVServiceImpl :: ListOperate(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response)
{
    INFO_LOG("ListOperate start");
    const uint32_t groupid  = request->groupid();
    const string list_key = request->key();
    if( !CheckGroupid( groupid )){
        ERROR_LOG(" ListOperate groupid list_key %s ", list_key.c_str() );
        response->set_ret_code( Response_enum_code_PARAM_ERROR );
        return Status::OK;
    }

    int data_type = request->data_type();//是不是
    INFO_LOG("data_type=%d",data_type );
    if(data_type != Request_req_type_LIST_REQ ){
        response->set_ret_code( Response_enum_code_PARAM_ERROR );
        return Status::OK;
    }
    kv_list->ListMsg(request ,response );
    INFO_LOG("ListOperate end");
    return Status::OK;
}
grpc::Status PhxKVServiceImpl :: SetOperate(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response)
{
    INFO_LOG("SetOperate start");
    const uint32_t groupid  = request->groupid();
    const string hash_key = request->key();
    if( !CheckGroupid( groupid )){
        ERROR_LOG(" SetOperate groupid hash_key %s ", hash_key.c_str() );
        response->set_ret_code( Response_enum_code_PARAM_ERROR );
        return Status::OK;
    }

    int data_type = request->data_type();//是不是
    INFO_LOG("data_type=%d",data_type );
    if(data_type != Request_req_type_SET_REQ ){
        response->set_ret_code( Response_enum_code_PARAM_ERROR );
        return Status::OK;
    }
    kv_set->SetMsg(request ,response );
    INFO_LOG("SetOperate end");
    return Status::OK;
}

grpc::Status PhxKVServiceImpl :: ZsetOperate(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response)
{
    INFO_LOG("ZsetOperate start");
    const uint32_t groupid  = request->groupid();
    const string hash_key = request->key();
    if( !CheckGroupid( groupid )){
        ERROR_LOG(" ZsetOperate groupid hash_key %s ", hash_key.c_str() );
        response->set_ret_code( Response_enum_code_PARAM_ERROR );
        return Status::OK;
    }

    int data_type = request->data_type();//是不是
    INFO_LOG("data_type=%d",data_type );
    if(data_type != Request_req_type_ZSET_REQ ){
        response->set_ret_code( Response_enum_code_PARAM_ERROR );
        return Status::OK;
    }
    kv_zset->ZsetMsg(request ,response );
    INFO_LOG("ZsetOperate end");
    return Status::OK;
}

grpc::Status PhxKVServiceImpl :: DelKey( ::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* reply)
{
    //类型判断
    if( request->groupid() >= CConfig::Get()->paxosnodes.group_cnt ){
        ERROR_LOG(" DelKey groupid error %d ", request->groupid() );
        reply->set_ret_code( Response_enum_code_PARAM_ERROR );
        return Status::OK;
    }
    if (!m_oPhxKV.IsMaster(request->groupid()))
    {
        reply->set_ret_code( Response_enum_code_RES_MASTER_REDIRECT );
        phxkv::GroupsMapMsg Msg;
        GetgroupList( Msg );
        phxkv::GroupsMapMsg* MapMsg = reply->mutable_submap();
        MapMsg->CopyFrom( Msg );
        INFO_LOG(" DelKey MASTER_REDIRECT ok  key  "  );
        return Status::OK;
    }
    auto start = std::chrono::system_clock::now();
    PhxKVStatus status = m_oPhxKV.DelKey(request->groupid() , request->key()  );
     
    reply->set_ret_code((::phxkv::Response_enum_code)status);

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    if (status != PhxKVStatus::SUCC )
    {
        ERROR_LOG("DelKey ret %d, key %s groupid=%d", status, request->key().c_str(),request->groupid() );
        vec_kvPutErrTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvPutErrTimer->Observe(elapsed_seconds.count());
    }else{
        INFO_LOG("DelKey ret %d, key %s groupid %d",status, request->key().c_str(),request->groupid()  );
        vec_kvPutOkTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvPutOkTimer->Observe(elapsed_seconds.count());
    }
    return Status::OK;
}
grpc::Status PhxKVServiceImpl :: ExpireKey(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* reply)
{
    //类型判断
    if( request->groupid() >= CConfig::Get()->paxosnodes.group_cnt ){
        ERROR_LOG(" ExpireKey groupid error %d ", request->groupid() );
        reply->set_ret_code( Response_enum_code_PARAM_ERROR);
        return Status::OK;
    }
    if (!m_oPhxKV.IsMaster(request->groupid()))
    {
        reply->set_ret_code( Response_enum_code_RES_MASTER_REDIRECT );
        phxkv::GroupsMapMsg Msg;
        GetgroupList( Msg );
        phxkv::GroupsMapMsg* MapMsg = reply->mutable_submap();
        MapMsg->CopyFrom( Msg );

        INFO_LOG(" ExpireKey MASTER_REDIRECT ok  key  "  );
        return Status::OK;
    }
    auto start = std::chrono::system_clock::now();
    PhxKVStatus status = m_oPhxKV.ExpireKey(request->groupid() , request->key(),request->ttl()  );
     
    reply->set_ret_code( (::phxkv::Response_enum_code)status );

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    if (status != PhxKVStatus::SUCC )
    {
        ERROR_LOG("ExpireKey ret %d, key %s groupid=%d", status, request->key().c_str(),request->groupid() );
        vec_kvPutErrTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvPutErrTimer->Observe(elapsed_seconds.count());
    }else{
        INFO_LOG("ExpireKey ret %d, key %s groupid %d", status, request->key().c_str(),request->groupid()  );
        vec_kvPutOkTimer[request->groupid()]->Observe(elapsed_seconds.count());
        Total_KvPutOkTimer->Observe(elapsed_seconds.count());
    }
    return Status::OK;
}

}

 