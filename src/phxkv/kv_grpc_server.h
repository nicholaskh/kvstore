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
#include "kv_hash.h"
#include "kv_list.h"
#include "kv_set.h"
#include "kv_zset.h"
#include <grpc++/grpc++.h>
#include "phxkv.grpc.pb.h"
#include "kv_paxos.h"
#include "metrics.h"
#include "kv_encode.h"
namespace phxkv
{
class PhxKVServiceImpl final : public PhxKVServer::Service 
{
public:
    PhxKVServiceImpl(const phxpaxos::NodeInfo & oMyNode, const phxpaxos::NodeInfoList & vecNodeList,
            const std::string & sKVDBPath, const std::string & sPaxosLogPath);

    int Init();
    ~PhxKVServiceImpl();
    void initPaxosBucket();//初始化paxos监控项
    void initGrpcBucket();//初始化grpc监控项
    void initTotalBucket();//用来统计监控所有的请求
    grpc::Status KvPut(grpc::ServerContext* context, const KVOperator * request, KVResponse * reply) override;

    grpc::Status KvGetLocal(grpc::ServerContext* context, const KVOperator * request, KVResponse * reply) override;

    grpc::Status KvGet(grpc::ServerContext* context, const KVOperator * request, KVResponse * reply) override;

    grpc::Status KvDelete(grpc::ServerContext* context, const KVOperator * request, KVResponse * reply) override;


    //批量操作
    grpc::Status KvBatchPut(::grpc::ServerContext* context, const KvBatchPutRequest* request, KvBatchPutResponse* response);
    grpc::Status KvBatchGet(::grpc::ServerContext* context, const KvBatchGetRequest* request, KvBatchGetResponse* response);

    //批量拉取集群信息
    grpc::Status KvGetgroupList(::grpc::ServerContext* context, const GroupListReq* request, GroupListRes* response);
    
    grpc::Status GetgroupList( GroupsMapMsg& response);
    

    //  以下与master漂移相关
    grpc::Status KvDropMaster(::grpc::ServerContext* context, const DropMastReq* request, DropMastRes* response);
    grpc::Status KvBeMaster(::grpc::ServerContext* context, const BemasterReq* request, BemasterRes* response);

    PhxKV& GetPhxKV( );

    bool CheckGroupid(const uint32_t groupid);
    
    grpc::Status HashOperate(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response)override;;
    grpc::Status ListOperate(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response);
    grpc::Status SetOperate(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response);
    grpc::Status ZsetOperate(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response);
    grpc::Status DelKey( ::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response);
    grpc::Status ExpireKey(::grpc::ServerContext* context, const ::phxkv::Request* request, ::phxkv::Response* response);
    

private:
    PhxKV m_oPhxKV;
    phxpaxos::Node * paxosNode;
    phxkv::KvHash* kv_hash;
    phxkv::KvList* kv_list;
    phxkv::KvSet* kv_set;
    phxkv::KvZset* kv_zset;

    int m_groupCnt ;
    //监控按分组统计
    std::vector<std::shared_ptr<TimerMetric> > vec_kvPutOkTimer;
    std::vector<std::shared_ptr<TimerMetric> > vec_kvPutErrTimer;
    std::vector<std::shared_ptr<TimerMetric> > vec_kvGetOkTimer;
    std::vector<std::shared_ptr<TimerMetric> > vec_kvGetErrTimer;
    //统计所有
    std::shared_ptr<TimerMetric> Total_KvPutOkTimer;
    std::shared_ptr<TimerMetric> Total_KvPutErrTimer;

    std::shared_ptr<TimerMetric> Total_KvGetOkTimer;
    std::shared_ptr<TimerMetric> Total_KvGetErrTimer;

    ////////////统计paxosmsg//////////////
    std::shared_ptr<TimerMetric> Count_PaxosPrepare;  //  1
    std::shared_ptr<TimerMetric> Count_PaxosPrepareReply;
    std::shared_ptr<TimerMetric> Count_PaxosAccept;
    std::shared_ptr<TimerMetric> Count_PaxosAcceptReply;
    std::shared_ptr<TimerMetric> Count_PaxosLearner_AskforLearn;
    std::shared_ptr<TimerMetric> Count_PaxosLearner_SendLearnValue;
    std::shared_ptr<TimerMetric> Count_PaxosLearner_ProposerSendSuccess;
    std::shared_ptr<TimerMetric> Count_PaxosProposal_SendNewValue;
    std::shared_ptr<TimerMetric> Count_PaxosLearner_SendNowInstanceID;
    std::shared_ptr<TimerMetric> Count_PaxosLearner_ComfirmAskforLearn;
    std::shared_ptr<TimerMetric> Count_PaxosLearner_SendLearnValue_Ack;
    std::shared_ptr<TimerMetric> Count_PaxosLearner_AskforCheckpoint;
    std::shared_ptr<TimerMetric> Count_PaxosLearner_OnAskforCheckpoint;
    std::shared_ptr<TimerMetric> Count_Checkpoint_SendFile;
    std::shared_ptr<TimerMetric> Count_Checkpoint_SendFile_Ack;  // 15

    std::vector< std::shared_ptr<TimerMetric> > vec_KvPaxosCount;

};

}
