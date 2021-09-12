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

#include "kvsm.h"
#include "phxkv.pb.h"
#include "log.h"
#include <algorithm>
#include "sysconfig.h"
using namespace phxpaxos;
using namespace std;

namespace phxkv
{

PhxKVSM :: PhxKVSM(const std::string & sDBPath)
    : m_llCheckpointInstanceID(phxpaxos::NoCheckpoint), m_iSkipSyncCheckpointTimes(0)
{
    m_sDBPath = sDBPath;

    cp_busy = false;//处于忙状态
}

PhxKVSM :: ~PhxKVSM()
{
}

const bool PhxKVSM :: Init( phxpaxos::LogStorage* &logstore )
{
    
    m_oKVClient.Init(logstore);
     
    for(int i=0;i< CConfig::Get()->paxosnodes.group_cnt;i++  ){


        m_llCheckpointInstanceID = phxpaxos::NoCheckpoint;
        int ret = m_oKVClient.GetCheckpointInstanceID( i ,m_llCheckpointInstanceID);
        if (ret != 0 && ret != KVCLIENT_KEY_NOTEXIST)
        {
            ERROR_LOG("KVClient.GetCheckpointInstanceID fail, ret %d", ret);
            return false;
        }
        if (ret == KVCLIENT_KEY_NOTEXIST)
        {
            INFO_LOG("no checkpoint");
            m_llCheckpointInstanceID = phxpaxos::NoCheckpoint;
        }
        else
        {
            INFO_LOG("Groupid %d CheckpointInstanceID %lu", i , m_llCheckpointInstanceID);
        }
        map_CheckpointInstanceID[ i ]= m_llCheckpointInstanceID;
    }
    
    return true;
}

int PhxKVSM :: SyncCheckpointInstanceID(const int iGroupIdx,const uint64_t llInstanceID)
{
    if (m_iSkipSyncCheckpointTimes++ < 1000)
    {
        DEBUG_LOG("no need to sync checkpoint, skiptimes %d", m_iSkipSyncCheckpointTimes);
        return 0;
    }

    int ret = m_oKVClient.SetCheckpointInstanceID(iGroupIdx,llInstanceID);
    if (ret != 0)
    {
        ERROR_LOG("KVClient::SetCheckpointInstanceID fail, ret %d instanceid %lu", ret, llInstanceID);
        return ret;
    }
    DEBUG_LOG("ok, groupid=%lu checkpoint instanceid %lu",iGroupIdx, llInstanceID);

    map_CheckpointInstanceID[iGroupIdx]= llInstanceID;
    m_iSkipSyncCheckpointTimes = 0;

    return 0;
}

bool PhxKVSM :: Execute(const int iGroupIdx, const uint64_t llInstanceID, 
        const std::string & sPaxosValue, SMCtx * poSMCtx)
{
     
    
    phxkv::KVMessage Message;
    bool bSucc = Message.ParseFromArray(sPaxosValue.data(), sPaxosValue.size());
    if (!bSucc)
    {
        ERROR_LOG("oKVOper data wrong");
        return true;
    }
    
    const int msg_type = Message.type();//分解出是多值操作还是单值操作
    int iExecuteRet = -1;
    std::string ret_value = "";//用来写操作时的返回值
    
    if( msg_type == KVMessage_Type_SingleMsg ){ //单值操作
    
        if( !Message.has_putopt() ){
            ERROR_LOG("Execute error,no putopt");
            return false;
        }
        KVOperator* oKVOper = Message.mutable_putopt();
        
        if (oKVOper->operator_() == KVOperatorType_WRITE)
        { 
            iExecuteRet = m_oKVClient.Set(oKVOper->key(), oKVOper->value()  );
        }
        else
        {
            ERROR_LOG("unknown op %u", oKVOper->operator_());
            return true;
        }
    }else if( msg_type == KVMessage_Type_BatchMsg ){  // 多值操作
        
        if( !Message.has_batchputopt() ){
            ERROR_LOG("Execute error,no batchputopt.");
            return false;
        }
        KvBatchPutRequest* oBatchOper = Message.mutable_batchputopt();
        
        DEBUG_LOG("oBatchOper batchSet...type %d size %d", oBatchOper->operator_(),oBatchOper->subs_size() );
        if( oBatchOper->operator_() == KVOperatorType_WRITE ){
            
            iExecuteRet = m_oKVClient.batchSet( oBatchOper );            
        }
        else {
            ERROR_LOG("unknown op %u", oBatchOper->operator_());
            return true;
        }

    }else if( msg_type == KVMessage_Type_RequestMsg){
        ::phxkv::Request sub_request = Message.requestopt();
        int data_type = sub_request.data_type();
    
        if( data_type == Request_req_type_HASH_REQ ){                                      //hash 类型

            string hash_key = sub_request.key();//======================hashhash key
            ::phxkv::HashRequest* hash_req = sub_request.mutable_hash_req();
            int req_type =  hash_req->req_type();//写  删
            
            if(req_type == HashRequest_enum_req_HASH_DEL ){    // 删除

                iExecuteRet = m_oKVClient.HashDel( hash_req ,hash_key );
            }else if( req_type ==HashRequest_enum_req_HASH_SET  ){   //hash 写

                iExecuteRet = m_oKVClient.HashSet( hash_req,hash_key );
            }else if(req_type == HashRequest_enum_req_HASH_MSET ){     ///hash 写

                iExecuteRet = m_oKVClient.HashMset(hash_req,hash_key );
            }else if(req_type == HashRequest_enum_req_HASH_SETNX ){    ///hash 写

                iExecuteRet = m_oKVClient.HashSetNx(hash_req , hash_key );
            }else if(req_type ==HashRequest_enum_req_HASH_INCR_INT ){

                iExecuteRet = m_oKVClient.HashIncrByInt(hash_req ,hash_key ,ret_value);
            }else if(req_type ==HashRequest_enum_req_HASH_INCR_FLOAT ){
                iExecuteRet = m_oKVClient.HashIncrByFloat( hash_req ,hash_key ,ret_value);
            }else{
                ERROR_LOG("error req_type=%d..",req_type );
            }
        }else if(data_type == Request_req_type_LIST_REQ){
            
            const string list_key = sub_request.key();//======================list key
            ::phxkv::ListRequest* list_req = sub_request.mutable_list_req();
            int req_type =  list_req->req_type();//写  删
            INFO_LOG("phxkvsm execute.=%d" ,req_type );
            if( req_type == ListRequest_enum_req_LIST_INSERT ){    // 删除

                iExecuteRet = m_oKVClient.ListInsert( list_req ,list_key );
            }else if( req_type == ListRequest_enum_req_LIST_LPOP){
                
                iExecuteRet = m_oKVClient.ListLpop( list_req ,list_key,ret_value );
            }else if( req_type == ListRequest_enum_req_LIST_LPUSH){
                
                iExecuteRet = m_oKVClient.ListLpush( list_req ,list_key );
            }else if( req_type == ListRequest_enum_req_LIST_LPUSHX){
               
                iExecuteRet = m_oKVClient.ListLpushx( list_req ,list_key );
            }else if( req_type == ListRequest_enum_req_LIST_REM){
               
                iExecuteRet = m_oKVClient.ListRem( list_req ,list_key );
            }else if( req_type == ListRequest_enum_req_LIST_RPOP){
                
                iExecuteRet = m_oKVClient.ListRpop( list_req ,list_key,ret_value );
            }else if( req_type == ListRequest_enum_req_LIST_RPOP_LPUSH){
                
                iExecuteRet = m_oKVClient.ListRpopLpush( list_req ,list_key,ret_value );
            }else if( req_type == ListRequest_enum_req_LIST_RPUSH){
                
                iExecuteRet = m_oKVClient.ListRpush( list_req ,list_key );
            }else if( req_type == ListRequest_enum_req_LIST_RPUSHX ){
                
                iExecuteRet = m_oKVClient.ListRpushx( list_req ,list_key );
            }
            else if( req_type == ListRequest_enum_req_LIST_SET){
                
                iExecuteRet = m_oKVClient.ListSet( list_req ,list_key );
            }else if( req_type == ListRequest_enum_req_LIST_TRIM){
                
                iExecuteRet = m_oKVClient.ListTtim( list_req ,list_key );
            }else{
                ERROR_LOG("list cannot into");
            }  

        }else if(data_type == Request_req_type_SET_REQ  ){   //==================set==================
            const string set_key = sub_request.key();//======================hashhash key
            ::phxkv::SetRequest* set_req = sub_request.mutable_set_req();
            int req_type =  set_req->req_type();//写  删

            if( req_type == SetRequest_enum_req_SET_ADD ){    

                iExecuteRet = m_oKVClient.SAdd( set_req ,set_key );
            }else if( req_type ==SetRequest_enum_req_SET_DIFFSTORE  ){   //hash 写

                iExecuteRet = m_oKVClient.SDiffStore(  set_req ,set_key );
            }else if(req_type ==SetRequest_enum_req_SET_INTERSTORE ){

                 iExecuteRet = m_oKVClient.SInterStore(  set_req ,set_key );
            }else if(req_type ==SetRequest_enum_req_SET_MOVE ){

                 iExecuteRet = m_oKVClient.SMove(  set_req ,set_key );
            }else if(req_type ==SetRequest_enum_req_SET_POP ){

                 iExecuteRet = m_oKVClient.SPop(  set_req ,set_key,ret_value );
            }else if(req_type ==SetRequest_enum_req_SET_REM ){

                 iExecuteRet = m_oKVClient.SRem(  set_req ,set_key );
            }else if(req_type ==SetRequest_enum_req_SET_UNONSTORE ){

                 iExecuteRet = m_oKVClient.SUnionStore(  set_req ,set_key );
            }else{
                ERROR_LOG("cannot into .%d",req_type );
            }
        }else if(data_type == Request_req_type_ZSET_REQ  ){   //==================zset==================
            const string zset_key = sub_request.key(); 
            ::phxkv::ZsetRequest* zset_req = sub_request.mutable_zset_req();
            int req_type =  zset_req->req_type();//写  删

            if( req_type == ZsetRequest_enum_req_ZSET_ADD ){    

                iExecuteRet = m_oKVClient.ZAdd( zset_req ,zset_key );
            }else if( req_type == ZsetRequest_enum_req_ZSET_INCRBY ){    

                iExecuteRet = m_oKVClient.ZIncrby( zset_req ,zset_key,ret_value );
            }else if( req_type == ZsetRequest_enum_req_ZSET_INTERSTORE ){    

                iExecuteRet = m_oKVClient.ZInterStore( zset_req ,zset_key );
            }else if( req_type == ZsetRequest_enum_req_ZSET_REM ){    

                iExecuteRet = m_oKVClient.ZRem( zset_req ,zset_key );
            }else if( req_type == ZsetRequest_enum_req_ZSET_REM_RANGEBYRANK ){    

                iExecuteRet = m_oKVClient.ZRemrangebyrank( zset_req ,zset_key );
            }else if( req_type == ZsetRequest_enum_req_ZSET_REM_RANGEBYSCORE ){    

                iExecuteRet = m_oKVClient.ZRemrangebyscore( zset_req ,zset_key );
            }else if( req_type == ZsetRequest_enum_req_ZSET_UNIONSTORE ){    

                iExecuteRet = m_oKVClient.ZUnionStore( zset_req ,zset_key );
            }else{

                ERROR_LOG("cannot into.");
            }
        }else if( Request_req_type_EXPIRE_REQ == data_type){    //过期key
                iExecuteRet = m_oKVClient.ExpireKey( sub_request.key(), sub_request.ttl() );
        }else if( Request_req_type_DEL_KEY_REQ ==data_type ){        //删除key
                iExecuteRet = m_oKVClient.DelKey(sub_request.key() );
        }  
    }else{
        ERROR_LOG("error log..");
    }
    Message.Clear();
     
    if (iExecuteRet == KVCLIENT_SYS_FAIL)
    {
        ERROR_LOG("SM iExecuteRet=-1");
        return false;
    }
    else
    {
        if (poSMCtx != nullptr && poSMCtx->m_pCtx != nullptr)
        {
            PhxKVSMCtx * poPhxKVSMCtx = (PhxKVSMCtx *)poSMCtx->m_pCtx;
            poPhxKVSMCtx->iExecuteRet = iExecuteRet;
            poPhxKVSMCtx->sReadValue = ret_value;
            
        }
        SyncCheckpointInstanceID(iGroupIdx,llInstanceID);

        return true;
    }
}

////////////////////////////////////////////////////

bool PhxKVSM :: MakeOpValue(const std::string & sKey, const std::string & sValue,  const KVOperatorType iOp,std::string & sPaxosValue)
{
    phxkv::KVMessage Message;
    
    Message.mutable_putopt()->set_key(sKey);
    Message.mutable_putopt()->set_value(sValue);
    Message.mutable_putopt()->set_operator_(iOp);
    //Message.mutable_putopt()->set_ttl( ttl );
    Message.set_type( KVMessage_Type_SingleMsg );

    return Message.SerializeToString(&sPaxosValue);
}

bool PhxKVSM :: MakeGetOpValue(const std::string & sKey,std::string & sPaxosValue)
{
    return MakeOpValue(sKey, "", KVOperatorType_READ, sPaxosValue);
}

bool PhxKVSM :: MakeSetOpValue(const std::string & sKey, const std::string & sValue,  std::string & sPaxosValue)
{
    return MakeOpValue(sKey, sValue,  KVOperatorType_WRITE, sPaxosValue);
}

bool PhxKVSM :: MakeDelOpValue(const std::string & sKey, std::string & sPaxosValue)
{
    return MakeOpValue(sKey, "", KVOperatorType_DELETE, sPaxosValue);
}

bool PhxKVSM :: MakeBatchGetOpValue(const phxkv::KvBatchGetRequest & batchData,std::string & sPaxosValue)
{
    

    KvBatchGetRequest Data ;
    Data.CopyFrom( batchData );
    Data.set_operator_( KVOperatorType_READ );

    bool ret = Data.SerializeToString(&sPaxosValue);
    Data.Clear();
    return ret;
}
        
bool PhxKVSM :: MakeBatchSetOpValue(const phxkv::KvBatchPutRequest & batchData, std::string & sPaxosValue)
{
    phxkv::KVMessage Message;
    Message.set_type( KVMessage_Type_BatchMsg );
    phxkv::KvBatchPutRequest* bat_req = Message.mutable_batchputopt();
    bat_req->set_operator_( KVOperatorType_WRITE );

    for(int i=0;i< batchData.subs_size();i++  ){
         
        phxkv::KvBatchPutSubRequest* sub = bat_req->add_subs();
        
        sub->set_key( batchData.subs( i ).key() );
        sub->set_value( batchData.subs( i ).value() );
    }
    bool ret =  Message.SerializeToString(&sPaxosValue);
    Message.Clear();
    return ret;
}


KVClient * PhxKVSM :: GetKVClient()
{
    return &m_oKVClient;
}

//cp文件路径   文件名列表
int PhxKVSM :: GetCheckpointState(const int iGroupIdx, std::string & sDirPath, std::vector<std::string> & vecFileList) 
{ 
    sDirPath = CConfig::Get()->backup.backup_checkpoints_dir;//cp存放路径

    int ret = FileUtils::IterFile( sDirPath,vecFileList);

    INFO_LOG("CheckpointFileSize %d", vecFileList.size() );
    return 0;

}

const uint64_t PhxKVSM :: GetCheckpointInstanceID(const int iGroupIdx)  const{ 
    
    //return phxpaxos::NoCheckpoint;
    if(map_CheckpointInstanceID.count( iGroupIdx )<= 0  ){
        ERROR_LOG("iGroupIdx error %d ", iGroupIdx );
        return 0;
    }
    std::map< int, uint64_t>::const_iterator iter=map_CheckpointInstanceID.find(iGroupIdx);

    if( iter!=map_CheckpointInstanceID.end() ){
       return  iter->second;;
    }else{
        return phxpaxos::NoCheckpoint;
    }
        
}

bool PhxKVSM :: ExecuteForCheckpoint( )
{    
    std::string checkpoint_dir=CConfig::Get()->backup.backup_checkpoints_dir;
    return m_oKVClient.CreateCheckpoint( checkpoint_dir );   
}


}

