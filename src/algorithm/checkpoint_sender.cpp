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
#include "sysconfig.h"
#include "checkpoint_sender.h"
#include "comm_include.h"
#include "learner.h"
#include "sm_base.h"
#include "cp_mgr.h"
#include "crc32.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "phxpaxos/breakpoint.h"
#include <fstream>  
namespace phxpaxos
{

CheckpointSender :: CheckpointSender(
    const nodeid_t iSendNodeID,
    Config * poConfig, 
    Learner * poLearner,
    SMFac * poSMFac, 
    CheckpointMgr * poCheckpointMgr) :
    m_iSendNodeID(iSendNodeID),
    m_poConfig(poConfig),
    m_poLearner(poLearner),
    m_poSMFac(poSMFac),
    m_poCheckpointMgr(poCheckpointMgr)
{
    m_bIsEnded = false;
    m_bIsEnd = false;
    m_bIsStarted = false;
}

CheckpointSender :: ~CheckpointSender()
{
}

void CheckpointSender :: Stop()
{
    if (m_bIsStarted && !m_bIsEnded)
    {
        m_bIsEnd = true;
        join();
    }
}

void CheckpointSender :: End()
{
    m_bIsEnd = true;
}

const bool CheckpointSender :: IsEnd() const
{
    return m_bIsEnded;
}

void CheckpointSender :: run()
{
    m_bIsStarted = true;
     

    //pause checkpoint replayer
    bool bNeedContinue = false;
    while (!m_poCheckpointMgr->GetReplayer()->IsPaused())
    {
        if (m_bIsEnd)
        {
            m_bIsEnded = true;
            return;
        }

        bNeedContinue = true;
        
        m_poCheckpointMgr->GetReplayer()->Pause();
        //PLGDebug("wait replayer paused.");
        Time::MsSleep(100);
    }

    int ret = LockCheckpoint();
    if (ret == 0)
    {
        //send
        SendCheckpoint();

        UnLockCheckpoint();
    }else{
        ERROR_LOG("LockCheckpoint error");
    }

    //continue checkpoint replayer
    if (bNeedContinue)
    {
        m_poCheckpointMgr->GetReplayer()->Continue();
    }

    INFO_LOG("Checkpoint.Sender [END]");
    m_bIsEnded = true;
}

int CheckpointSender :: LockCheckpoint()
{
    std::vector<StateMachine *> vecSMList = m_poSMFac->GetSMList();
    std::vector<StateMachine *> vecLockSMList;
    int ret = 0;
    for (auto & poSM : vecSMList)
    {
        ret = poSM->LockCheckpointState();
        if (ret != 0)
        {
            ERROR_LOG("LockCheckpointState error ");
            break;
        }

        vecLockSMList.push_back(poSM);
    }

    if (ret != 0)
    {
        for (auto & poSM : vecLockSMList)
        {
            poSM->UnLockCheckpointState();
        }
    }

    return ret;
}

void CheckpointSender :: UnLockCheckpoint()
{
    std::vector<StateMachine *> vecSMList = m_poSMFac->GetSMList();
    for (auto & poSM : vecSMList)
    {
        poSM->UnLockCheckpointState();
    }
}

void CheckpointSender :: SendCheckpoint()
{
    int ret = -1;
    for (int i = 0; i < 2; i++)
    {
        ret = m_poLearner->SendCheckpointBegin(
                m_iSendNodeID,
                m_poSMFac->GetCheckpointInstanceID(m_poConfig->GetMyGroupIdx()));
        if (ret != 0)
        {
            ERROR_LOG("SendCheckpointBegin fail, ret %d", ret);
            return;
        }
    }
    bool stat = CheckAckBegin(  );
    if( !stat ){
        ERROR_LOG("CheckAckBegin Error..");
        return ;
    }

    BP->GetCheckpointBP()->SendCheckpointBegin();

    std::vector<StateMachine *> vecSMList = m_poSMFac->GetSMList();
    for (auto & poSM : vecSMList)
    {
        if (poSM->SMID() == SYSTEM_V_SMID
                || poSM->SMID() == MASTER_V_SMID)
        {
            continue;
        }
        ret = SendCheckpointFofaSM(poSM);
        if (ret != 0)
        {
            return;
        }
    }

    ret = m_poLearner->SendCheckpointEnd(
            m_iSendNodeID,
            m_poSMFac->GetCheckpointInstanceID(m_poConfig->GetMyGroupIdx()));
    if (ret != 0)
    {
        ERROR_LOG("SendCheckpointEnd fail,ret %d",  ret);
    }
    
    BP->GetCheckpointBP()->SendCheckpointEnd();
}

int CheckpointSender :: SendCheckpointFofaSM(StateMachine * poSM)
{
    string sDirPath;
    std::vector<std::string> vecFileList;

    int ret = poSM->GetCheckpointState(m_poConfig->GetMyGroupIdx(), sDirPath, vecFileList);//所有的CP文件
    if (ret != 0)
    {
        ERROR_LOG("GetCheckpointState fail ret %d, smid %d", ret, poSM->SMID());
        return -1;
    }

    if (vecFileList.size() == 0)
    {
        ERROR_LOG("No Checkpoint, smid %d", poSM->SMID());
        return 0;
    }
   
    for (auto & sFilePath : vecFileList)
    {
        ret = SendFile(poSM, sDirPath, sFilePath);//发送一个文件
        if (ret != 0 )
        {
            ERROR_LOG("SendFile fail, ret %d smid %d", ret , poSM->SMID());
            return -1;
        } 
        
        if( !CheckAckFile( sFilePath.c_str()  ) ){
           ERROR_LOG("CheckAckFile fail  %s" , sFilePath.c_str() );
           return -1;
        }
    }

    INFO_LOG("END, send ok, smid %d filelistcount %zu", poSM->SMID(), vecFileList.size());
    return 0;
}

int CheckpointSender :: SendFile(const StateMachine * poSM, const std::string & sDirPath, const std::string & sFilePath)
{
    INFO_LOG("SendFile filepath %s",( sDirPath+sFilePath ).c_str());

    if (m_mapAlreadySendedFile.find(sFilePath) != end(m_mapAlreadySendedFile))
    {
        ERROR_LOG("file already send, filepath %s", sFilePath.c_str());
        return 0;
    }

    phxpaxos::NodeInfo nodeinfo( m_iSendNodeID );
    string rsyncip = nodeinfo.GetIP();
    int port =CConfig::Get()->dbrsync.rsync_port;

    RsyncService::RsyncRemote remote(rsyncip ,port );
    int ret = global_rsync->RsyncSendFile(sDirPath + sFilePath,remote);

    if(0!= ret ){
        ERROR_LOG("CheckpointSender %s error", sFilePath.c_str() );
        return -1;
    }
    
    uint32_t iChecksum = CheckSum( sDirPath +sFilePath );
    if( iChecksum == 0 ){
        ERROR_LOG("CheckSum %s error", sFilePath.c_str() );
        return -1;
    }

    ret = m_poLearner->SendCheckpoint(//告诉对方，本文件的教研和。
                m_iSendNodeID, iChecksum, sFilePath, poSM->SMID());
    if( 0!= ret ){
        ERROR_LOG("SendCheckpoint error..");
        return ret;
    }

    m_mapAlreadySendedFile[sFilePath] = true;
    
    return ret;
}


void CheckpointSender :: Ack(const nodeid_t iSendNodeID, const string& filepath)
{
    INFO_LOG("CheckpointSender Ack=%s",filepath.c_str() );
    if (iSendNodeID != m_iSendNodeID)
    {
        ERROR_LOG("send nodeid not same, ack.sendnodeid %lu self.sendnodeid %lu", iSendNodeID, m_iSendNodeID);
        return;
    }
    
    if( !filepath.empty() ){  //正常确认
    
        ack_filepath = filepath;
        INFO_LOG("Ack file .%s", filepath.c_str() );
    }else{  //对开始状态进行确认

        has_begin_send = true;
        INFO_LOG("Ack begin pass.");
    }
    
}

const bool CheckpointSender :: CheckAckBegin( )//校验开始的状态
{
     uint64_t StartTime = Time::GetSteadyClockMS();
     Time::MsSleep(20);
    while ( true  )
    {
       uint64_t EndTime = Time::GetSteadyClockMS();
        uint64_t llPassTime = EndTime -StartTime;
 
        if( has_begin_send ){

            INFO_LOG("CheckAckBegin ok" );
            has_begin_send = false; 
            return true;
        }
        if (llPassTime >= Checkpoint_ACK_TIMEOUT)
        {       
            ERROR_LOG("CheckAckBegin timeout");
            return false;
        }       
        Time::MsSleep(20);
    }

    return true;
}
const bool CheckpointSender :: CheckAckFile(const string& filepath)//从 0 开始确认
{
     uint64_t StartTime = Time::GetSteadyClockMS();
     Time::MsSleep(20);
    while ( true  )
    {
       uint64_t EndTime = Time::GetSteadyClockMS();
        uint64_t llPassTime = EndTime -StartTime;
 
        if( filepath ==ack_filepath){

            INFO_LOG("CheckAckFile ok, filepath %s ", filepath.c_str() );
              
            return true; 
        }
        if (llPassTime >= Checkpoint_ACK_TIMEOUT)
        {       
            ERROR_LOG("CheckAckFile timeout");
            return false;
        }       
        Time::MsSleep(20);
    }

    return true;
}
uint32_t CheckpointSender :: CheckSum(const std::string  &fileneme )
{
    std::ifstream t( fileneme.c_str() );  
    if( !t ){
        ERROR_LOG("checkpoint file not exist.. %s ", fileneme.c_str() );
               
        return 0;
    }
    std::stringstream buffer;  
        buffer << t.rdbuf();  
    string sBuffer(buffer.str());

    uint32_t iBufferChecksum = crc32(0, (const uint8_t *)sBuffer.data(), sBuffer.size(), 
                        NET_CRC32SKIP);
    return iBufferChecksum;

}


}


