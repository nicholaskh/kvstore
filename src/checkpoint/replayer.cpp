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
#include "replayer.h"
#include "phxpaxos/storage.h"
#include "sm_base.h"
#include "comm_include.h"
#include "config_include.h"
#include "cp_mgr.h"
#include "log.h"
namespace phxpaxos
{

Replayer :: Replayer(
    Config * poConfig, 
    SMFac * poSMFac, 
    LogStorage * poLogStorage, 
    CheckpointMgr * poCheckpointMgr)
    : m_poConfig(poConfig), 
    m_poSMFac(poSMFac), 
    m_oPaxosLog(poLogStorage), 
    m_poCheckpointMgr(poCheckpointMgr),
    m_bCanrun(false),
    m_bIsPaused(true),
    m_bIsEnd(false)
{
    CP_COST_MS.reset(g_pmetrics->NewGuage("checkpoint_cost_ms", "", {{"type", "checkpoint"}}));
}

Replayer :: ~Replayer()
{
}

void Replayer :: Stop()
{
    m_bIsEnd = true;
    join();
}

void Replayer :: Pause()
{
    m_bCanrun = false;
}

void Replayer :: Continue()
{
    m_bIsPaused = false;
    m_bCanrun = true;
}

const bool Replayer:: IsPaused() const
{
    return m_bIsPaused;
}

void Replayer :: run()
{
    int conf_hour = CConfig::Get()->backup.backup_hour;
    int conf_minut =CConfig::Get()->backup.backup_minute;
    INFO_LOG("Replayer Run..");
    while (true)
    {
        time_t now = time(0);
        tm *gmtm = gmtime(&now);
        //每天定时生成
        if( conf_hour ==gmtm->tm_hour+8  &&  gmtm->tm_min == conf_minut  ){

            CheckpointStatus::GetInstance()->SetBackFinished( false );//标记未开始
            //先记录一下
            
            uint64_t llCPInstanceID = m_poSMFac->GetCheckpointInstanceID( m_poConfig->GetMyGroupIdx() )+1 ;
            uint64_t llMaxChosenInstanceID = m_poCheckpointMgr->GetMaxChosenInstanceID();
            int ret = m_poCheckpointMgr->SetMinChosenInstanceID(llCPInstanceID );
            
            INFO_LOG("replay groupid=%d,cpid=min=%lu,maxix=%lu" , m_poConfig->GetMyGroupIdx(),llCPInstanceID , llMaxChosenInstanceID);
            
            //设置确定的值,后面拿来做比较删除
            CheckpointStatus::GetInstance()->SetGroupCpid(m_poConfig->GetMyGroupIdx(), llCPInstanceID );
            CheckpointStatus::GetInstance()->SetGroupMaxId( m_poConfig->GetMyGroupIdx(),llMaxChosenInstanceID );
            //CheckpointStatue::GetInstance()->SetGroupMinId(m_poConfig->GetMyGroupIdx(),llCPInstanceID );

            CheckpointStatus::GetInstance()->SetGroupFinishTag( m_poConfig->GetMyGroupIdx() );
            if(m_poConfig->GetMyGroupIdx() != 0 ){
        
                Time::MsSleep( 1000*65 );
                continue;
            }
            while( !CheckpointStatus::GetInstance()->GetGroupFinishTag() ){
                
                Time::MsSleep( 65 );
            }
            //Time::MsSleep( 1000*2 );
            uint64_t start_ms = Time::GetTimestampMS();
            bool bPlayRet = PlayOne(  );
            uint64_t end_ms = Time::GetTimestampMS();
            CP_COST_MS->SetValue( end_ms -start_ms );
            CheckpointStatus::GetInstance()->SetBackFinished( true );//标记
            
            if (bPlayRet)
            {
                INFO_LOG("Replayer Ok...");
            }
            else
            {
                ERROR_LOG("Replayer Error..");
            }
            Time::MsSleep( 1000*65 );
        }else{
            Time::MsSleep(1000*1);
        }
        
    }
}

bool Replayer :: PlayOne()
{
    bool bExecuteRet = m_poSMFac->ExecuteForCheckpoint( );
    if (!bExecuteRet)
    {
        ERROR_LOG("Checkpoint sm excute fail" );
    }else{
        INFO_LOG("Checkpoint finish...");
    }
    
    return bExecuteRet;
}

}


