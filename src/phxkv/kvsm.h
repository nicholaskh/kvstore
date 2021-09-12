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

#include "phxpaxos/sm.h"
#include "kv.h"
#include <limits>
#include "def.h"

#include "log.h"
#include "phxpaxos/sm.h"
#include "kv.h"
#include <limits>
#include "def.h"
#include "paxos_msg.pb.h"
#include "log_store.h"
#include "utils_include.h"
#include "comm_include.h"

namespace phxkv
{

class PhxKVSMCtx
{
public:
    int iExecuteRet;
    std::string sReadValue;

    PhxKVSMCtx()
    {
        iExecuteRet = -1;
    }
};

////////////////////////////////////////////////

class PhxKVSM : public phxpaxos::StateMachine
{
public:
    PhxKVSM(const std::string & sDBPath);
    ~PhxKVSM();

    const bool Init(phxpaxos::LogStorage* &logstore);

    bool Execute(const int iGroupIdx, const uint64_t llInstanceID, 
            const std::string & sPaxosValue, phxpaxos::SMCtx * poSMCtx);

    const int SMID() const {return 1;}

public:
    //no use
    bool ExecuteForCheckpoint( );

    //have checkpoint.
    const uint64_t GetCheckpointInstanceID(const int iGroupIdx) const;

public:
    //have checkpoint, but not impl auto copy checkpoint to other node, so return fail.
    int LockCheckpointState() {           
        return 0;
    }
    
    int GetCheckpointState(const int iGroupIdx, std::string & sDirPath, 
            std::vector<std::string> & vecFileList) ;

    void UnLockCheckpointState() {
        return ;
     }
    
    int LoadCheckpointState(const int iGroupIdx, const std::string & sCheckpointTmpFileDirPath,
            const std::vector<std::string> & vecFileList, const uint64_t llCheckpointInstanceID) { return -1; }

public:
        //将操作key+value+opt 序列化成一个字符串
    static bool MakeOpValue(const std::string & sKey, const std::string & sValue,const KVOperatorType iOp,std::string & sPaxosValue);
        //序列化get操作
    static bool MakeGetOpValue(const std::string & sKey,std::string & sPaxosValue);
        //序列化set操作
    static bool MakeSetOpValue(const std::string & sKey, const std::string & sValue, std::string & sPaxosValue);
        //序列化del操作
    static bool MakeDelOpValue(const std::string & sKey,  std::string & sPaxosValue);
        
        //批量请求序列化
        //序列化batch get操作
    static bool MakeBatchGetOpValue(const phxkv::KvBatchGetRequest & batchData,std::string & sPaxosValue);
        //序列化batch set操作
    static bool MakeBatchSetOpValue(const phxkv::KvBatchPutRequest & batchData, std::string & sPaxosValue);

    

    KVClient * GetKVClient();

    int SyncCheckpointInstanceID(const int iGroupIdx,const uint64_t llInstanceID);

private:
    std::string m_sDBPath;
    KVClient m_oKVClient;
    
    uint64_t m_llCheckpointInstanceID;
    std::map< int, uint64_t> map_CheckpointInstanceID;
    int m_iSkipSyncCheckpointTimes;

    std::shared_ptr<std::mutex> _mutex;
    
    
    bool cp_busy;
};
    
}
