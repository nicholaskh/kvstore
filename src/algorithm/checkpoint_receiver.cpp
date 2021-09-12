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

#include "checkpoint_receiver.h"
#include "comm_include.h"
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>  
#include <fstream>  
#include <sstream>  
using namespace std;

namespace phxpaxos
{

CheckpointReceiver :: CheckpointReceiver(Config * poConfig, LogStorage * poLogStorage) :
    m_poConfig(poConfig), m_poLogStorage(poLogStorage)
{
    Reset();
}

CheckpointReceiver :: ~CheckpointReceiver()
{
}

void CheckpointReceiver :: Reset()
{
    m_mapHasInitDir.clear();
    
    m_iSenderNodeID = nullnode;
     
}

int CheckpointReceiver :: NewReceiver(const nodeid_t iSenderNodeID )
{
    int ret = ClearCheckpointTmp();//清理旧CP数据
    if (ret != 0)
    {
        return ret;
    }
     
    /*ret = m_poLogStorage->ClearAllLog(m_poConfig->GetGroupCount());//清理状态信息
    if (ret != 0)
    {
        ERROR_LOG("ClearAllLog fail, GetGroupCount %d ret %d", 
                m_poConfig->GetMyGroupIdx(), ret);
        return ret;
    }*/
    
    m_mapHasInitDir.clear();

    m_iSenderNodeID = iSenderNodeID;
    
    return 0;
}

int CheckpointReceiver :: ClearCheckpointTmp()
{
    string sLogStoragePath = m_poLogStorage->GetLogStorageDirPath(m_poConfig->GetMyGroupIdx());
    //清空路径
    int ret = FileUtils::DeleteDirFile(sLogStoragePath);
            
    if (ret != 0)
    {
        ERROR_LOG("rm dir %s error!", sLogStoragePath.c_str());
        return ret;
    }
    INFO_LOG("rm dir %s done!", sLogStoragePath.c_str());

    return ret;
}

const bool CheckpointReceiver :: IsReceiverFinish(const nodeid_t iSenderNodeID )
{
    
    
    return false;
    
}


int CheckpointReceiver :: InitFilePath(const std::string & sFilePath, std::string & sFormatFilePath)
{
    DEBUG_LOG("START filepath %s", sFilePath.c_str());

    string sNewFilePath = "/" + sFilePath + "/";
    vector<std::string> vecDirList;

    std::string sDirName;
    for (size_t i = 0; i < sNewFilePath.size(); i++)
    {
        if (sNewFilePath[i] == '/')
        {
            if (sDirName.size() > 0)
            {
                vecDirList.push_back(sDirName);
            }

            sDirName = "";
        }
        else
        {
            sDirName += sNewFilePath[i];
        }
    }

    sFormatFilePath = "";
    if (vecDirList.size() > 0 && vecDirList[0].size() > 0 && vecDirList[0][0] != '.')
    {
        sFormatFilePath += "/";
    }

    for (size_t i = 0; i < vecDirList.size(); i++)
    {
        if (i + 1 == vecDirList.size())
        {
            sFormatFilePath += vecDirList[i];
        }
        else
        {
            sFormatFilePath += vecDirList[i] + "/";
            if (m_mapHasInitDir.find(sFormatFilePath) == end(m_mapHasInitDir))
            {
                int ret = CreateDir(sFormatFilePath);
                if (ret != 0)
                {
                    return ret;
                }

                m_mapHasInitDir[sFormatFilePath] = true;
            }
        }
    }

    INFO_LOG("ok, format filepath %s", sFormatFilePath.c_str());

    return 0;
}

int CheckpointReceiver :: CreateDir(const std::string & sDirPath)
{
    if (access(sDirPath.c_str(), F_OK) == -1)
    {
        if (mkdir(sDirPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1)
        {       
            ERROR_LOG("Create dir fail, path %s", sDirPath.c_str());
            return -1;
        }       
    }

    return 0;
}

int CheckpointReceiver :: ReceiveCheckpoint(const CheckpointMsg & oCheckpointMsg)
{

    if (oCheckpointMsg.nodeid() != m_iSenderNodeID)
    {
        ERROR_LOG("msg not valid, Msg.SenderNodeID %lu Receiver.SenderNodeID %lu  ",
                oCheckpointMsg.nodeid(), m_iSenderNodeID );
        return -2;
    }

    std::string filename = oCheckpointMsg.filepath() ;
    uint32_t checksum = oCheckpointMsg.checksum();
    filename=CConfig::Get()->kvdb.dataPath+"g0/" + filename;
    uint32_t recv_checksum =CheckSum( filename );
    if(checksum == recv_checksum ){
        INFO_LOG("checksum pass..");
        return 0;//接收通过
    }else{
        ERROR_LOG("checksum error..");
        return -1;//接收失败
    }
}
    

uint32_t CheckpointReceiver :: CheckSum(const std::string  &fileneme )
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


