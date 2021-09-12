#pragma once

#include <map>
#include "timer.h"
#include "utils_include.h"
#include <string>
#include "comm_include.h"
#include <queue>
#include "config_include.h"
#include "kv_paxos.h"

#define STATUS_ID_ROCKSDB 1000
#define STATUS_ID_NODECNT 1001

namespace phxpaxos
{


class Instance;

class StatusMng : public Thread
{
public:
    StatusMng(phxkv::PhxKV& oPhxKV);
    virtual ~StatusMng();

    void run();
    void stop();

public:
    bool AddTimer(const int iTimeout, const int iType, uint32_t & iTimerID);

    void RemoveTimer(uint32_t & iTimerID);

    void DealwithTimeout( );

    void DealwithTimeoutOne(const uint32_t iTimerID, const int iType);

private:
   
    Timer oTimer;
    std::map<uint32_t, bool> mapTimerIDExist;

    phxkv::PhxKV m_oPhxKV;

    int iTimeout;
    int iType;
    uint32_t iTimerID;
    bool falg;
};
    
}