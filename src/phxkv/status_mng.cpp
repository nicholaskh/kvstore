#include "status_mng.h"
#include "utils_include.h"
#include "instance.h"

using namespace std;

namespace phxpaxos
{

StatusMng :: StatusMng(phxkv::PhxKV& oPhxKV)
    : m_oPhxKV( oPhxKV )
{
    falg = true;
    int iTimeout_=5000;
    int iType_=0;
    uint32_t  iTimerID_ = STATUS_ID_ROCKSDB;
    AddTimer(iTimeout_, iType_,iTimerID_ );

}

StatusMng :: ~StatusMng()
{
    INFO_LOG("~StatusMng."); 
}

void StatusMng :: run()
{
    while( falg )
    {
        DEBUG_LOG("start DealwithTimeout");     
        DealwithTimeout( );
        
        AddTimer(iTimeout, iType, iTimerID);
    }
    
}



bool StatusMng :: AddTimer(const int iTimeout_, const int iType_, uint32_t & iTimerID_)
{
    if (iTimeout == -1)
    {
        return true;
    }
    iTimeout = iTimeout_;
    iType = iType_;
    iTimerID= iTimerID_;
    uint64_t llAbsTime = Time::GetSteadyClockMS() ;
    llAbsTime+= iTimeout;
     
    oTimer.AddTimerWithType(llAbsTime, iType, iTimerID);

    mapTimerIDExist[iTimerID] = true;

    return true;
}

void StatusMng :: RemoveTimer(uint32_t & iTimerID)
{
    auto it = mapTimerIDExist.find(iTimerID);
    if (it != end(mapTimerIDExist))
    {
        mapTimerIDExist.erase(it);
    }

    iTimerID = 0;
}

void StatusMng :: DealwithTimeoutOne(const uint32_t iTimerID, const int iType)
{
    auto it = mapTimerIDExist.find(iTimerID);
    if (it == end(mapTimerIDExist))
    {
        ERROR_LOG("Timeout aready remove!, timerid %u iType %d", iTimerID, iType);
        return;
    }

    mapTimerIDExist.erase(it);

    m_oPhxKV.DumpRocksdbStatus();

    m_oPhxKV.DumpClusterStatus();

}

void StatusMng :: DealwithTimeout()
{

    while( true )
    {
        uint32_t iTimerID = 0;
        int iType = 0;
        bool bHasTimeout = oTimer.PopTimeout(iTimerID, iType);

        if (bHasTimeout)
        {
            DealwithTimeoutOne(iTimerID, iType);

            RemoveTimer( iTimerID );

            break;
        }else{
            sleep(10 );
        }
    }
    
}

void StatusMng :: stop()
{
    falg =false;
    join();
}
}
