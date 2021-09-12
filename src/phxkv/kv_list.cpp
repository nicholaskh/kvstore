#include "kv_list.h"

namespace phxkv
{

KvList::KvList( PhxKV& phxKv ):m_oPhxkv( phxKv)
{

}
KvList::~KvList()
{
    INFO_LOG("KvList::~KvList.");
}
int KvList :: BuildgroupList( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    if (!m_oPhxkv.IsMaster( request->groupid() ) )
    {
        response->set_ret_code(Response_enum_code_RES_MASTER_REDIRECT);
        phxkv::GroupsMapMsg Msg;
        
        int cong_group = CConfig::Get()->paxosnodes.group_cnt;
        for(int i=0;i<cong_group;i++  ){

            phxkv::GroupMsg* msg = Msg.add_subgroup();

            uint64_t llMasterNodeID = m_oPhxkv.GetMasterByGroup( i ).GetNodeID();
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
        phxkv::GroupsMapMsg* MapMsg = response->mutable_submap();
        MapMsg->CopyFrom( Msg );
    }
    INFO_LOG(" HashDel MASTER_REDIRECT ok hash_key %s ", request->key().c_str() );
    return 0;
}

int KvList::ListMsg(const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int req_type = request->list_req().req_type();//实际的请求类型，读写。。。。
    INFO_LOG("KvList req_type=%d",req_type );
    switch ( req_type )
    {
    case ListRequest_enum_req_LIST_LPOP:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListLpop( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case ListRequest_enum_req_LIST_RPOP:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListRpop( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case ListRequest_enum_req_LIST_RPOP_LPUSH:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListRpopLpush( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case ListRequest_enum_req_LIST_INDEX:
        ListIndex( request, response ); 
        break;
    case ListRequest_enum_req_LIST_INSERT:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListInsert( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case ListRequest_enum_req_LIST_LENGTH: 
        ListLen(request ,response );
        break;
    case ListRequest_enum_req_LIST_LPUSH:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListLpush( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case ListRequest_enum_req_LIST_LPUSHX:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListLpushx( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case ListRequest_enum_req_LIST_RANGE:
        ListRange(request,response );
        break;
    case ListRequest_enum_req_LIST_REM:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListRem( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case ListRequest_enum_req_LIST_RPUSH:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListRpush( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case ListRequest_enum_req_LIST_RPUSHX:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListRpushx( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case ListRequest_enum_req_LIST_SET:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListSet( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
        case ListRequest_enum_req_LIST_TRIM:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ListTtim( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    default:
        ERROR_LOG("ListMsg req_type error=%d" , req_type );
        break;
    }

}

int KvList::ListLpop(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.ListLpop( request , response);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}

int KvList::ListRpop(  const ::phxkv::Request* request, ::phxkv::Response* response)//弹出表尾
{
    int status = m_oPhxkv.ListRpop( request , response);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvList::ListRpopLpush(  const ::phxkv::Request* request, ::phxkv::Response* response)//尾部进行头插
{
    int status = m_oPhxkv.ListRpopLpush( request ,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}

int KvList::ListInsert(  const ::phxkv::Request* request, ::phxkv::Response* response)//插入到指定位置
{
    int status = m_oPhxkv.ListInsert( request);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvList::ListLen(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.ListLength( request, response);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvList::ListLpush(  const ::phxkv::Request* request, ::phxkv::Response* response)//头插法
{
    int status = m_oPhxkv.ListLpush( request);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
    
int KvList::ListLpushx(  const ::phxkv::Request* request, ::phxkv::Response* response)//链表存在时，才执行头部插入
{
    int status = m_oPhxkv.ListLpushx( request);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvList::ListRange(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.ListRange( request, response);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvList::ListRem(  const ::phxkv::Request* request, ::phxkv::Response* response)//删除N个等值元素
{
    int status = m_oPhxkv.ListRem( request);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvList::ListSet(  const ::phxkv::Request* request, ::phxkv::Response* response)//根据下表进行更新元素
{
    int status = m_oPhxkv.ListSet( request);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvList::ListTtim(  const ::phxkv::Request* request, ::phxkv::Response* response)//保留指定区间元素，其余删除
{
    int status = m_oPhxkv.ListTtim( request);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;   
}
int KvList::ListRpush(  const ::phxkv::Request* request, ::phxkv::Response* response)//尾插法
{
    int status = m_oPhxkv.ListRpush( request);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvList::ListRpushx(  const ::phxkv::Request* request, ::phxkv::Response* response)//链表存在时，才执行尾部插入
{
    int status = m_oPhxkv.ListRpushx( request);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvList::ListIndex(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.ListIndex( request , response);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}

}