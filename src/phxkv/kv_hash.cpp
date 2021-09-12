#include "kv_hash.h"
namespace phxkv
{
KvHash::KvHash( PhxKV& phxKv ):m_oPhxkv( phxKv)
{
    
}
KvHash::~KvHash()
{
    INFO_LOG("KvHash::~KvHash.");
}

int KvHash :: BuildgroupList( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    if (!m_oPhxkv.IsMaster( request->groupid() ) )
    {
        response->set_ret_code(Response_enum_code_RES_MASTER_REDIRECT);
         
        phxkv::GroupsMapMsg* Msg = response->mutable_submap();
        int cong_group = CConfig::Get()->paxosnodes.group_cnt;
        for(int i=0;i<cong_group;i++  ){

            phxkv::GroupMsg* msg = Msg->add_subgroup();

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
        INFO_LOG(" HashDel MASTER_REDIRECT ok hash_key %s Msg=%d", request->key().c_str() ,Msg->subgroup_size() );
    }
    
    return 0;
}

int KvHash :: StatusCode(int status)
{
    INFO_LOG("StatusCode status=%d",status );
    return status;
}

int KvHash::HashMsg(const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int req_type = request->hash_req().req_type();//实际的请求类型，读写。。。。
    INFO_LOG("HashMsg req_type=%d",req_type );
    switch ( req_type )
    {
    case HashRequest_enum_req_HASH_DEL:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            HashDel( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );   
        }
        break;
    case HashRequest_enum_req_HASH_GET:
        HashGet( request, response );//为响应复制
         break;
    case HashRequest_enum_req_HASH_SET:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            HashSet( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );
        }
         break;
    case HashRequest_enum_req_HASH_GETALL:
         HashGetall( request, response );//为响应复制
         break;
    case HashRequest_enum_req_HASH_EXISTS:
         HashExist( request, response );//为响应复制
         break;
    case HashRequest_enum_req_HASH_KEYS:
         HashKeys( request, response );//为响应复制
         break;
    case HashRequest_enum_req_HASH_VALUES:
         HashValues( request, response );//为响应复制
         break;
    case HashRequest_enum_req_HASH_SETNX:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            HashSetNx( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );
        }
         break;
    case HashRequest_enum_req_HASH_MSET:
    if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            HashMSet( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );
        }
         break;
    case HashRequest_enum_req_HASH_MGET:
         HashMget( request, response );//为响应复制
         break;
    case HashRequest_enum_req_HASH_LEN:
         HashLen( request, response );//为响应复制
         break;
    case HashRequest_enum_req_HASH_INCR_INT:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            HashIncrByInt( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );
        }
        break;
    case HashRequest_enum_req_HASH_INCR_FLOAT:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            HashIncrByFloat( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );
        }
        break;
    default:
        ERROR_LOG("HashMsg cantnot into.");
        break;
    }
    return 0;
}
int KvHash::HashDel( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashDel( request );//提案错误0，转为了1
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}

int KvHash :: HashGet( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    INFO_LOG("HashSet start");
    int status = m_oPhxkv.HashGet( request ,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );

    return 0;
}
int KvHash :: HashSet( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashSet( request );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}

int KvHash::HashGetall(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashGetAll( request,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvHash::HashExist(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashExist( request,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    if( status==Response_enum_code_RES_SUCC ){
        response->set_exist(true);
    }
    return 0;
}
int KvHash::HashIncrByInt(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashIncrByInt( request,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvHash::HashIncrByFloat(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashIncrByFloat( request,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvHash::HashKeys(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashKeys( request,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvHash::HashLen(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashLen( request,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvHash::HashMget(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashMget( request,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvHash::HashMSet(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashMset( request,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvHash::HashSetNx(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashExist( request,response);
    INFO_LOG("HashSetNx1=%d", status);
    status = StatusCode( status );
    INFO_LOG("HashSetNx2=%d", status);
    if( status==Response_enum_code_RES_SUCC ){
        response->set_exist(true);
        response->set_ret_code( Response_enum_code_RES_KEY_EXISTS );
        return 0;
    }
    status =m_oPhxkv.HashSetNx( request );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvHash::HashValues(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.HashValues( request,response );
    status = StatusCode( status );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}

}