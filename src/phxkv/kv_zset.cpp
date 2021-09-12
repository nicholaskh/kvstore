#include "kv_zset.h"

namespace phxkv
{
KvZset::KvZset(PhxKV& phxKv):m_oPhxkv(phxKv)
{

}
KvZset::~KvZset()
{
    
}

int KvZset::ZsetMsg(const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int req_type = request->zset_req().req_type();//实际的请求类型，读写。。。。
    INFO_LOG("ZsetMsg req_type=%d",req_type );
    switch ( req_type )
    {
    case ZsetRequest_enum_req_ZSET_ADD:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ZAdd( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break; 
    case ZsetRequest_enum_req_ZSET_CARD:
        ZCard( request, response ); 
        break; 
    case ZsetRequest_enum_req_ZSET_COUNT:
        ZCount( request, response ); 
        break; 
    case ZsetRequest_enum_req_ZSET_INCRBY:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ZIncrby( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break; 
    case ZsetRequest_enum_req_ZSET_RANGE:
        
        ZRange( request, response ); 
        break; 
    case ZsetRequest_enum_req_ZSET_UNIONSTORE:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ZUnionStore( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break; 
    case ZsetRequest_enum_req_ZSET_INTERSTORE:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ZInterStore( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break; 
    case ZsetRequest_enum_req_ZSET_RANGEBYSCORE:
        
        ZRangebyscore( request, response );
        break; 
    case ZsetRequest_enum_req_ZSET_REM:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ZRem( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break; 
    case ZsetRequest_enum_req_ZSET_RANK:
        ZRank( request, response ); 
        break; 
    case ZsetRequest_enum_req_ZSET_REVRANK:
        ZRevrank( request, response ); 
        break; 
    case ZsetRequest_enum_req_ZSET_SCORE: 
        ZScore( request, response ); 
        break; 
    case ZsetRequest_enum_req_ZSET_REVRANGE:
        ZREVRange( request, response ); 
        break; 
    case ZsetRequest_enum_req_ZSET_REVRANGEBYSCORE:
        
        ZREVRangebylscore( request, response ); 
        break; 
    case ZsetRequest_enum_req_ZSET_REM_RANGEBYRANK:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ZRemrangebyrank( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break; 
    case ZsetRequest_enum_req_ZSET_REM_RANGEBYSCORE:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            ZRemrangebyscore( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break; 
    default:
        break;   
    }
    return 0;
}
int KvZset::BuildgroupList( const ::phxkv::Request* request, ::phxkv::Response* response)
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
    }return 0;
}

int KvZset::ZAdd( const ::phxkv::Request* request, ::phxkv::Response* response)//一个或多个
{
    int status = m_oPhxkv.ZAdd( request );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZCard( const ::phxkv::Request* request, ::phxkv::Response* response)//元素数量
{
    int status = m_oPhxkv.ZCard( request,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZCount( const ::phxkv::Request* request, ::phxkv::Response* response)//有序集合中，在区间内的数量
{
    int status = m_oPhxkv.ZCount( request,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZRange( const ::phxkv::Request* request, ::phxkv::Response* response)//有序集合区间内的成员
{
    int status = m_oPhxkv.ZRange( request,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}

int KvZset::ZIncrby( const ::phxkv::Request* request, ::phxkv::Response* response)//为score增加或减少
{
    int status = m_oPhxkv.ZIncrby( request,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZUnionStore( const ::phxkv::Request* request , ::phxkv::Response* response)//并集不输出
{
    int status = m_oPhxkv.ZUnionStore( request  );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZInterStore( const ::phxkv::Request* request , ::phxkv::Response* response)//交集不输出
{
    int status = m_oPhxkv.ZInterStore( request );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZRangebyscore( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.ZRangebyscore( request ,response);
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZRem( const ::phxkv::Request* request, ::phxkv::Response* response )//移除有序集 key 中的一个或多个成员
{
    int status = m_oPhxkv.ZRem( request );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZRank( const ::phxkv::Request* request, ::phxkv::Response* response)//返回member的排名，
{
    int status = m_oPhxkv.ZRank( request,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZRevrank( const ::phxkv::Request* request, ::phxkv::Response* response)//member逆序排名
{
    int status = m_oPhxkv.ZRevrank( request,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZScore( const ::phxkv::Request* request, ::phxkv::Response* response)//member的score
{
    int status = m_oPhxkv.ZScore( request,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZREVRange( const ::phxkv::Request* request, ::phxkv::Response* response)//指定区间成员，逆序输出
{
    int status = m_oPhxkv.ZREVRange( request,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZREVRangebylscore( const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int status = m_oPhxkv.ZREVRangebylscore( request,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZRemrangebyrank( const ::phxkv::Request* request, ::phxkv::Response* response )
{
    int status = m_oPhxkv.ZRemrangebyrank( request );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvZset::ZRemrangebyscore( const ::phxkv::Request* request, ::phxkv::Response* response )
{
    int status = m_oPhxkv.ZRemrangebyscore( request );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}

}