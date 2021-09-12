#include "kv_set.h"

namespace phxkv
{

KvSet::KvSet(PhxKV& phxKv):m_oPhxkv(phxKv)
{
}
KvSet::~KvSet()
{

}

int KvSet::SetMsg(const ::phxkv::Request* request, ::phxkv::Response* response)
{
    int req_type = request->set_req().req_type();//实际的请求类型，读写。。。。
    INFO_LOG("SetMsg req_type=%d",req_type );
    switch ( req_type )
    {
    case SetRequest_enum_req_SET_ADD:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            SAdd( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;    
    case SetRequest_enum_req_SET_CARD:
        SCard( request, response );
        break;
    case SetRequest_enum_req_SET_DIFFSTORE:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            SDiffStore( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case SetRequest_enum_req_SET_DIFF:
        SDiff( request, response ); 
        break;
    case SetRequest_enum_req_SET_INTER:
        SInter( request, response ); 
        break;
    case SetRequest_enum_req_SET_INTERSTORE:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            SInterStore( request, response ); 
        }else{
            BuildgroupList( request, response );
        }
        break;
    case SetRequest_enum_req_SET_ISMEMBER:
        SIsMember( request, response ); 
        break;
    case SetRequest_enum_req_SET_MEMBERS:
        SMembers( request, response );//为响应复制
        break;
    case SetRequest_enum_req_SET_MOVE:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            SMove( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );
        }
        break;
    case SetRequest_enum_req_SET_POP:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            SPop( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );
        }
        break;
    case SetRequest_enum_req_SET_RANDMEMBER:
        SRandMember( request, response );//为响应复制
        break;
    case SetRequest_enum_req_SET_REM:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            SRem( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );
        }
        break;
    case SetRequest_enum_req_SET_UNION:
        SUnion( request, response );//为响应复制
        break;
    case SetRequest_enum_req_SET_UNONSTORE:
        if ( m_oPhxkv.IsMaster( request->groupid() ) ){
            SUnionStore( request, response );//为响应复制
        }else{
            BuildgroupList( request, response );
        }
        break;
    default:
        break;
    }
     
      return 0; 
}

int KvSet::BuildgroupList( const ::phxkv::Request* request, ::phxkv::Response* response)
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
int KvSet::SAdd(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    INFO_LOG("SAdd start");
    int status = m_oPhxkv.SAdd( request );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SRem(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
     
    int status = m_oPhxkv.SRem( request  );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SCard(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    
    int status = m_oPhxkv.SCard( request ,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SMembers(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
     
    int status = m_oPhxkv.SMembers( request ,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SUnionStore(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    
    int status = m_oPhxkv.SUnionStore( request  );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SUnion(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    
    int status = m_oPhxkv.SUnion( request ,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SInterStore(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
     
    int status = m_oPhxkv.SInterStore( request );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SInter(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    
    int status = m_oPhxkv.SInter( request ,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SDiffStore(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
     
    int status = m_oPhxkv.SDiffStore( request  );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SDiff(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
     
    int status = m_oPhxkv.SDiff( request ,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SIsMember(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
    
    int status = m_oPhxkv.SIsMember( request ,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SPop(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
     
    int status = m_oPhxkv.SPop( request,response   );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SRandMember(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
     
    int status = m_oPhxkv.SRandMember( request ,response );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}
int KvSet::SMove(  const ::phxkv::Request* request, ::phxkv::Response* response)
{
     
    int status = m_oPhxkv.SMove( request  );
    response->set_ret_code((phxkv::Response_enum_code) status );
    return 0;
}


}
