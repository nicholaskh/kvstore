

#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include "util.h"
#include <grpc++/grpc++.h>
#include "phxkv.pb.h"
#include "phxkv.grpc.pb.h"
#include "KVClient.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace phxpaxos;
using namespace std;


using namespace phxkv;
 
void PhxKVClient::NewChannel(const std::string ip,int port){
    char sAddress[128] = {0};
    snprintf(sAddress, sizeof(sAddress), "%s:%d", ip.c_str() , port);

    string sServerAddress = sAddress;

    stub_ = PhxKVServer::NewStub(grpc::CreateChannel( sServerAddress,
                    grpc::InsecureChannelCredentials() ) );
}

int PhxKVClient::Put(const std::string & sKey, const std::string & sValue, const int ms,int groupid , const int iDeep ){
        if (iDeep > 3)
        {
            return static_cast<int>(PhxKVStatus::FAIL);
        }

        KVOperator oRequest;
        oRequest.set_key( kv_encode::EncodeStringKey( sKey ) );
        if( 0 == ms ){
            oRequest.set_value(kv_encode::EncodeStringValue( sValue , 0 ) );
        }else{
            oRequest.set_value(kv_encode::EncodeStringValue( sValue , ms+ Time::GetTimestampSec() ) );
        }
        
        //oRequest.set_ttl( ms );
        oRequest.set_groupid( groupid );
        oRequest.set_operator_(KVOperatorType_WRITE);

        KVResponse oResponse;
        ClientContext context;
        Status status = stub_->KvPut(&context, oRequest, &oResponse);
            
        if (status.ok())
        {
            if (oResponse.ret() == static_cast<int>( MASTER_REDIRECT))
            {
                 
            phxkv::GroupsMapMsg map_sub = oResponse.submap();
            int size = map_sub.subgroup_size();
            printf("kvput ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == groupid ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return Put(sKey, sValue,ms,groupid, iDeep + 1);
                }
            }
        }
        printf("kvput finish...\n");
        return oResponse.ret();
    }
    else
    {
        printf("Kv Put %s", status.error_message().c_str() );
        return static_cast<int>( FAIL);
    }
}

    int PhxKVClient::Get(const std::string & sKey, std::string & sValue,int groupid , const int iDeep ){
            if (iDeep > 3)
            {
                return static_cast<int>(PhxKVStatus::FAIL);
            }

            KVOperator oRequest;
            oRequest.set_key(kv_encode::EncodeStringKey( sKey ));
            oRequest.set_groupid( groupid );

            KVResponse oResponse;
            ClientContext context;
            Status status = stub_->KvGet(&context, oRequest, &oResponse);

            if (status.ok())
            {
                //printf("get ok.\n");
                if (oResponse.ret() == static_cast<int>( MASTER_REDIRECT))
                {
                    
                    phxkv::GroupsMapMsg map_sub = oResponse.submap();
                    int size = map_sub.subgroup_size();
                    for(int k=0;k< size;k++ ){
                        phxkv::GroupMsg sub =  map_sub.subgroup(k);
                        std::string ip = sub.masterip();
                        int port = sub.masterport();
                        int gpid = sub.groupid();
                        
                        if( groupid == gpid ){
                            printf("kv get MASTER_REDIRECT,groupid %d %s %d\n", gpid, ip.c_str() , port );
                            NewChannel( ip, port );
                            return Get(sKey, sValue, groupid , iDeep + 1);
                        }

                    }
                }
                sValue = oResponse.data();
                if( sValue.empty() ){
                    printf("value empty:" );
                }
                return oResponse.ret();
            }
            else
            {       
                printf("get error.");
                return   FAIL;
            }
    }
    int PhxKVClient::GeLocal(const std::string & sKey, std::string & sValue, const int iDeep ){
        if (iDeep > 3)
            {
                return static_cast<int>(PhxKVStatus::FAIL);
            }

            KVOperator oRequest;
            oRequest.set_key(sKey);
             

            KVResponse oResponse;
            ClientContext context;
            Status status = stub_->KvGetLocal(&context, oRequest, &oResponse);

            if (status.ok())
            {
                printf("GeLocal ok.");
                sValue = oResponse.data();
                printf("value length:%d", sValue.size() );
                return oResponse.ret();
            }
            else
            {       
                printf("GeLocal error.");
                return   FAIL;
            }
    }

    int PhxKVClient::BatchPut(std::vector<string> vec_key_value,int groupid,const int iDeep ){
            if (iDeep > 3)
            {
                return static_cast<int>(PhxKVStatus::FAIL);
            }
            KvBatchPutRequest batchrequest;
            for( int i=0;i<vec_key_value.size();i++ ){

                string key_value =  vec_key_value[i];

                std::vector<string> sub_key_value;
                StringUtil::splitstr(key_value, ":",sub_key_value );
                if(sub_key_value.size()!=3 ){
                    printf("StringUtil splitstr error,size != 3 ");
                    return -1;
                }
                
                string key =sub_key_value[0];
                string value =sub_key_value[1];
                int ms = atoi(sub_key_value[2].c_str() );
                phxkv::KvBatchPutSubRequest* sub = batchrequest.add_subs();
                sub->set_key( kv_encode::EncodeStringKey( key ) );
                if( ms != 0 ){
                    sub->set_value( kv_encode::EncodeStringValue( value , ms+ Time::GetTimestampSec()) );
                }else{
                    sub->set_value( kv_encode::EncodeStringValue( value , 0 ) );
                }
                
               
                std::cout<<"data:"<<key<<"  "<<value<<"   "<<ms<<std::endl;
            }
            

            KvBatchPutResponse  oResponse;
            ClientContext context;
            Status status = stub_->KvBatchPut(&context, batchrequest, &oResponse);
            
            if (status.ok())
            {
                printf("KvBatchPut put ret %d\n",oResponse.ret() );
                if (oResponse.ret() == static_cast<int>( MASTER_REDIRECT))
                {

                    phxkv::GroupsMapMsg map_sub = oResponse.submap();
                    int size = map_sub.subgroup_size();
                    for(int k=0;k< size;k++ ){
                        phxkv::GroupMsg sub =  map_sub.subgroup(k);
                        std::string ip = sub.masterip();
                        int port = sub.masterport();
                        int gpid = sub.groupid();
                        
                        if( gpid == groupid ){
                            printf("kv KvBatchPut MASTER_REDIRECT,groupid %d %s %d\n", gpid, ip.c_str() , port );
                            NewChannel( ip, port );
                            return BatchPut(vec_key_value, iDeep + 1);
                        }
                    }
                }
                return oResponse.ret();
            }
            else
            {
                printf("KvBatchPut Put %s", status.error_message().c_str() );
                return static_cast<int>( FAIL);
            }
        }

        void PhxKVClient::dropMast(const int num){

            ClientContext context;
            phxkv::DropMastReq request;
            request.set_cnt( num );
            phxkv::DropMastRes response;

            Status status = stub_->KvDropMaster( &context, request, &response );
            
            if (status.ok())
            {
                int ret_num = response.ret();//返回个数
                
                printf("drop num=%d\n", ret_num );
                if(ret_num = num ){
                    printf("...ok...");
                }else{
                    printf("...warn....");
                }
            }else{
                printf("rpc error.%d....%s.",status.error_code(),status.error_message().c_str() );
            }
        }

void PhxKVClient::Hset(const std::string hash_key,const std::string field_key,const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"Hset iDeep > 3 "<<std::endl;
        return ;
    }
    std::vector<string> vec;
    StringUtil::splitstr(field_key, ":" , vec );
    string key = vec[0];
    string value = vec[1];
    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest= req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_SET );
     
    phxkv::HashField *field = oRequest->add_field();
    key = kv_encode::EncodeHashFieldKey(hash_key , key);
    field->set_field_key( key );
    field->set_field_value( value );

    Response res;
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
            
    if (status.ok())
    {
        HashResponse oResponse = res.hash_response();
        printf("ret=%d",res.ret_code() );
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("ret ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return Hset(hash_key, field_key, iDeep + 1);
                }
            }
        }
        printf("kvput finish...\n");
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}

void PhxKVClient::Hget(const std::string hash_key,const std::string field_key ,const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"Hget iDeep > 3 "<<std::endl;
        return ;
    }
    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest = req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_GET );

    phxkv::HashField *field = oRequest->add_field();
    string new_field_key = kv_encode::EncodeHashFieldKey(hash_key , field_key);
    field->set_field_key( new_field_key );

    Response res;
    
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
            
    if (status.ok())
    {    HashResponse oResponse = res.hash_response();
        std::cout<<"ret=="<<res.ret_code()<<"size==="<<oResponse.field_size()<<std::endl;
        if( res.ret_code() != static_cast<int>( Response_enum_code_RES_SUCC)){
            
            return;
        }
        printf( "value value=%s\n",oResponse.field().at(0).field_value().c_str() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}

void PhxKVClient::Hdel(const std::string hash_key,const std::string field_key,  const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"Hdel iDeep > 3 "<<std::endl;
        return ;
    }
    std::vector<string> vec;
    StringUtil::splitstr(field_key, "," , vec );
     Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest = req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_DEL );
    for(int i=0;i<vec.size() ;i++ ){
        string key = "";
        phxkv::HashField *field = oRequest->add_field();
        key = kv_encode::EncodeHashFieldKey(hash_key , field_key);
        field->set_field_key( key );
    }
    Response res;
    
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
    HashResponse oResponse =res.hash_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return Hdel(hash_key, field_key, iDeep + 1);
                }
            }
        }
        
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}

void PhxKVClient::HashGetAll( const std::string hash_key ,const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"HashGetAll iDeep > 3 "<<std::endl;
        return ;
    }
    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest = req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_GETALL );
    phxkv::HashField *field = oRequest->add_field();
    string new_field_key = kv_encode::EncodeKey(hash_key ,HASH_TYPE);
    field->set_field_key( new_field_key );
    Response res;
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
     HashResponse oResponse=res.hash_response();   

    if (status.ok())
    {
        if( res.ret_code() != static_cast<int>( Response_enum_code_RES_SUCC)){
            printf("ret= %d", res.ret_code() );
            return;
        }
        printf( "key size=%d  \n",oResponse.field_size());
        for(int k=0;k<oResponse.field_size();k++ ){
            string key =oResponse.field().at(k).field_key();
            string value=oResponse.field().at(k).field_value();
            printf( "key size=%d  value=%s \n",key.length(), value.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}

void PhxKVClient::HashExist( const std::string hash_key ,const string field_key,const int iDeep )
{
    if (iDeep > 3)
    {   
        std::cout<<"Hget iDeep > 3 "<<std::endl;
        return ;
    }
    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest= req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_EXISTS );

    phxkv::HashField *field = oRequest->add_field();
    string new_field_key = kv_encode::EncodeHashFieldKey(hash_key , field_key);
    field->set_field_key( new_field_key );
    Response res;
    
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
    HashResponse oResponse = res.hash_response();       
    if (status.ok())
    {
        if( res.ret_code() != static_cast<int>( Response_enum_code_RES_SUCC)){
            printf( "ret =[%d] \n",res.ret_code() );
            return;
        }
        printf( "ret =[%d] \n",res.ret_code() );
        printf( "value value=[%d] \n",res.exist() );
        for(int k=0;k<oResponse.field_size();k++ ){
            string key =oResponse.field().at(k).field_key();
            string value=oResponse.field().at(k).field_value();
            printf( "key size=%d  value=%s \n",key.length(), value.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::HashIncrByInt( const std::string hash_key ,const string& field_key,const string num,const int iDeep )
{
     if (iDeep > 3)
    {   
        std::cout<<"Hset iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest= req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_INCR_INT );
    oRequest->set_int_value( atoi(num.c_str() ) );
    phxkv::HashField *field = oRequest->add_field();
    string new_key = kv_encode::EncodeHashFieldKey(hash_key , field_key);
    field->set_field_key( new_key );
    

    Response res;
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
            
    if (status.ok())
    {
        printf("ret111=%d",res.ret_code() );
        if( res.ret_code() == 0 ){  
            HashResponse oResponse = res.hash_response();
            for(int i=0;i< oResponse.field_size();i++ ){
                printf("HashIncrByInt finish.%s..\n" , oResponse.field(0).field_value().c_str() );
            }
            
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::HashIncrByFloat(const std::string hash_key ,const string& field_key,const string num ,const int iDeep )
{
    if (iDeep > 3){
        std::cout<<"Hset iDeep > 3 "<<std::endl;
        return ;
    }
    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest= req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_INCR_FLOAT );
    oRequest->set_float_value( atof(num.c_str() ) );
    phxkv::HashField *field = oRequest->add_field();
    string new_key = kv_encode::EncodeHashFieldKey(hash_key , field_key);
    field->set_field_key( new_key );
     

    Response res;
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
            
    if (status.ok())
    {
        HashResponse oResponse = res.hash_response();
        printf("ret=%d",res.ret_code() );
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("ret ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return HashIncrByFloat(hash_key, field_key, num, iDeep + 1);
                }
            }
        }
        for(int i=0;i< oResponse.field_size();i++ ){
                printf("HashIncrByInt finish.%s..\n" , oResponse.field(0).field_value().c_str() );
            }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::HashKeys( const std::string hash_key ,const int iDeep )
{
    return HashGetAll(hash_key, iDeep );
}
void PhxKVClient::HashLen( const std::string hash_key ,const int iDeep )
{
     
    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest=req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_LEN );

    Response res;
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
     HashResponse oResponse =res.hash_response();       
    if (status.ok())
    {
        if( res.ret_code() != static_cast<int>( Response_enum_code_RES_SUCC)){
            printf( "ret =[%d] \n",res.ret_code() );
            return;
        }
        printf( "length length=[%d] \n",res.length() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::HashMget( const std::string hash_key ,const std::vector<string> vec, const int iDeep )
{
    if (iDeep > 3)
    {   
        std::cout<<"HashMget iDeep > 3 "<<std::endl;
        return ;
    }
    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest=req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_MGET );

    for(int k=0;k<vec.size();k++ ){
        string field_key = vec[k];
        string new_field_key = kv_encode::EncodeHashFieldKey(hash_key , field_key);
        phxkv::HashField *field = oRequest->add_field();
        field->set_field_key( new_field_key );
    }

    Response res;
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
     HashResponse oResponse= res.hash_response();       
    if (status.ok())
    {
        if( res.ret_code() != static_cast<int>( Response_enum_code_RES_SUCC)){
            printf( "HashMget error....\n" );
            return;
        }
        for(int j=0;j< oResponse.field_size();j++ ){
            string key = oResponse.field().at(j).field_key();
            string value = oResponse.field().at(j).field_value();
            printf( "HashMget keysize=%d value=%s \n",key.size(),value.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::HashMSet( const std::string hash_key ,const std::vector<string> vec,const int iDeep )
{
    std::cout<<"param size="<<vec.size()<<std::endl;
    if (iDeep > 3)
    {   
        std::cout<<"Hset iDeep > 3 "<<std::endl;
        return ;
    }
    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest= req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_MSET );

    for(int k=0;k<vec.size();k++ ){
        string field = vec[k];
        std::vector<string> tt_vec;
         StringUtil::splitstr(field, ":" , tt_vec );
         string field_key = tt_vec[0];
        string field_value = tt_vec[1];
        phxkv::HashField *hashfield = oRequest->add_field();

        string new_key = kv_encode::EncodeHashFieldKey(hash_key , field_key);
        hashfield->set_field_key( new_key );
        hashfield->set_field_value( field_value );
    }

    Response res;
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res);
    HashResponse oResponse =  res.hash_response();    
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("ret ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return HashMSet(hash_key, vec, iDeep + 1);
                }
            }
        }
        printf("ret= %d", res.ret_code() );
        printf("kvput finish...\n");
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::HashSetNx( const std::string hash_key ,const std::string field_key,const int iDeep )
{
    if (iDeep > 3)
    {   
        std::cout<<"HashSetNx iDeep > 3 "<<std::endl;
        return ;
    }
    std::vector<string> vec;
    StringUtil::splitstr(field_key, ":" , vec );
    string key = vec[0];
    string value = vec[1];

    Request req;
    req.set_data_type( Request_req_type_HASH_REQ );
    req.set_groupid(0);
    req.set_key(hash_key);
    HashRequest* oRequest=req.mutable_hash_req();
    oRequest->set_req_type( HashRequest_enum_req_HASH_SETNX );

    phxkv::HashField *field = oRequest->add_field();
    key = kv_encode::EncodeHashFieldKey(hash_key , key);
    field->set_field_key( key );
    field->set_field_value( value );

    Response res;
    ClientContext context;
    Status status = stub_->HashOperate(&context, req, &res );
    HashResponse oResponse =  res.hash_response();
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("ret ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return HashSetNx(hash_key, field_key, iDeep + 1);
                }
            }
        }
        printf("HashSetNx finish..ret=%d.\n",res.ret_code());
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::HashValues( const std::string hash_key ,const int iDeep )
{
    return HashGetAll(hash_key, iDeep );
}

//===================list===================
void PhxKVClient::ListLpush( std::string list_key, std::vector<string>& field_key,const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_LPUSH );
    for(int i=0;i<field_key.size() ;i++ ){
       
        string * field = oRequest->add_field();
        string key = field_key[i];
        field->assign(  key.data() , key.length()  );
    }
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListLpush(list_key, field_key, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListLpushx(  std::string list_key, string& field_key,const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_LPUSHX );
    for(int i=0;i<1 ;i++ ){
       
        string * field = oRequest->add_field();
        string key = kv_encode::EncodeHashFieldKey(list_key , field_key);
        field->assign( key.data() , key.length()  );
    }
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListLpushx(list_key, field_key, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListLpop(  std::string list_key ,const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_LPOP );
     
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListLpop(list_key,   iDeep + 1);
                }
            }
        }
        for(int k=0;k<oResponse.field_size();k++ ){
            string key =oResponse.field(k);
           
            printf( " value=%s \n",  key.c_str() );
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListLength(  std::string list_key,const int iDeep)
{
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest=req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_LENGTH );

    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
     ListResponse oResponse =res.list_response();       
    if (status.ok())
    {
        if( res.ret_code() != static_cast<int>( Response_enum_code_RES_SUCC)){
            printf( "ret =[%d] \n",res.ret_code() );
            return;
        }
        printf( "length length=[%d] \n",res.length() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListRpop(  std::string list_key,const int iDeep )//弹出表尾
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_RPOP );
     
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListRpop(list_key,   iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListRpopLpush(  std::string list_src ,std::string list_dest,const int iDeep)//尾部进行头插
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_dest );//目的表
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_RPOP_LPUSH );
    oRequest->set_src_list(list_src); // ->assign( .data(),list_src.length() );//原表
     
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListRpopLpush(list_src, list_dest,  iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListRpush(     std::string list_key,std::vector<string>& field_key ,const int iDeep)//尾插法
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_RPUSH );
    for(int i=0;i<field_key.size() ;i++ ){
       
        string * field = oRequest->add_field();
        string key = field_key[i];
        field->assign(  key.data() , key.length()  );
    }
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListRpush(list_key, field_key, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListRpushx(    std::string list_key, string& field_key ,const int iDeep)//链表存在时，才执行尾部插入
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_RPUSHX );
    for(int i=0;i<1 ;i++ ){
       
        string * field = oRequest->add_field();
        string key = kv_encode::EncodeHashFieldKey(list_key , field_key);
        field->assign(  key.data() , key.length()  );
    }
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListRpushx(list_key, field_key, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListIndex(   std::string list_key,const int index,const int iDeep)
{
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    
    ListRequest* oRequest=req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_INDEX );
    oRequest->set_index( index );
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
     ListResponse oResponse =res.list_response();       
    if (status.ok())
    {
        if( res.ret_code() != static_cast<int>( Response_enum_code_RES_SUCC)){
            printf( "ret =[%d] \n",res.ret_code() );
            return;
        }
        printf( "index value=[%s] \n",oResponse.field(0).c_str() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListInsert(   std::string list_key ,string field,int flag,string new_value,const int iDeep)//插入到指定位置
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_INSERT );
    oRequest->set_pivot( field );
    oRequest->set_pos_flag( flag );
    oRequest->add_field()->assign(new_value.data() , new_value.length() );

    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListInsert(list_key, field,flag, new_value ,iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("error ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListRange(     std::string list_key,const int start,const int end,const int iDeep)
{
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest=req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_RANGE );
    oRequest->set_start( start );
    oRequest->set_end( end );
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
     ListResponse oResponse =res.list_response();       
    if (status.ok())
    {
        if( res.ret_code() != static_cast<int>( Response_enum_code_RES_SUCC)){
            printf( "ret =[%d] \n",res.ret_code() );
            return;
        }
        for(int i=0;i<oResponse.field_size();i++  ){
            string val = oResponse.field(i);

            printf( "value =[%s] \n",val.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListRem(    std::string list_key,const string & des_value,const int cnt,const int iDeep )// LREM key count value
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_REM );
    oRequest->set_count( cnt );
    oRequest->add_field()->assign( des_value.data(), des_value.length() );
    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListRem(list_key, des_value,cnt, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListSet(   std::string list_key,std::string field,int index ,const int iDeep)//根据下表进行更新元素
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_SET );
    oRequest->set_index(index );
    oRequest->add_field()->assign(field.data() , field.length() );

    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListSet(list_key, field,index, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ListTtim( std::string list_key ,int start ,int end ,const int iDeep)//保留指定区间元素，其余删除
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_LIST_REQ );
    req.set_groupid(0);
    req.set_key(list_key);
    ListRequest* oRequest = req.mutable_list_req();
    oRequest->set_req_type( ListRequest_enum_req_LIST_TRIM );
    oRequest->set_start( start );
    oRequest->set_end( end );

    Response res;
    ClientContext context;
    Status status = stub_->ListOperate(&context, req, &res);
    ListResponse oResponse =res.list_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf("del ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ListTtim(list_key, start,end, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}

//============set=================================

void PhxKVClient::SAdd( const std::string& set_key,std::vector<std::string> vec,const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_ADD );
    for(int i=0;i<vec.size();i++ ){
        std::string ttt = vec[i];
        ttt = kv_encode::EncodeSetFieldKey(set_key , ttt );
        oRequest->add_field()->assign( ttt.data(),ttt.length() );
    }
    
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse =res.set_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return SAdd(set_key, vec, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SRem(   const std::string& set_key,std::vector<std::string> vec,const int iDeep  )
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_REM );
    for(int i=0;i<vec.size();i++ ){
        std::string ttt = vec[i];
        ttt = kv_encode::EncodeSetFieldKey(set_key , ttt );
        oRequest->add_field()->assign( ttt.data(),ttt.length() );
    }
    
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse =res.set_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return SRem(set_key, vec, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SCard(    const std::string& set_key,const int iDeep )
{
     Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_CARD );
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..length=%d.\n" ,res.length() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SMembers(   const std::string& set_key,const int iDeep )
{
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_MEMBERS );
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse = res.set_response();
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        for(int i=0;i< oResponse.field_size();i++ ){
            std::string ttt = oResponse.field(i);
            printf(" finish..value=%s.\n" ,ttt.c_str() );
        }
        
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SUnionStore( const std::string& set_key,std::vector<std::string> vec,const int iDeep  )
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_UNONSTORE );
    for(int i=0;i<vec.size();i++ ){
        std::string ttt = vec[i];
         
        oRequest->add_src_set()->assign( ttt.data(),ttt.length() );
    }
    
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse =res.set_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return SUnionStore(set_key, vec, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SUnion( std::vector<std::string> vec,const int iDeep )
{
    std::cout<<"SUnion size=%d"<<vec.size()<<std::endl;
     Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
     
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_UNION );
    for(int i=0;i<vec.size();i++ ){
        std::string ttt = vec[i];
        oRequest->add_src_set()->assign( ttt.data(), ttt.length() );
    }
    Response res;
    ClientContext context;
     
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse = res.set_response();
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        for(int i=0;i< oResponse.field_size();i++ ){
            std::string ttt = oResponse.field(i);
            printf(" finish..value=%s.\n" ,ttt.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SInterStore( const std::string& set_key,std::vector<std::string> vec,const int iDeep )
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_INTERSTORE );
    for(int i=0;i<vec.size();i++ ){
        std::string ttt = vec[i];
         
        oRequest->add_src_set()->assign( ttt.data(),ttt.length() );
    }
    
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse =res.set_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return SInterStore(set_key, vec, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SInter( std::vector<std::string> vec, const int iDeep )
{
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
     
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_INTER );
    for(int i=0;i<vec.size();i++ ){
        std::string ttt = vec[i];
        oRequest->add_src_set()->assign( ttt.data(), ttt.length() );
    }
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse = res.set_response();
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        for(int i=0;i< oResponse.field_size();i++ ){
            std::string ttt = oResponse.field(i);
            printf(" finish..value=%s.\n" ,ttt.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SDiffStore(  const std::string& set_key ,std::vector<std::string> vec,const int iDeep )
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_DIFFSTORE );
    for(int i=0;i<vec.size();i++ ){
        std::string ttt = vec[i];
         
        oRequest->add_src_set()->assign( ttt.data(),ttt.length() );
    }
    
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse =res.set_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return SDiffStore(set_key, vec, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SDiff(std::vector<std::string> vec,  const int iDeep )
{
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
     
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_DIFF );
    for(int i=0;i<vec.size();i++ ){
        std::string ttt = vec[i];
        oRequest->add_src_set()->assign( ttt.data(), ttt.length() );
    }
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse = res.set_response();
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        for(int i=0;i< oResponse.field_size();i++ ){
            std::string ttt = oResponse.field(i);
            printf(" finish..value=%s.\n" ,ttt.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SIsMember(  const std::string& set_key,std::string& field_key,const int iDeep )
{
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_ISMEMBER );
    field_key = kv_encode::EncodeSetFieldKey(set_key , field_key);
    oRequest->add_field()->assign(field_key.data() , field_key.length() );
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse =res.set_response() ;        
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..ret=%d.\n" , res.exist() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SPop(  const std::string& set_key,  const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_POP );
 
    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse =res.set_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return SPop(set_key,  iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..ret=%s.\n" ,oResponse.field(0).c_str() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SRandMember(   const std::string& set_key,const int iDeep)
{
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( set_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_RANDMEMBER );

    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse =res.set_response() ;        
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..ret=%s.\n" ,oResponse.field(0).c_str() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::SMove( const std::string& src_key,const std::string& dest_key ,string field,const int iDeep  )
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_SET_REQ );
    req.set_groupid(0);
    req.set_key( dest_key );
    SetRequest* oRequest = req.mutable_set_req();
    oRequest->set_req_type( SetRequest_enum_req_SET_MOVE );
    oRequest->add_src_set()->assign(src_key.data()  ,src_key.length() );
    oRequest->add_field()->assign( field.data(),field.length() );

    Response res;
    ClientContext context;
    Status status = stub_->SetOperate(&context, req, &res);
    SetResponse oResponse =res.set_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();
                         
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return SMove(src_key, dest_key,field, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
//============zset=================================
void PhxKVClient::ZAdd(  const std::string & zset_key,std::vector<std::string> vec_t,const int iDeep)//一个或多个
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_ADD );
    for(int i=0;i<vec_t.size();i++ ){
        std::string ttt = vec_t[i];
        std::vector<string> vec ;
        StringUtil::splitstr(ttt, ":",vec  );

        const std::string key_ = vec[0];
        const std::string score_ = vec[1];

        std::string str_field_key = kv_encode::EncodeZSetFieldKey(zset_key ,key_ );

        phxkv::ZsetField * field_key_ = oRequest->add_field_key();
        field_key_->set_field_key( str_field_key );
        field_key_->set_field_value( kv_encode::EncodeZSetFieldValue( atoll(vec[1].c_str() )) );
        
        ////
        std::string str_field_score  = kv_encode::EncodeZSetScoreKey(zset_key ,atoll(score_.c_str()) );
        phxkv::ZsetField * field_score_ = oRequest->add_field_score();
        field_score_->set_field_key( str_field_score );
        field_score_->set_field_value( key_);
    }
    
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();            
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ZAdd(zset_key, vec_t, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZCard(  const std::string & zset_key,const int iDeep)//元素数量
{   
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_CARD );
     
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..length=%d.\n" ,res.length() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZCount(  const std::string & zset_key,std::string score1,std::string score2 ,const int iDeep)//有序集合中，在区间内的数量
{
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_COUNT );
    int64_t a = atoll( score1.c_str() );
    int64_t b = atoll( score2.c_str() );
    oRequest->set_min(a  );
    oRequest->set_max( b );

    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
         
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..length=%d.\n" ,res.length() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZRange(  const std::string & zset_key,std::string start,std::string end ,const int iDeep)//有序集合区间内的成员
{
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_RANGE );
    oRequest->set_start_pos( atoi(start.c_str() ));
    oRequest->set_end_pos( atoi(end.c_str()) );

    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
         
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..length=%d.\n" ,oResponse.field_size() );
        for(int i=0;i<oResponse.field_size();i++ ){
            string key1 = oResponse.field(i).field_key();
            string val1 = oResponse.field(i).field_value();
            printf(" finish.key=%s.value=%s.\n" ,key1.c_str(),val1.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZIncrby(   const std::string & zset_key,std::string field_key,std::string score,const int iDeep )//为score增加或减少
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_INCRBY );

    std::string str_field_key = kv_encode::EncodeZSetFieldKey(zset_key ,field_key );

    phxkv::ZsetField * field_key_ = oRequest->add_field_key();
    field_key_->set_field_key( str_field_key );
    field_key_->set_field_value( kv_encode::EncodeZSetFieldValue( atoll(score.c_str() ) ) );
    
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();            
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ZIncrby(zset_key, field_key,score, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZUnionStore(  const std::string & zset_key,std::vector<std::string> vec ,const int iDeep)//并集不输出
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_UNIONSTORE  );
    
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();            
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ZUnionStore(zset_key, vec, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZInterStore(  const std::string & zset_key,std::vector<std::string> vec,const int iDeep)//交集不输出
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_INTERSTORE  );
    
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();            
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ZInterStore(zset_key, vec, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZRangebyscore(  const std::string & zset_key,std::string score1,std::string score2,const int iDeep)
{
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_RANGEBYSCORE );
    oRequest->set_min( atoll(score1.c_str()) );
    oRequest->set_max( atoll(score2.c_str() ) );

    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
         
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..length=%d.\n" ,oResponse.field_size() );
        for(int i=0;i<oResponse.field_size();i++ ){
            string key1 = oResponse.field(i).field_key();
            string val1 = oResponse.field(i).field_value();
            printf(" finish.key=%s.value=%s.\n" ,key1.c_str(),val1.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZRem(  const std::string & zset_key,std::vector<std::string> vec_t,const int iDeep)//移除有序集 key 中的一个或多个成员
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_REM );
    for(int i=0;i<vec_t.size();i++ ){
        
        std::string key_ = vec_t[i];
       
        key_ = kv_encode::EncodeZSetFieldKey(zset_key ,key_ );

        phxkv::ZsetField * field_key_ = oRequest->add_field_key();
        field_key_->set_field_key( key_ );
    }
    
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();            
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ZRem(zset_key, vec_t, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZRank(   const std::string & zset_key,std::string field_key,const int iDeep)//返回member的排名
{
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_RANK );
    oRequest->add_field_key()->set_field_key( field_key );
    
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;   
     
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..ret=%d.\n" ,oResponse.mem_rank() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZRevrank(   const std::string & zset_key,std::string field_key,const int iDeep)//member逆序排名
{
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_REVRANK );
    oRequest->add_field_key()->set_field_key( field_key );
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;   
     
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..ret=%d.\n" ,oResponse.mem_rank() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZScore(  const std::string & zset_key,std::string field_key,const int iDeep)//member的score
{
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    field_key = kv_encode::EncodeZSetFieldKey(zset_key , field_key);
    oRequest->add_field_key()->set_field_key( field_key );
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_SCORE );
    
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;   
     
    if (status.ok())
    {
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        std::cout<<oResponse.mem_score()<<std::endl;
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZREVRange(  const std::string & zset_key,std::string start ,std::string end ,const int iDeep)
{
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_REVRANGE );
    oRequest->set_start_pos( atoi(start.c_str() ));
    oRequest->set_end_pos( atoi(end.c_str()) );

    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
         
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..length=%d.\n" ,oResponse.field_size() );
        for(int i=0;i<oResponse.field_size();i++ ){
            string key1 = oResponse.field(i).field_key();
            string val1 = oResponse.field(i).field_value();
            printf(" finish.key=%s.value=%s.\n" ,key1.c_str(),val1.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZREVRangebylscore(  const std::string & zset_key,string score1,string score2,const int iDeep)
{
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_REVRANGEBYSCORE );
    oRequest->set_min( atoll(score1.c_str()) );
    oRequest->set_max( atoll(score2.c_str() ) );

    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
         
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        printf(" finish..length=%d.\n" ,oResponse.field_size() );
        for(int i=0;i<oResponse.field_size();i++ ){
            string key1 = oResponse.field(i).field_key();
            string val1 = oResponse.field(i).field_value();
            printf(" finish.key=%s.value=%s.\n" ,key1.c_str(),val1.c_str() );
        }
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZRemrangebyrank(  const std::string & zset_key,const std::string rank1,std::string rank2,const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_REM_RANGEBYRANK );
    oRequest->set_start_pos( atoll(rank1.c_str() ) );
    oRequest->set_end_pos( atoll( rank2.c_str() ) );
    
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();            
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ZRemrangebyrank(zset_key, rank1,rank2, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}
void PhxKVClient::ZRemrangebyscore(  const std::string & zset_key,const std::string score1,const std::string score2 ,const int iDeep)
{
    if (iDeep > 3)
    {   
        std::cout<<"iDeep > 3 "<<std::endl;
        return ;
    }
    
    Request req;
    req.set_data_type( Request_req_type_ZSET_REQ );
    req.set_groupid(0);
    req.set_key( zset_key );
    ZsetRequest* oRequest = req.mutable_zset_req();
    oRequest->set_req_type( ZsetRequest_enum_req_ZSET_REM_RANGEBYSCORE );
    oRequest->set_min( atoll(score1.c_str()) );
    oRequest->set_max( atoll(score2.c_str() ) );
    
    Response res;
    ClientContext context;
    Status status = stub_->ZsetOperate(&context, req, &res);
    ZsetResponse oResponse =res.zset_response() ;        
    if (status.ok())
    {
        if (res.ret_code() == static_cast<int>( Response_enum_code_RES_MASTER_REDIRECT))
        {
            phxkv::GroupsMapMsg map_sub = res.submap();
            int size = map_sub.subgroup_size();
            printf(" ok...MASTER_REDIRECT size=%d \n",size );
            for(int k=0;k< size;k++ ){
                phxkv::GroupMsg sub =  map_sub.subgroup(k);

                std::string ip = sub.masterip();
                int port = sub.masterport();
                int gpid = sub.groupid();            
                if( gpid == 0 ){
                    printf("do request groupid =%d ip=%s port=%d\n", gpid, ip.c_str() , port );
                    NewChannel( ip, port );
                    return ZRemrangebyscore(zset_key, score1,score2, iDeep + 1);
                }
            }
        }
        printf(" finish..ret=%d.\n" ,res.ret_code() );
        return  ;
    }
    else
    {
        printf("ret= %s", status.error_message().c_str() );
        return  ;
    }
}

