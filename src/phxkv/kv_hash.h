#pragma once

#include "phxkv.pb.h"
#include "kv_paxos.h"
#include "sysconfig.h"
namespace phxkv
{
class KvHash{
public:
    KvHash(PhxKV& phxKv);
    ~KvHash();
    
private:
    int StatusCode(int code);
    PhxKV& m_oPhxkv;
public:
    int HashMsg(const ::phxkv::Request* request, ::phxkv::Response* response); 
private:
    int BuildgroupList( const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashDel( const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashGet(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashSet(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashGetall(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashExist(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashIncrByInt(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashIncrByFloat(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashKeys(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashLen(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashMget(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashMSet(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashSetNx(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int HashValues(  const ::phxkv::Request* request, ::phxkv::Response* response);
       
};
}