#pragma once
#include "phxkv.pb.h"
#include "kv_paxos.h"
#include "sysconfig.h"
namespace phxkv
{


class KvSet{
public:
    KvSet(PhxKV& phxKv);
    ~KvSet();
public:
    int SetMsg(const ::phxkv::Request* request, ::phxkv::Response* response); 
private:
    PhxKV& m_oPhxkv;
    int BuildgroupList( const ::phxkv::Request* request, ::phxkv::Response* response);
    int SAdd(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SRem(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SCard(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SMembers(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SUnionStore(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SUnion(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SInterStore(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SInter(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SDiffStore(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SDiff(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SIsMember(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SPop(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SRandMember(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int SMove(  const ::phxkv::Request* request, ::phxkv::Response* response);
};
}