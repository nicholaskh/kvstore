
#pragma once


#include "phxkv.pb.h"
#include "kv_paxos.h"
#include "sysconfig.h"
namespace phxkv
{
class KvZset{
public:
    KvZset(PhxKV& phxKv);
    ~KvZset();
public:
    int ZsetMsg(const ::phxkv::Request* request, ::phxkv::Response* response); 
    int BuildgroupList( const ::phxkv::Request* request, ::phxkv::Response* response);
private:
    PhxKV& m_oPhxkv;

    int ZAdd( const ::phxkv::Request* request, ::phxkv::Response* response);//一个或多个
    int ZCard( const ::phxkv::Request* request, ::phxkv::Response* response);//元素数量
    int ZCount( const ::phxkv::Request* request, ::phxkv::Response* response);//有序集合中，在区间内的数量
    int ZRange( const ::phxkv::Request* request, ::phxkv::Response* response);//有序集合区间内的成员
    int ZIncrby( const ::phxkv::Request* request, ::phxkv::Response* response);//为score增加或减少
    int ZUnionStore( const ::phxkv::Request* request, ::phxkv::Response* response);//并集不输出
    int ZInterStore( const ::phxkv::Request* request,::phxkv::Response* response);//交集不输出
    int ZRangebyscore( const ::phxkv::Request* request, ::phxkv::Response* response);
    int ZRem( const ::phxkv::Request* request, ::phxkv::Response* response);//移除有序集 key 中的一个或多个成员
    int ZRank( const ::phxkv::Request* request, ::phxkv::Response* response);//返回member的排名，
    int ZRevrank( const ::phxkv::Request* request, ::phxkv::Response* response);//member逆序排名
    int ZScore( const ::phxkv::Request* request, ::phxkv::Response* response);//member的score
    int ZREVRange( const ::phxkv::Request* request, ::phxkv::Response* response);//指定区间成员，逆序输出
    int ZREVRangebylscore( const ::phxkv::Request* request, ::phxkv::Response* response);
    int ZRemrangebyrank( const ::phxkv::Request* request, ::phxkv::Response* response);
    int ZRemrangebyscore( const ::phxkv::Request* request, ::phxkv::Response* response);

};
}