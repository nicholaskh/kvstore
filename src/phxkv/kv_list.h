
#pragma once
#include "phxkv.pb.h"
#include "kv_paxos.h"
#include "sysconfig.h"
namespace phxkv
{

class KvList{

public:
    KvList(PhxKV& phxKv);
    ~KvList();
public:
    int ListMsg(const ::phxkv::Request* request, ::phxkv::Response* response); 
private:
    PhxKV& m_oPhxkv;
    int BuildgroupList( const ::phxkv::Request* request, ::phxkv::Response* response);
    int ListLpop(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int ListRpop(  const ::phxkv::Request* request, ::phxkv::Response* response);//弹出表尾
    int ListRpopLpush(  const ::phxkv::Request* request, ::phxkv::Response* response);//尾部进行头插
    int ListIndex(  const ::phxkv::Request* request, ::phxkv::Response* response);//插入到指定位置
    int ListInsert(  const ::phxkv::Request* request, ::phxkv::Response* response);//插入到指定位置
    int ListLen(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int ListLpush(  const ::phxkv::Request* request, ::phxkv::Response* response);//头插法
    int ListLpushx(  const ::phxkv::Request* request, ::phxkv::Response* response);//链表存在时，才执行头部插入
    int ListRange(  const ::phxkv::Request* request, ::phxkv::Response* response);
    int ListRem(  const ::phxkv::Request* request, ::phxkv::Response* response);//删除N个等值元素
    int ListSet(  const ::phxkv::Request* request, ::phxkv::Response* response);//根据下表进行更新元素
    int ListTtim(  const ::phxkv::Request* request, ::phxkv::Response* response);//保留指定区间元素，其余删除
    int ListRpush(  const ::phxkv::Request* request, ::phxkv::Response* response);//尾插法
    int ListRpushx(  const ::phxkv::Request* request, ::phxkv::Response* response);//链表存在时，才执行尾部插入
};
}