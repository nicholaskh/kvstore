

#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include "util.h"
#include <grpc++/grpc++.h>
#include "phxkv.pb.h"
#include "phxkv.grpc.pb.h"
 
#include "kv_encode.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace phxpaxos;
using namespace std;


using namespace phxkv;
 
enum KVOperatorType
{
    KVOperatorType_WRITE = 2,
    KVOperatorType_DELETE = 3,
};
enum PhxKVStatus
{
    SUCC = 0,
    FAIL = -1,
    KEY_NOTEXIST = 1,
    MASTER_REDIRECT = 10,
    NO_MASTER = 101,
};
class PhxKVClient
{
public:
    PhxKVClient(std::shared_ptr<grpc::Channel> channel) : 
        stub_(PhxKVServer::NewStub(channel)) {

    }
    void NewChannel(const std::string ip,int port);

    int Put(const std::string & sKey, const std::string & sValue, const int ms,int groupid , const int iDeep = 0);

    int Get(const std::string & sKey, std::string & sValue,int groupid , const int iDeep = 0);
            
    int GeLocal(const std::string & sKey, std::string & sValue, const int iDeep = 0);

    int BatchPut(std::vector<string> vec_key_value,int groupid,const int iDeep = 0);
    void dropMast(const int num);

    void Hset(const std::string hash_key, const std::string field_key,const int iDeep = 0);
    void Hget(const std::string hash_key, const std::string field_key,const int iDeep = 0);
    void Hdel(const std::string hash_key, const std::string field_key ,const int iDeep = 0);
    void HashGetAll( const std::string hash_key ,const int iDeep = 0);
    void HashExist( const std::string hash_key ,const string field_key,const int iDeep = 0);
    void HashIncrByInt(  const std::string hash_key ,const string& field_key,const string num,const int iDeep = 0);
    void HashIncrByFloat(  const std::string hash_key ,const string& field_key,const string num ,const int iDeep = 0);
    void HashKeys( const std::string hash_key ,const int iDeep = 0);
    void HashLen( const std::string hash_key ,const int iDeep = 0);
    void HashMget( const std::string hash_key ,const std::vector<string> vec, const int iDeep = 0);
    void HashMSet( const std::string hash_key ,const std::vector<string> field_key,const int iDeep = 0);
    void HashSetNx( const std::string hash_key ,const std::string field_key,const int iDeep = 0);
    void HashValues( const std::string hash_key ,const int iDeep = 0);
    //=====================list====================
    void ListLpush( std::string list_key, std::vector<string>& field_key,const int iDeep = 0);
    void ListLpushx(  std::string list_key, string& field_key,const int iDeep = 0);
    void ListLpop(  std::string list_key ,const int iDeep = 0);
    void ListLength(  std::string list_key, const int iDeep = 0);
    void ListRpop(  std::string list_key ,const int iDeep = 0);//弹出表尾
    void ListRpopLpush(  std::string list_src ,std::string list_dest, int iDeep = 0);//尾部进行头插
    void ListIndex(   std::string list_key,const int index,const int iDeep = 0);//插入到指定位置
    void ListInsert(   std::string list_key ,string field,int flag,string new_value,const int iDeep = 0);//插入到指定位置
    void ListRange(     std::string list_key,const int start,const int end,const int iDeep = 0);
    void ListRem(    std::string list_key,const string & des_value,const int cnt,const int iDeep = 0 );//删除N个等值元素
    void ListSet(   std::string list_key ,std::string field,int index,const int iDeep = 0);//根据下表进行更新元素
    void ListTtim(     std::string list_key ,int start ,int end ,const int iDeep = 0);//保留指定区间元素，其余删除
    void ListRpush(     std::string list_key, std::vector<string>& field_key,const int iDeep = 0 );//尾插法
    void ListRpushx(    std::string list_key, string& field_key ,const int iDeep = 0);//链表存在时，才执行尾部插入
    //==================set==============================
    void SAdd(    const std::string& set_key,std::vector<std::string> vec,const int iDeep = 0);
     void SRem(   const std::string& set_key,std::vector<std::string> vec,const int iDeep = 0 );
     void SCard(    const std::string& set_key,const int iDeep = 0);
     void SMembers(   const std::string& set_key,const int iDeep = 0);
     void SUnionStore( const std::string& set_key,std::vector<std::string> vec,const int iDeep = 0 );
     void SUnion( std::vector<std::string> vec,const int iDeep = 0);
     void SInterStore( const std::string& set_key,std::vector<std::string> vec,const int iDeep = 0 );
     void SInter( std::vector<std::string> vec, const int iDeep = 0);
     void SDiffStore(  const std::string& set_key ,std::vector<std::string> vec,const int iDeep = 0);
     void SDiff(std::vector<std::string> vec,  const int iDeep = 0);
     void SIsMember(  const std::string& set_key,std::string& field_key,const int iDeep = 0);
     void SPop(  const std::string& set_key,  const int iDeep = 0);
     void SRandMember(   const std::string& set_key,const int iDeep = 0);
     void SMove( const std::string& src_key,const std::string& dest_key,string field,const int iDeep = 0 );
    //==================zset==============================
void ZAdd( const std::string & zset_key,std::vector<std::string> vec,const int iDeep = 0 );//一个或多个
void ZCard(  const std::string & zset_key,const int iDeep = 0 );//元素数量
void ZCount(  const std::string & zset_key,std::string score1,std::string score2 ,const int iDeep = 0 );//有序集合中，在区间内的数量
void ZRange(  const std::string & zset_key,std::string start,std::string end ,const int iDeep = 0 );//有序集合区间内的成员
void ZIncrby(  const std::string & zset_key,std::string field_key,std::string score ,const int iDeep = 0 );//为score增加或减少
void ZUnionStore(  const std::string & zset_key,std::vector<std::string> vec ,const int iDeep = 0 );//并集不输出
void ZInterStore(  const std::string & zset_key,std::vector<std::string> vec,const int iDeep = 0 );//交集不输出
void ZRangebyscore(  const std::string & zset_key,std::string start,std::string end,const int iDeep = 0 );
void ZRem(  const std::string & zset_key,std::vector<std::string> vec,const int iDeep = 0 );//移除有序集 key 中的一个或多个成员
void ZRank(   const std::string & zset_key,std::string field_key,const int iDeep = 0 );//返回member的排名，
void ZRevrank(   const std::string & zset_key,std::string field_key,const int iDeep = 0 );//member逆序排名
void ZScore(   const std::string & zset_key,std::string field_key,const int iDeep = 0 );//member的score
void ZREVRange(   const std::string & zset_key,std::string start ,std::string end ,const int iDeep = 0 );
void ZREVRangebylscore(  const std::string & zset_key,string score1,string score2,const int iDeep = 0 );
void ZRemrangebyrank(  const std::string & zset_key,const std::string rank1,std::string rank2,const int iDeep = 0 );
void ZRemrangebyscore(  const std::string & zset_key,const std::string score1,const std::string score2 ,const int iDeep = 0);

private:
    std::shared_ptr<PhxKVServer::Stub> stub_;
};





