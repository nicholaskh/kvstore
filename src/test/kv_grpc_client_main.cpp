

#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include "util.h"
#include <grpc++/grpc++.h>
#include "phxkv.pb.h"
#include "phxkv.grpc.pb.h"
#include "KVClient.h"
#include <memory>
#include <thread>
#include <unistd.h>
#include "rocksdb/db.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace phxpaxos;
using namespace std;
using namespace phxkv;
 

void Put(PhxKVClient & oPhxKVClient, const string & sKey, const string & sValue,int ms ,int groupid )
{
    oPhxKVClient.Put(sKey, sValue,ms , groupid);    
}


void GetGlobal(PhxKVClient & oPhxKVClient,  string & sKey,int groupid)
{
    string sReadValue;
    uint64_t iReadVersion = 0;
    int ret = oPhxKVClient.Get(sKey, sReadValue, groupid);
    if (ret == 0)
    {
        /*printf("GetGlobal ok, key %s value %s \n", 
                sKey.c_str(), sReadValue.c_str());*/
    }
    else if (ret == 1)
    {
        printf("GetGlobal key %s not exist\n", sKey.c_str());
    }
    else if (ret == 101)
    {
        printf("GetGlobal no master\n");
    }
    else
    {
        printf("GetGlobal fail, ret %d key %s\n", ret, sKey.c_str());
    }
}

void GetLocal(PhxKVClient & oPhxKVClient,  string & sKey)
{
    string sReadValue;
    uint64_t iReadVersion = 0;
    int ret = oPhxKVClient.GeLocal(sKey, sReadValue,0 );
    if (ret == 0)
    {
        /*printf("GetGlobal ok, key %s value %s \n", 
                sKey.c_str(), sReadValue.c_str());*/
    }
    else if (ret == 1)
    {
        printf("GetGlobal key %s not exist\n", sKey.c_str());
    }
    else if (ret == 101)
    {
        printf("GetGlobal no master\n");
    }
    else
    {
        printf("GetGlobal fail, ret %d key %s\n", ret, sKey.c_str());
    }
}
void DropMaster( PhxKVClient & oPhxKVClient, int num )
{
    oPhxKVClient.dropMast( num );
}
void BatchPut(PhxKVClient & oPhxKVClient,std::vector<string> vec_keys_values,int gd)
{
    int ret = oPhxKVClient.BatchPut(vec_keys_values,gd );
    if (ret == 0)
    {
        printf("BatchPut ok,  " );
    }
    else if (ret == -11)
    {
        printf("BatchPut version conflict,  " );
    }
    else
    {
        printf("BatchPut fail,  " );
    }
}

void Hset(PhxKVClient & oPhxKVClient,std::string hash_key, std::string field_key)
{
    oPhxKVClient.Hset(hash_key,field_key );
}

void Hget(PhxKVClient & oPhxKVClient,std::string hash_key, std::string field_key)
{
    oPhxKVClient.Hget(hash_key,field_key );
}

void Hdel(PhxKVClient & oPhxKVClient,std::string hash_key, std::string field_key)
{
    oPhxKVClient.Hdel(hash_key,field_key );
}

void HashGetAll(PhxKVClient & oPhxKVClient,std::string hash_key )
{
    oPhxKVClient.HashGetAll( hash_key );
}

void HashExist(PhxKVClient & oPhxKVClient, const std::string hash_key ,const string field_key )
{
    oPhxKVClient.HashExist(hash_key, field_key );
}
void HashIncrByInt(PhxKVClient & oPhxKVClient, const std::string hash_key ,const string& field_key,const string num,const int iDeep = 0)
{
    oPhxKVClient.HashIncrByInt(hash_key, field_key,num );
}
void HashIncrByFloat(PhxKVClient & oPhxKVClient, const std::string hash_key ,const string& field_key,const string num,const int iDeep = 0)
{
    oPhxKVClient.HashIncrByFloat(hash_key, field_key,num );
}
void HashKeys(PhxKVClient & oPhxKVClient, const std::string hash_key   )
{
    oPhxKVClient.HashKeys(hash_key);
}
void HashLen(PhxKVClient & oPhxKVClient, const std::string hash_key   )
{
    oPhxKVClient.HashLen(hash_key);
}
void HashMget(PhxKVClient & oPhxKVClient, const std::string hash_key ,const std::vector<string> vec )
{
    oPhxKVClient.HashMget( hash_key, vec);
}
void HashMSet(PhxKVClient & oPhxKVClient, const std::string hash_key ,const std::vector<string> vec )
{
    oPhxKVClient.HashMSet( hash_key, vec);
}
void HashSetNx( PhxKVClient & oPhxKVClient,const std::string hash_key,const string field_key  )
{
    oPhxKVClient.HashSetNx(hash_key,field_key );
}
void HashValues(PhxKVClient & oPhxKVClient, const std::string hash_key   )
{
    oPhxKVClient.HashValues( hash_key );
}
//=================list================================================
void ListLpush(PhxKVClient & oPhxKVClient, std::string list_key, std::vector<string>& field_key)
{
    oPhxKVClient.ListLpush( list_key, field_key);
}
void ListLpushx(PhxKVClient & oPhxKVClient, std::string list_key, string& field_key)
{
    oPhxKVClient.ListLpushx( list_key, field_key);
}
void ListLpop(PhxKVClient & oPhxKVClient, std::string list_key )
{
    oPhxKVClient.ListLpop( list_key );
}
void ListLength(PhxKVClient & oPhxKVClient, std::string list_key )
{
    oPhxKVClient.ListLength( list_key );
}
void ListRpop( PhxKVClient & oPhxKVClient,  std::string list_key )//弹出表尾
{
    oPhxKVClient.ListRpop( list_key );
}
void ListRpopLpush(PhxKVClient & oPhxKVClient, std::string list_src,std::string list_dest )//尾部进行头插
{
    oPhxKVClient.ListRpopLpush( list_src,list_dest );
}
void ListIndex(  PhxKVClient & oPhxKVClient, std::string list_key,int index)//插入到指定位置
{
    oPhxKVClient.ListIndex( list_key,index );
}
void ListInsert( PhxKVClient & oPhxKVClient, std::string list_key , string field,int pos,string new_value)//插入到指定位置
{
    oPhxKVClient.ListInsert( list_key ,field , pos,new_value);
}
void ListRange(  PhxKVClient & oPhxKVClient,  std::string list_key,int start,int end)
{
    oPhxKVClient.ListRange(list_key ,start,end );
}
void ListRem(  PhxKVClient & oPhxKVClient,  std::string list_key,const string & des_value ,const int cnt)//删除N个等值元素
{
oPhxKVClient.ListRem( list_key ,des_value , cnt );
}
void ListSet( PhxKVClient & oPhxKVClient, std::string list_key ,std::string field,int index )//根据下表进行更新元素
{
oPhxKVClient.ListSet( list_key ,field ,index );
}
void ListTtim(  PhxKVClient & oPhxKVClient,  std::string list_key,int start,int end )//保留指定区间元素，其余删除
{
oPhxKVClient.ListTtim( list_key,start,end );
}
void ListRpush(  PhxKVClient & oPhxKVClient,  std::string list_key, std::vector<string>& field_key )//尾插法
{
    oPhxKVClient.ListRpush(list_key, field_key);
}
void ListRpushx(  PhxKVClient & oPhxKVClient,  std::string list_key, string& field_key )//链表存在时，才执行尾部插入
{
    oPhxKVClient.ListRpushx(list_key, field_key);
}
//===================set===================================
void SAdd( PhxKVClient & oPhxKVClient,    const std::string& set_key,std::vector<std::string> vec)
{
    oPhxKVClient.SAdd(set_key, vec);
}
void SRem( PhxKVClient & oPhxKVClient,   const std::string& set_key,std::vector<std::string> vec  )
{
    oPhxKVClient.SRem(set_key, vec);
}
void SCard( PhxKVClient & oPhxKVClient,    const std::string& set_key )
{
    oPhxKVClient.SCard(set_key);
}
void SMembers( PhxKVClient & oPhxKVClient,   const std::string& set_key )
{
    oPhxKVClient.SMembers(set_key);
}
void SUnionStore(PhxKVClient & oPhxKVClient,  const std::string& set_key,std::vector<std::string> vec  )
{
    oPhxKVClient.SUnionStore(set_key ,vec );
}
void SUnion( PhxKVClient & oPhxKVClient, std::vector<std::string> vec )
{
    oPhxKVClient.SUnion(vec );
}
void SInterStore(PhxKVClient & oPhxKVClient,  const std::string& set_key,std::vector<std::string> vec )
{
     oPhxKVClient.SInterStore(set_key ,vec );
}
void SInter( PhxKVClient & oPhxKVClient, std::vector<std::string> vec )
{
     oPhxKVClient.SInter(vec );
}
void SDiffStore( PhxKVClient & oPhxKVClient,  const std::string& set_key ,std::vector<std::string> vec )
{
     oPhxKVClient.SDiffStore(set_key ,vec );
}
void SDiff(PhxKVClient & oPhxKVClient, std::vector<std::string> vec )
{
     oPhxKVClient.SDiff(vec );
}
void SIsMember( PhxKVClient & oPhxKVClient,  const std::string& set_key,std::string& field_key )
{
     oPhxKVClient.SIsMember(set_key , field_key  );
}
void SPop( PhxKVClient & oPhxKVClient,  const std::string& set_key)
{
     oPhxKVClient.SPop(set_key );
}
void SRandMember( PhxKVClient & oPhxKVClient,   const std::string& set_key)
{
     oPhxKVClient.SRandMember(set_key );
}
void SMove(PhxKVClient & oPhxKVClient,const std::string& src_key,  const std::string& dest_key, string field_key )
{
     oPhxKVClient.SMove(src_key ,dest_key,  field_key );
}

//====================zset===========================
void ZAdd( PhxKVClient & oPhxKVClient,const std::string & zset_key,std::vector<std::string> vec)//一个或多个
{
    oPhxKVClient.ZAdd(zset_key, vec );
}
void ZCard( PhxKVClient & oPhxKVClient,const std::string & zset_key)//元素数量
{
    oPhxKVClient.ZCard(zset_key  );
}
void ZCount( PhxKVClient & oPhxKVClient,const std::string & zset_key,std::string score1,std::string score2 )//有序集合中，在score内的数量
{
    oPhxKVClient.ZCount(zset_key ,score1,score2 );
}
void ZRange( PhxKVClient & oPhxKVClient,const std::string & zset_key,std::string start,std::string end )//有序集合区间内的成员
{
    oPhxKVClient.ZRange(zset_key ,start,end );
}
void ZIncrby(PhxKVClient & oPhxKVClient,   const std::string & zset_key,std::string field_key,std::string score )//为score增加或减少
{
    oPhxKVClient.ZIncrby(zset_key ,field_key,score );//=======notice======
}
void ZUnionStore(PhxKVClient & oPhxKVClient, const std::string & zset_key,std::vector<std::string> vec )//并集不输出
{
    oPhxKVClient.ZUnionStore(zset_key ,vec );
}
void ZInterStore( PhxKVClient & oPhxKVClient,const std::string & zset_key,std::vector<std::string> vec)//交集不输出
{
    oPhxKVClient.ZInterStore(zset_key ,vec );
}
void ZRangebyscore(PhxKVClient & oPhxKVClient, const std::string & zset_key,std::string start,std::string end)
{
    oPhxKVClient.ZRangebyscore(zset_key ,start, end );
}
void ZRem( PhxKVClient & oPhxKVClient, const std::string & zset_key,std::vector<std::string> vec)//移除有序集 key 中的一个或多个成员
{
    oPhxKVClient.ZRem(zset_key ,vec );
}
void ZRank( PhxKVClient & oPhxKVClient, const std::string & zset_key,std::string field_key)//返回member的排名，
{
    oPhxKVClient.ZRank(zset_key ,field_key );
}
void ZRevrank( PhxKVClient & oPhxKVClient, const std::string & zset_key,std::string field_key)//member逆序排名
{
    oPhxKVClient.ZRevrank(zset_key ,field_key );
}
void ZScore(PhxKVClient & oPhxKVClient, const std::string & zset_key,std::string field_key)//member的score
{
    oPhxKVClient.ZScore(zset_key ,field_key );
}
void ZREVRange( PhxKVClient & oPhxKVClient, const std::string & zset_key,std::string start ,std::string end )//指定区间成员，逆序输出
{
    oPhxKVClient.ZREVRange(zset_key ,start, end);
}
void ZREVRangebylscore( PhxKVClient & oPhxKVClient,const std::string & zset_key,string score1,string score2)
{
    oPhxKVClient.ZREVRangebylscore(zset_key ,score1, score2);
}
void ZRemrangebyrank( PhxKVClient & oPhxKVClient,const std::string & zset_key,const std::string rank1,std::string rank2)
{
    oPhxKVClient.ZRemrangebyrank(zset_key ,rank1, rank2);
}
void ZRemrangebyscore( PhxKVClient & oPhxKVClient, const std::string & zset_key,const std::string score1,const std::string score2)
{
    oPhxKVClient.ZRemrangebyscore(zset_key ,score1, score2);
}
/////////////////////////////
int main(int argc, char ** argv)
{

     /*rocksdb::DB * m_rocksdb=NULL;
    rocksdb::Options oOptions;
    oOptions.create_if_missing = true;
    rocksdb::Status oStatus = rocksdb::DB::Open(oOptions, "/usr/local/temp_data/", &m_rocksdb );
    if (!oStatus.ok())
    {
        printf("error....");
    }
    printf("step1...");
    rocksdb::WriteOptions oLevelDBWriteOptions;
    m_rocksdb->Put(oLevelDBWriteOptions,"test_key1", "value1");
    m_rocksdb->Put(oLevelDBWriteOptions,"1111111", "value4");
    m_rocksdb->Put(oLevelDBWriteOptions,"222222", "value4");
    m_rocksdb->Put(oLevelDBWriteOptions,"333333", "value4");
    m_rocksdb->Put(oLevelDBWriteOptions,"444444", "value4");
    m_rocksdb->Put(oLevelDBWriteOptions,"55555", "value4");
    m_rocksdb->Put(oLevelDBWriteOptions,"test_key2", "value2");
     printf("step2...");
     string strStartKey = "test_";
    rocksdb::ReadOptions tROpt;
    std::unique_ptr<rocksdb::Iterator> iter(m_rocksdb->NewIterator(tROpt));
    for (iter->Seek(strStartKey); iter->Valid(); iter->Next()) {
        cout << iter->key().ToString()<<"    " << iter->value().ToString() << endl;
    }
    return 0;*/

    string sServerAddress = argv[1]; 
    PhxKVClient oPhxKVClient(grpc::CreateChannel(sServerAddress, grpc::InsecureChannelCredentials()));

    string sFunc = argv[2];//操作类型    
    if (sFunc == "put")
    {
        string sKey = argv[3];//key
        string sValue = argv[4];
        int ms = atoi(argv[5] );//超时时间
        Put(oPhxKVClient, sKey, sValue , ms , 0 );

        /*for(int i=1;i<10000;i++ ){
            stringstream ss;
            ss<<"_"<<i;
            string key = sKey+ss.str();
            string value = sValue+ss.str();
            Put(oPhxKVClient, key, value, ms,0);
        }*/
        
    }else if(sFunc == "test_log_put"){
        string sKey = argv[3];
        string sValue = argv[4];
        int ms = atoi(argv[5] );//超时时间
        int cnt = atoi(argv[6]);

        for(int i=0;i<cnt;i++ ){
            stringstream ss;
            ss<<"_"<<i;
            string key = sKey+ss.str();
            string value = sValue+ss.str();
            Put(oPhxKVClient, key, value, ms,i%32 );
        }
    }else if(sFunc == "test_log_get"){
        string sKey = argv[3];//key
        int cnt = atoi(argv[4]);
        for(int i=0;i<cnt ;i++ ){
             stringstream ss;
             ss<<"_"<<i;
             string key = sKey+ss.str();
             GetGlobal(oPhxKVClient, key , 0);
        }
    }
    else if (sFunc == "get")
    {
        string sKey = argv[3];//key
        
        GetGlobal(oPhxKVClient, sKey , 0);
        /*for(int i=1;i<10000 ;i++ ){
             stringstream ss;
             ss<<"_"<<i;
             string key = sKey+ss.str();
             GetGlobal(oPhxKVClient, key , 0);
        }*/
    }else if(sFunc == "getlocal"){
        string sKey = argv[3];//key
        int key_num = atoi( argv[4] );// num
        GetLocal( oPhxKVClient, sKey );

        for(int i=1;i<key_num ;i++ ){
             stringstream ss;
             ss<<"_"<<i;
             string key = sKey+ss.str();
             GetLocal(oPhxKVClient, key );
        }
    }else if(sFunc == "batchput"){

        string keys = argv[3];
        std::vector<string> vec_key_value;
        StringUtil::splitstr(keys, ",",vec_key_value );
        if( vec_key_value.size() == 0 ){
            printf("vec_key_value size 0 ");
            return 0;
        }
        BatchPut( oPhxKVClient ,vec_key_value,0 );
    }else if(sFunc == "hset"){//====
        
        string hash_key =argv[3] ;// ./ ip hset hash k1:v1 
        string fields = argv[4];
        Hset( oPhxKVClient, hash_key, fields);

    }else if(sFunc == "hget"){//====
        string hash_key = argv[3] ;// ./ ip hget hash k1 
        string field_key = argv[4];

        Hget(oPhxKVClient, hash_key ,field_key );
    }else if(sFunc=="hdel"){
        string hash_key = argv[3] ;// ./ ip hdel hash k1,k2,k3 groupid  
        string field_key = argv[4];
        
        Hdel(oPhxKVClient,hash_key, field_key );
    }else if(sFunc=="hgetall"){
        string hash_key = argv[3] ;// ./ ip hgetall hashtable 

        HashGetAll(oPhxKVClient, hash_key );
    }else if(sFunc=="hexist"){    // ./kvtools 10.90.81.9:8000 hexist hashtable key
         string hash_key = argv[3] ;
         string field_key = argv[4];
         HashExist(oPhxKVClient, hash_key,field_key );
    }else if(sFunc=="hkeys"){    // ./kvtools 10.90.81.9:8000 hkeys hashtable key 
         string hash_key = argv[3] ;
         
         HashKeys(oPhxKVClient, hash_key );
    }else if(sFunc=="hlength"){    // ./kvtools 10.90.81.9:8000 hlength hashtable key 
         string hash_key = argv[3] ;
          
         HashLen(oPhxKVClient, hash_key );
    }else if(sFunc=="hmget"){    // ./kvtools 10.90.81.9:8000 hmget hashtable k1,k2,k3
         string hash_key = argv[3] ;
         string field_key = argv[4];
         std::vector<string> vec;
         StringUtil::splitstr(field_key, ",",vec );
         HashMget(oPhxKVClient, hash_key,vec );
    }else if(sFunc=="hvalues"){    // ./kvtools 10.90.81.9:8000 hvalues hashtable
         string hash_key = argv[3] ;
         HashValues(oPhxKVClient, hash_key );
    }else if(sFunc=="hsetnx"){  //  ./kvtools 10.90.81.9:8000 hsetnx hashtable k:v 
        string hash_key = argv[3] ;
        string fields = argv[4];
        HashSetNx( oPhxKVClient, hash_key, fields );
    }else if(sFunc=="hmset"){   //   ./ ip hmset hashtable k1:v1,k2:v2 
        string hash_key = argv[3] ;
        string field = argv[4];
        std::vector<string> vec;
         StringUtil::splitstr(field, ",",vec );
         HashMSet(oPhxKVClient, hash_key,vec  );
    }else if(sFunc=="hint"){   //   ./ ip hint hashtable k1 num1
        string hash_key = argv[3] ;
        string field_key = argv[4];
        string int_num = argv[5];
         HashIncrByInt(oPhxKVClient, hash_key,field_key, int_num );
    }else if(sFunc=="hfloat"){   //   ./ ip hmset hashtable k1:v1,k2:v2 
        string hash_key = argv[3] ;
        string field_key = argv[4];
        string float_num = argv[5];
         HashIncrByFloat(oPhxKVClient, hash_key,field_key, float_num );
    }//list操作
    else if(sFunc == "llpush"){  //   ./ ip llpush list k1
        string list_key = argv[3] ;
        string field_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        ListLpush(oPhxKVClient, list_key,vec );

    }else if(sFunc == "llpushx"){   //   ./ ip llpushx list k1

        string list_key = argv[3] ;
        string field_key = argv[4];
        ListLpushx(oPhxKVClient, list_key,field_key );
    }else if(sFunc == "llpop"){      //   ./ ip llpop list

        string list_key = argv[3] ;
        
        ListLpop(oPhxKVClient, list_key );
    }else if(sFunc == "llength"){     //   ./ ip llength list

        string list_key = argv[3] ;
        ListLength(oPhxKVClient, list_key );
    }else if(sFunc == "lrpop"){      //   ./ ip lrpop list

         string list_key = argv[3] ;
        ListRpop(oPhxKVClient, list_key );
    }else if(sFunc == "lrpoplpush"){   //   ./ ip lrpop listsrc listdest

         string list_src = argv[3] ;
        string list_dest = argv[4] ;
        ListRpopLpush(oPhxKVClient, list_src, list_dest );
    }else if(sFunc == "lindex"){      //   ./ ip lindex list  0

        string list_key = argv[3] ;
        int index = atoi(argv[4]);
        ListIndex(oPhxKVClient, list_key,index );
    }else if(sFunc == "linsert"){     

        string list_key = argv[3] ;
        string field =  argv[4] ;
        int flag =atoi( argv[5]);// 1 
        string new_value = argv[6] ;
        ListInsert(oPhxKVClient, list_key,field,flag,new_value );
    }else if(sFunc == "lrange"){
        
        string list_key = argv[3] ;
        int start =atoi( argv[4]);
        int end =atoi( argv[5]);
        ListRange(oPhxKVClient, list_key,start,end );
    }else if(sFunc == "lrem"){

        string list_key = argv[3] ;
        string field_key = argv[4] ;
        int cnt =atoi( argv[5]);
        ListRem(oPhxKVClient, list_key,field_key,cnt );
    }else if(sFunc == "lset"){ 

        string list_key = argv[3] ;
        int index =atoi( argv[4]);
        string field_key = argv[5] ;
        
        ListSet(oPhxKVClient, list_key,field_key,index );
    }else if(sFunc == "ltrim"){

        string list_key = argv[3] ;
        int start =atoi( argv[4]);
        int end =atoi( argv[5]);
        ListTtim(oPhxKVClient, list_key,start,end );
    }else if(sFunc == "lrpush"){   //   ./ ip lrpush list k1

        string list_key = argv[3] ;
        string field_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );

        ListRpush(oPhxKVClient, list_key,vec );
    }else if(sFunc == "lrpushx"){  //   ./ ip lrpushx list k1

        string list_key = argv[3] ;
        string field_key = argv[4];
        ListRpushx(oPhxKVClient, list_key,field_key );
    }
    else if(sFunc == "sadd"){  //   ./ ip sadd set1 k1,k2,k3

        string set_key = argv[3] ;
        string field_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        SAdd(oPhxKVClient, set_key,vec );
    }else if(sFunc == "scard" ){      //   ./ ip scard set1 

        string set_key = argv[3] ;
        SCard(oPhxKVClient, set_key );
    }else if(sFunc == "srem"){  //   ./ ip srem set1 k1,k2,k3

         string set_key = argv[3] ;
        string field_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        SRem(oPhxKVClient, set_key,vec );
    }else if(sFunc == "smembers"){  //   ./ ip smembers set1 

        string set_key = argv[3] ;
        SMembers(oPhxKVClient, set_key );
    }else if(sFunc == "sunionstore"){  //   ./ ip sunionstore set1 set2,set3

        string set_key = argv[3] ;
        string field_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        SUnionStore(oPhxKVClient, set_key,vec );
    }else if(sFunc == "sunion"){  //   ./ ip sunion set1,set2

        string field_key = argv[3];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        SUnion(oPhxKVClient,vec );
    }else if(sFunc == "sinter"){  //   ./ ip sinter  set1 set2

         string field_key = argv[3];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        SInter(oPhxKVClient,vec );
    }else if(sFunc == "sinterstore"){  //   ./ ip sinterstore set1 set2,set3

         string set_key = argv[3] ;
        string field_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        SInterStore(oPhxKVClient, set_key,vec );
    }else if(sFunc == "sdiff"){  //   ./ ip sdiff set1 set2

         string field_key = argv[3];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        SDiff(oPhxKVClient,vec );
    }else if(sFunc == "sdiffstore"){  //   ./ ip sdiffstore set1 set2,set3 

         string set_key = argv[3] ;
        string field_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        SDiffStore(oPhxKVClient, set_key,vec );
    }else if(sFunc == "sismember"){  //   ./ ip sismember set1 k1

          string set_key = argv[3] ;
          string field_key = argv[4] ;
        SIsMember(oPhxKVClient, set_key,field_key );
    }else if(sFunc == "spop"){  //   ./ ip spop set1

         string set_key = argv[3] ;
        SPop(oPhxKVClient, set_key );
    }else if(sFunc == "srangemember"){  //   ./ ip srangemember set1

          string set_key = argv[3] ;
        SRandMember(oPhxKVClient, set_key );
    }else if(sFunc == "smove"){  //   ./ ip smove set1 set2 k1

        string src_key = argv[3] ;
        string dest_key = argv[4] ;
        string field_key = argv[5] ;
        SMove(oPhxKVClient,src_key,dest_key ,  field_key );
    }else if(sFunc == "zadd"){       // ./ ip zadd set1 k1:score1,k2:score2                                     //////////////////////////////

        string zset_key = argv[3] ;
        string field_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        ZAdd(oPhxKVClient, zset_key,vec );
    }else if(sFunc == "zcard"){       // ./ ip zadd set1                                   //////////////////////////////

        string zset_key = argv[3] ;
        ZCard(oPhxKVClient, zset_key );
    }else if(sFunc == "zcount"){       // ./ ip zcount set1 score1 score2                                  //////////////////////////////

        string zset_key = argv[3] ;
        string start = argv[4];
        string end = argv[5];
        ZCount(oPhxKVClient, zset_key,start , end );
    }else if(sFunc == "zrange"){       // ./ ip zcount set1 score1 score2                                  //////////////////////////////

        string zset_key = argv[3] ;
        string start = argv[4];
        string end = argv[5];
        ZRange(oPhxKVClient, zset_key,start , end );
    }else if(sFunc == "zincrby"){       // ./ ip zincrby set1 k1 score                                 //////////////////////////////

        string zset_key = argv[3] ;
        string field_key = argv[4];
        string score = argv[5];
        ZIncrby(oPhxKVClient, zset_key,field_key , score );
    }else if(sFunc == "zunionstore"){       // ./ ip zcount set1 set2,set3                                  //////////////////////////////

        string zset_key = argv[3] ;
        string src_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(src_key, ",",vec  );

        ZUnionStore(oPhxKVClient, zset_key,vec );
    }else if(sFunc == "zinterstore"){       // ./ ip zcount set1 score1 score2                                  //////////////////////////////

        string zset_key = argv[3] ;
        string src_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(src_key, ",",vec  );
        ZInterStore(oPhxKVClient, zset_key,vec );
    }else if(sFunc == "zrangebyscore"){       // ./ ip zrangebyscore set1 start end                                  //////////////////////////////

        string zset_key = argv[3] ;
        string start = argv[4];
        string end = argv[5];
        ZRangebyscore(oPhxKVClient, zset_key,start , end );
    }else if(sFunc == "zrem"){       // ./ ip zrem set1 k1,k2,k3                              //////////////////////////////

        string zset_key = argv[3] ;
        string field_key = argv[4];
        std::vector<string> vec ;
        StringUtil::splitstr(field_key, ",",vec  );
        ZRem(oPhxKVClient, zset_key,vec );
    }else if(sFunc == "zrank"){       // ./ ip zcount set1 k1                               //////////////////////////////

        string zset_key = argv[3] ;
        string field_key = argv[4];
        ZRank(oPhxKVClient, zset_key,field_key );
    }else if(sFunc == "zrevrank"){       // ./ ip zrevrank set1 k1                                  //////////////////////////////

        string zset_key = argv[3] ;
        string field_key = argv[4];
        ZRevrank(oPhxKVClient, zset_key,field_key );
    }else if(sFunc == "zscore"){       // ./ ip zscore set1 k1                                 //////////////////////////////

        string zset_key = argv[3] ;
        string field_key = argv[4];
        
        ZScore(oPhxKVClient, zset_key,field_key );
    }else if(sFunc == "zrevrange"){       // ./ ip zrevrange set1 start end                                  //////////////////////////////

        string zset_key = argv[3] ;
        string start = argv[4];
        string end = argv[5];
        ZREVRange(oPhxKVClient, zset_key,start , end );
    }else if(sFunc == "zrevrangebylscore"){       // ./ ip zrevrangebylscore set1 score1 score2                                  //////////////////////////////

        string zset_key = argv[3] ;
        string score1 = argv[4];
        string score2 = argv[5];
        ZREVRangebylscore(oPhxKVClient, zset_key,score1 , score2 );
    }else if(sFunc == "zremrangebyrank"){       // ./ ip zremrangebyrank set1 rank1 rank2                                  //////////////////////////////

        string zset_key = argv[3] ;
        string rank1 = argv[4];
        string rank2 = argv[5];
        ZRemrangebyrank(oPhxKVClient, zset_key,rank1 , rank2 );
    }else if(sFunc == "zremrangebyscore"){       // ./ ip zremrangebyscore set1 score1 score2                                  //////////////////////////////

        string zset_key = argv[3] ;
        string score1 = argv[4];
        string score2 = argv[5];
        ZRemrangebyscore(oPhxKVClient, zset_key,score1 , score2 );
    }
    return 0;
}

