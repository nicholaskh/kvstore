
#pragma once
#include <string>
#include "def.h"
#include "log.h"
using namespace std;
//对复杂类型的meta进行编码

enum DataType {
    STRING_TYPE    = 1,
    HASH_TYPE      = 2,
    HASH_META      = 61,
    LIST_TYPE      = 3,
    LIST_META      = 62,
    SET_TYPE       = 4,
    SET_META      = 63,
    ZSET_TYPE     = 5,
    ZSET_SCORE     = 6,
    ZSET_META     = 64,
};

class kv_encode
{
private:
    /* data */
public:
    kv_encode(/* args */);
    ~kv_encode();
    //字符串相关
    static string EncodeStringKey( const string& key );//编码key
    static string EncodeStringValue( const string& value,const __uint32_t ttl );//编码value
    
    static void DecodeStringValue(const string& value, __uint32_t& ttl,  string& field_key);//string decode ttl

    //公用方法
    static string EncodeMetaKey(const string& key,const int type);//根据类型编码元信息的key部分
    static void DecodeMetaKey(const string& meta_key, std::string& key );//根据解码后的实际key 和类型

    static string EncodeKey(const string& key,const int type);//只有type+key size + key，用来进行迭代查询
    static void DecodeKey(const string& value, string& key);//只得到 key，不要field


    

    //编码hash key value 
    static string EncodeHashMetaValue( const __uint32_t ttl,const __uint64_t length );
    static void DecodeHashMetaValue( const string& value, __uint32_t& ttl, __uint64_t& length );

    static string EncodeHashFieldKey(const string& key,const string& field_key);
    static void DecodeHashFieldKey(const string& key, string & hash_key , string& field_key);
    
    
    //===========list=============================
    
    static string EncodeListMetaValue( const __uint32_t ttl,const __uint64_t length,const __uint64_t list_head,
                                        const __uint64_t list_tail,const __uint64_t list_next_seq );
    static void DecodeListMetaValue( const string& value, __uint32_t& ttl, __uint64_t& length , __uint64_t& list_head,
                                         __uint64_t& list_tail, __uint64_t& list_next_seq );

    static string EncodeListFieldKey(const string& key,const __uint64_t& field_seq);
    static string EncodeListFieldValue(const __uint64_t& pre_seq,const __uint64_t& next_seq,const string& field_value);
    static void DecodeListFieldValue(const string& key,__uint64_t& pre_seq,__uint64_t& next_seq,string& field_value);

    //==================set========================
    static string EncodeSetMetaValue( const __uint32_t ttl,const __uint64_t length );
    static void DecodeSetMetaValue( const string& value, __uint32_t& ttl, __uint64_t& length );
    static string EncodeSetFieldKey(const string& key,const string& field_key);
    static void DecodeSetFieldKey(const string& key, string & set_key , string& field_key);//找到key field 字段
    //==================zset======================
    static string EncodeZSetMetaValue( const __uint32_t ttl,const __uint64_t length );
    static void DecodeZSetMetaValue( const string& value, __uint32_t& ttl, __uint64_t& length );

    static string EncodeZSetFieldKey(const string& key,const string& field_key);
    static void DecodeZSetFieldKey(const string& key, string& field_key);

    static string EncodeZSetFieldValue(const __int64_t score);//int64 score 转string
    static void DecodeZSetFieldValue(const string& key, __int64_t & score);// 字符串转 int64 score

    static string EncodeZSetScoreKey(const string& key,const __int64_t& score);
    static void DecodeZSetScoreKey(const string& key,string& field_key, __int64_t& score);//从score头中得到score,field_key
     
};


