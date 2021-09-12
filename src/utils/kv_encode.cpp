
#include "kv_encode.h"
#include <string.h>




#define HASH_META_VALUE_LENGTH  ( sizeof(__uint32_t)+ sizeof(__uint64_t) )  //meta value 类型长度
#define LIST_META_VALUE_LENGTH  ( sizeof(__uint32_t)+ sizeof(__uint64_t)+ sizeof(__uint64_t)+ sizeof(__uint64_t)+ sizeof(__uint64_t) )  //meta value 类型长度
#define SET_META_VALUE_LENGTH  ( sizeof(__uint32_t)+ sizeof(__uint64_t) )  //meta value 类型长度
#define ZSET_META_VALUE_LENGTH  ( sizeof(__uint32_t)+ sizeof(__uint64_t) )  //meta value 类型长度

kv_encode::kv_encode()
{
}

kv_encode::~kv_encode()
{
}

string kv_encode::EncodeStringKey( const string& key )
{
    string buff="";
    char sztype[1];
    __uint8_t data_type = STRING_TYPE;
    memcpy(sztype,&data_type, sizeof(__uint8_t) );
    buff.append(sztype,1 );
    
    buff.append( key.data(), key.size() );
    return buff;
}
string kv_encode::EncodeStringValue( const string& value,const __uint32_t ttl )
{
    string buff="";
    char szttl[sizeof(uint32_t)];
    memcpy(szttl,&ttl, sizeof(uint32_t) );
    buff.append(szttl,4 );
    buff.append( value.data(), value.size() );
    return buff;
}

void kv_encode::DecodeStringValue(const string& value, __uint32_t& ttl,  string& field_key)
{
    memcpy( &ttl , value.c_str() , sizeof(__uint32_t) );
   field_key.assign(value.c_str()+ sizeof(__uint32_t) , value.size()- sizeof(__uint32_t));

}

string kv_encode::EncodeHashMetaValue( const __uint32_t ttl,const __uint64_t length )
{
    char sz[HASH_META_VALUE_LENGTH];
    memcpy( sz,  &ttl, sizeof(__uint32_t) );
    memcpy( sz+ sizeof(__uint32_t),  &length, sizeof(__uint64_t) );

    string buff="";
    buff.append(sz,HASH_META_VALUE_LENGTH );
    return buff;
}


void kv_encode::DecodeHashMetaValue( const string& value, __uint32_t& ttl, __uint64_t& length )
{
    memcpy( &ttl , value.c_str() , sizeof(__uint32_t) );
    memcpy( &length , value.c_str()+ sizeof(__uint32_t), sizeof(__uint64_t) );
}

string kv_encode::EncodeHashFieldKey(const string& key,const string& field_key)
{
    string buff="";

    char sztype[1];
    __uint8_t data_type = HASH_TYPE;
    memcpy(sztype,&data_type, sizeof(__uint8_t) );
    buff.append(sztype,1 );

    char szsize[1];
    __uint8_t length = key.length();
    memcpy(szsize,&length, sizeof(__uint8_t) );
    buff.append(szsize,1 );
    
    buff.append( key.data(), key.size() );
    buff.append( field_key.data(), field_key.size() );
    return buff;
}

void kv_encode::DecodeHashFieldKey(const string& key, string & hash_key , string& field_key)
{
    __uint8_t key_size = 0;
    memcpy( &key_size , key.c_str()+sizeof(__uint8_t) , sizeof(__uint8_t) );
   
   hash_key.assign(key.c_str()+ sizeof(__uint8_t)*2, key_size );

   field_key.assign(key.c_str()+ sizeof(__uint8_t)*2+key_size, key.size()- sizeof(__uint8_t)*2-key_size );

}
void kv_encode::DecodeKey(const string& value, string& hash_key)
{
    __uint8_t key_length = 0;
    memcpy(&key_length ,value.c_str()+sizeof(__uint8_t), sizeof(__uint8_t) );

    hash_key.append(value.data()+sizeof(__uint8_t)+sizeof(__uint8_t) ,key_length );
}

string kv_encode::EncodeKey(const string& key,const int type)
{
    string buff="";

    char sztype[1];
    memcpy(sztype,&type, sizeof(__uint8_t) );
    buff.append(sztype,1 );

    char szsize[1];
    __uint8_t length = key.length();
    memcpy(szsize,&length, sizeof(__uint8_t) );
    buff.append(szsize,1 );
    
    buff.append( key.data(), key.size() );
    return buff;
}

string kv_encode::EncodeMetaKey(const string& key,const int type)
{
    __uint8_t data_type = type;

    string buff="";
    char sztype[1];
    
    memcpy(sztype,&data_type, sizeof(__uint8_t) );
    buff.append(sztype,1 );
    
    buff.append( key.data(), key.size() );
    return buff;

}

void kv_encode::DecodeMetaKey(const string& meta_key ,std::string& key )
{
    key.append(meta_key.c_str() + sizeof(__uint8_t) ,meta_key.length() - sizeof(__uint8_t) );
}
//=============list============
string kv_encode::EncodeListMetaValue( const __uint32_t ttl,const __uint64_t length,const __uint64_t list_head,
                                        const __uint64_t list_tail,const __uint64_t list_next_seq  )
{
    char sz[LIST_META_VALUE_LENGTH];
    memcpy( sz,  &ttl, sizeof(__uint32_t) );//ttl
    memcpy( sz+ sizeof(__uint32_t),  &length, sizeof(__uint64_t) );
    memcpy( sz+ sizeof(__uint32_t)+sizeof(__uint64_t),  &list_head, sizeof(__uint64_t) );
    memcpy( sz+ sizeof(__uint32_t)+sizeof(__uint64_t)+sizeof(__uint64_t),  &list_tail, sizeof(__uint64_t) );
    memcpy( sz+ sizeof(__uint32_t)+sizeof(__uint64_t)+sizeof(__uint64_t)+sizeof(__uint64_t),  &list_next_seq, sizeof(__uint64_t) );

    string buff="";
    buff.append(sz,LIST_META_VALUE_LENGTH );

    return buff;
}
void kv_encode::DecodeListMetaValue( const string& value, __uint32_t& ttl, __uint64_t& length , __uint64_t& list_head,
                                         __uint64_t& list_tail, __uint64_t& list_next_seq )
{
    ttl=0;
    length=0;
    list_head =0;
    list_tail=0;
    list_next_seq=0;
    memcpy( &ttl , value.c_str() , sizeof(__uint32_t) );
    memcpy( &length , value.c_str()+ sizeof(__uint32_t), sizeof(__uint64_t) );
    memcpy( &list_head , value.c_str()+ sizeof(__uint32_t)+sizeof(__uint64_t)*1, sizeof(__uint64_t) );
    memcpy( &list_tail , value.c_str()+ sizeof(__uint32_t)+sizeof(__uint64_t)*2 , sizeof(__uint64_t) );
    memcpy( &list_next_seq , value.c_str()+ sizeof(__uint32_t)+sizeof(__uint64_t)*3, sizeof(__uint64_t) );
}
string kv_encode::EncodeListFieldKey(const string& key,const __uint64_t& field_seq)
{
    string buff="";

    char sztype[1];
    __uint8_t data_type = LIST_TYPE;
    memcpy(sztype,&data_type, sizeof(__uint8_t) );
    buff.append(sztype,1 );

    char szsize[1];
    __uint8_t length = key.length();
    memcpy(szsize,&length, sizeof(__uint8_t) );
    buff.append(szsize,1 );
    
    buff.append( key.data(), key.size() );
    
    char seq[8];
    memcpy(seq,&field_seq, sizeof(__uint64_t) );
    buff.append( seq, 8 );
    return buff;
}
string kv_encode::EncodeListFieldValue(const __uint64_t& pre_seq,const __uint64_t& next_seq,const string& field_value )
{
    string buff="";

    char seq[8];
    memcpy(seq,&pre_seq, sizeof(__uint64_t) );
    buff.append(seq,8 );

   char next[8];
    memcpy(next,&next_seq, sizeof(__uint64_t) );
    buff.append(next,8);
    
    buff.append( field_value.data(), field_value.size() );
    return buff;
}
 
void kv_encode::DecodeListFieldValue(const string& value,__uint64_t& pre_seq,__uint64_t& next_seq,string& field_value)
{
    field_value="";
    memcpy( &pre_seq , value.c_str() , sizeof(__uint64_t) );
    memcpy( &next_seq , value.c_str()+ sizeof(__uint64_t), sizeof(__uint64_t) );

   field_value.assign(value.c_str()+ sizeof(__uint64_t)*2,  value.size() - sizeof(__uint64_t)*2 );
}
//=====================
string kv_encode::EncodeSetMetaValue( const __uint32_t ttl,const __uint64_t length )
{
    char sz[SET_META_VALUE_LENGTH];
    memcpy( sz,  &ttl, sizeof(__uint32_t) );
    memcpy( sz+ sizeof(__uint32_t),  &length, sizeof(__uint64_t) );

    string buff="";
    buff.append(sz,SET_META_VALUE_LENGTH );
    return buff;
}
void kv_encode::DecodeSetMetaValue( const string& value, __uint32_t& ttl, __uint64_t& length )
{
    memcpy( &ttl , value.c_str() , sizeof(__uint32_t) );
    memcpy( &length , value.c_str()+ sizeof(__uint32_t), sizeof(__uint64_t) );
}
string kv_encode::EncodeSetFieldKey(const string& key,const string& field_key)
{
    string buff="";

    char sztype[1];
    __uint8_t data_type = SET_TYPE;
    memcpy(sztype,&data_type, sizeof(__uint8_t) );
    buff.append(sztype,1 );

    char szsize[1];
    __uint8_t length = key.length();
    memcpy(szsize,&length, sizeof(__uint8_t) );
    buff.append(szsize,1 );
    
    buff.append( key.data(), key.size() );
    buff.append( field_key.data(), field_key.size() );
    return buff;
}
void kv_encode::DecodeSetFieldKey(const string& key, string & set_key , string& field_key)
{
   __uint8_t key_size = 0;
    memcpy( &key_size , key.c_str()+sizeof(__uint8_t) , sizeof(__uint8_t) );
   
   set_key.assign(key.c_str()+ sizeof(__uint8_t)*2, key_size );

   field_key.assign(key.c_str()+ sizeof(__uint8_t)*2+key_size, key.size()- sizeof(__uint8_t)*2-key_size );
}
//=====================
string kv_encode::EncodeZSetMetaValue( const __uint32_t ttl,const __uint64_t length )
{
    char sz[SET_META_VALUE_LENGTH];
    memcpy( sz,  &ttl, sizeof(__uint32_t) );
    memcpy( sz+ sizeof(__uint32_t),  &length, sizeof(__uint64_t) );

    string buff="";
    buff.append(sz,ZSET_META_VALUE_LENGTH );
    return buff;
}
void kv_encode::DecodeZSetMetaValue( const string& value, __uint32_t& ttl, __uint64_t& length )
{
    memcpy( &ttl , value.c_str() , sizeof(__uint32_t) );
    memcpy( &length , value.c_str()+ sizeof(__uint32_t), sizeof(__uint64_t) );
}
string kv_encode::EncodeZSetFieldKey(const string& key,const string& field_key)
{
    string buff="";

    char sztype[1];
    __uint8_t data_type = ZSET_TYPE;
    memcpy(sztype,&data_type, sizeof(__uint8_t) );
    buff.append(sztype,1 );

    char szsize[1];
    __uint8_t length = key.length();
    memcpy(szsize,&length, sizeof(__uint8_t) );
    buff.append(szsize,1 );
    
    buff.append( key.data(), key.size() );
    buff.append( field_key.data(), field_key.size() );
    return buff;
}
void kv_encode::DecodeZSetFieldKey(const string& key, string& field_key)
{
    __uint8_t key_size = 0;
    memcpy( &key_size , key.c_str()+sizeof(__uint8_t) , sizeof(__uint8_t) );
   
   field_key.assign(key.c_str()+ sizeof(__uint8_t)*2+key_size, key.size()- sizeof(__uint8_t)*2-key_size );
}
string kv_encode::EncodeZSetFieldValue(const __int64_t score)
{
    string buff="";

    char sztype[8];
    memcpy(sztype,&score, sizeof(__int64_t) );
    buff.append(sztype,8 );

    return buff;
}
void kv_encode::DecodeZSetFieldValue(const string& key, __int64_t & score)
{
    memcpy( &score , key.c_str()  , sizeof(__int64_t) );
}

string kv_encode::EncodeZSetScoreKey(const string& key,const __int64_t& score)
{
    string buff="";

    char sztype[1];
    __uint8_t data_type = ZSET_SCORE;
    memcpy(sztype,&data_type, sizeof(__uint8_t) );
    buff.append(sztype,1 );

    char szlength[1];
    __uint8_t length = key.length();
    memcpy(szlength,&length, sizeof(__uint8_t) );
    buff.append(szlength,1 );
    
    buff.append( key.data(), length );

    char szscore[sizeof(__int64_t)];
    memcpy( szscore, &score, sizeof(__int64_t) );
    buff.append(szscore,sizeof(__int64_t) );
    return buff;
}
void kv_encode::DecodeZSetScoreKey(const string& key,string& field_key, __int64_t& score)
{
    field_key="";
    __int8_t key_size=0;
    memcpy( &key_size , key.c_str() + sizeof(uint8_t), sizeof(uint8_t) );
    field_key.assign( key.c_str() + sizeof(uint8_t)*2 + key_size ,key.size()- sizeof(uint8_t)*2 - key_size );

    memcpy( &score, key.c_str()+(key.size()-sizeof(__int64_t) ), sizeof(__int64_t) );
}
