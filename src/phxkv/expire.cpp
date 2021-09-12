#include "expire.h"
#include "sysconfig.h"
#include "log.h"
#include "kv_encode.h"
#include "util.h"
namespace phxpaxos
{

expire::expire(rocksdb::TransactionDB * m_rocks,rocksdb::ColumnFamilyHandle* cf)
{
    m_rocksdb = m_rocks;
    cf_ = cf;
}

expire::~expire()
{
    
}

void expire::Stop()
{

}

void expire::run()
{
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    
    INFO_LOG("expire.Cleaner start");

    int conf_hour = CConfig::Get()->backup.backup_hour+1;
    int conf_minut =CConfig::Get()->backup.backup_minute;

    while( true ){
        //定时清理
       //时间不到执行等待
        time_t now = time(0);
        tm *gmtm = gmtime(&now);
        if( conf_hour !=gmtm->tm_hour+8  ||   gmtm->tm_min != conf_minut  ){
            Time::MsSleep(1000*1);
            continue;
        }
        it = m_rocksdb->NewIterator(iterate_options, cf_ );//清理过期
     
        while (it->Valid()) {
            std::string key = it->key().ToString();
            std::string value = it->value().ToString();
            const char type = key.at(0 );
            if( type== STRING_TYPE){
                
                delete_string( key , value);
               
            }else if( type == HASH_META  ){
                
                delete_hash( key , value );
            }else if( type==LIST_META ){

                delete_list( key , value );
            }else if( type==SET_META ){

                delete_set( key , value );
            }else if( type == ZSET_META ){
                delete_zset( key , value );
            }
            it->Next();
        }
    }

}

void expire::delete_string(const std::string& string_key,const std::string & string_value)
{
    uint32_t ttl=0;
    uint64_t length = 0;
    std::string field_value ="";
    int32_t cur_ttl = Time:: GetTimestampSec();
    
    kv_encode::DecodeStringValue( string_value, ttl,field_value );
    if( ttl != 0 && ttl <= cur_ttl ){

        m_rocksdb->Delete(rocksdb::WriteOptions(), cf_,string_key );
         INFO_LOG("delete key=%s", string_key.c_str() );
    }
}

void expire::delete_hash(const std::string& hash_meta_key,const std::string& hash_meta_value)
{
    uint32_t ttl=0;
    uint64_t length = 0;
    std::string hash_key ="";
    int32_t cur_ttl = Time:: GetTimestampSec();

    kv_encode::DecodeHashMetaValue( hash_meta_key, ttl,length );
    if( ttl != 0 && ttl <= cur_ttl ){
        rocksdb::WriteBatch batch;
        kv_encode::DecodeMetaKey( hash_meta_key , hash_key );
        std::string key_start =kv_encode::EncodeKey( hash_key , HASH_TYPE );
                        
        //进行前缀查询，批量删除
        rocksdb::Iterator *it;
        rocksdb::ReadOptions iterate_options;
        iterate_options.fill_cache = false;
    
        it = m_rocksdb->NewIterator(iterate_options, cf_ );
        it->Seek(key_start);
        while (it->Valid()) {
            string key = it->key().ToString();
                             
            if ( (it->key())[0] != HASH_TYPE) {  //类型相等
                break;
            }
            std::string dbkey="";
            kv_encode::DecodeKey(key, dbkey);
            if (dbkey == hash_key) {//加个验证
                batch.Delete( cf_,key );  
                INFO_LOG("delete_hash key=%s" , hash_key.c_str() ); 
            } else {
                break;
            }
            it->Next();
        }
        delete it;
        batch.Delete( cf_,hash_meta_key );  
        auto ret = m_rocksdb->Write(rocksdb::WriteOptions(),  &batch);
        if (!ret.ok())
        {
            ERROR_LOG("delete_hash batch put error ret %s ",ret.ToString().c_str() );    
        }
    }
}
void expire::delete_set(const std::string& set_meta_key , const std::string& set_meta_value)
{
    uint32_t ttl=0;
    uint64_t length = 0;
    std::string set_key ="";
    int32_t cur_ttl = Time:: GetTimestampSec();
    
    kv_encode::DecodeSetMetaValue( set_meta_key, ttl,length );
    if( ttl != 0 && ttl <= cur_ttl ){
        rocksdb::WriteBatch batch;
         kv_encode::DecodeMetaKey( set_meta_key , set_key );
        std::string key_start =kv_encode::EncodeKey( set_key , SET_TYPE );
                        
        //进行前缀查询，批量删除
        rocksdb::Iterator *it;
        rocksdb::ReadOptions iterate_options;
        iterate_options.fill_cache = false;
    
        it = m_rocksdb->NewIterator(iterate_options, cf_ );
        it->Seek(key_start);
        while (it->Valid()) {
            string key = it->key().ToString();
                             
            if ( (it->key())[0] != SET_TYPE) {  //类型相等
                break;
            }
            std::string dbkey="";
            kv_encode::DecodeKey(key, dbkey);
            if (dbkey == set_key) {//加个验证
                batch.Delete( cf_,key );   
                INFO_LOG("delete_hash key=%s" , set_key.c_str()); 
            } else {
                break;
            }
            it->Next();
        }
        delete it;
        batch.Delete( cf_, set_meta_key );  

        auto ret = m_rocksdb->Write(rocksdb::WriteOptions(),  &batch);
        if (!ret.ok())
        {
            ERROR_LOG("delete_set batch put error ret %s ",ret.ToString().c_str() );    
        }
    }
}
void expire::delete_zset(const std::string& zset_meta_key , const std::string& zset_meta_value )
{
    uint32_t ttl=0;
    uint64_t length = 0;
    std::string zset_key ="";
    int32_t cur_ttl = Time:: GetTimestampSec();
    
    kv_encode::DecodeZSetMetaValue( zset_meta_value, ttl,length );
    if( ttl != 0 && ttl <= cur_ttl ){
        rocksdb::WriteBatch batch;
         kv_encode::DecodeMetaKey( zset_meta_key , zset_key );
        std::string key_start =kv_encode::EncodeKey( zset_key , ZSET_TYPE );
                        
        //进行前缀查询，批量删除
        rocksdb::Iterator *it;
        rocksdb::ReadOptions iterate_options;
        iterate_options.fill_cache = false;
    
        it = m_rocksdb->NewIterator(iterate_options, cf_ );
        it->Seek(key_start);
        while (it->Valid()) {
                             
            if ( (it->key())[0] != ZSET_TYPE) {  //类型相等
                break;
            }
            string key = it->key().ToString();
            std::string dbkey="";
            kv_encode::DecodeKey(key, dbkey);
            if (dbkey == zset_key) {//加个验证
                batch.Delete( cf_,key );   
                INFO_LOG("decode key=%s", zset_key.c_str() );
                //清理掉score field,拿到score
                std::string db_score = it->value().ToString();
                int64_t score =0;
                kv_encode::DecodeZSetFieldValue( db_score ,score  );
                INFO_LOG("decode score=%l", score );
                std::string score_key = kv_encode::EncodeZSetScoreKey(zset_key , score );
                batch.Delete( cf_,score_key );
            } else {
                break;
            }
            it->Next();
        }
        delete it;
        batch.Delete( cf_, zset_meta_key );  
        auto ret = m_rocksdb->Write(rocksdb::WriteOptions(),  &batch);
        if (!ret.ok())
        {
            ERROR_LOG("delete_zset batch put error ret %s ",ret.ToString().c_str() );    
        }
    }
}
void expire::delete_list(const std::string& list_meta_key , const std::string& list_meta_value)
{
    uint32_t ttl=0;
    uint64_t length = 0;
    std::string list_key ="";
    uint64_t list_head=0;
    uint64_t list_tail = 0;
    uint64_t list_next_seq=0;
    int32_t cur_ttl = Time:: GetTimestampSec();
    
    kv_encode::DecodeListMetaValue( list_meta_value, ttl,length ,list_head,list_tail , list_next_seq );
    if( ttl != 0 && ttl <= cur_ttl ){
        rocksdb::WriteBatch batch;
        kv_encode::DecodeMetaKey( list_meta_key , list_key );
        INFO_LOG("delete_list list_key=%s", list_key.c_str() );
        uint64_t cur_seq = list_head ; 
        uint64_t pre_seq=0;uint64_t next_seq=0;
        while ( cur_seq != 0 ) {
                             
            std::string key_field = kv_encode::EncodeListFieldKey( list_key , cur_seq );
            std::string value_field =""; std::string value_ =""; 
            m_rocksdb->Get(rocksdb::ReadOptions() , cf_, key_field ,& value_field );
            kv_encode::DecodeListFieldValue(value_field , pre_seq, next_seq,value_ );
            batch.Delete( cf_,key_field );   
            cur_seq = next_seq;
            INFO_LOG("delete_list key=%s", value_.c_str() );
        }
      
        batch.Delete( cf_, list_meta_key );  
        auto ret = m_rocksdb->Write(rocksdb::WriteOptions(),  &batch);
        if (!ret.ok())
        {
            ERROR_LOG("delete_list batch put error ret %s ",ret.ToString().c_str() );    
        }
    }
}

}