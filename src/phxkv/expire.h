#pragma once
#include <typeinfo>
#include "utils_include.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/db.h"
#include "rocksdb/comparator.h" 


namespace phxpaxos
{
 class expire : public Thread
{
private:
    /* data */
public:
    expire(rocksdb::TransactionDB * m_rocks,rocksdb::ColumnFamilyHandle* cf );
    ~expire();
    void Stop();
    void run();
    void delete_string(const std::string& string_key , const std::string& string_value);
    void delete_hash(const std::string& hash_meta_key,const std::string& hash_meta_value );
    void delete_set(const std::string& set_meta_key , const std::string& set_meta_value);
    void delete_list(const std::string& list_meta_key , const std::string& list_meta_value);
    void delete_zset(const std::string& zset_meta_key , const std::string& zset_meta_value);
private:
    rocksdb::TransactionDB * m_rocksdb;
    rocksdb::ColumnFamilyHandle* cf_;

};
}
