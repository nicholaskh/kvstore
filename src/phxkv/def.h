/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#pragma once

namespace phxkv
{

const uint64_t NullVersion = std::numeric_limits<uint64_t>::min();



enum KVOperatorType
{
    KVOperatorType_READ = 1,
    KVOperatorType_WRITE = 2,
    KVOperatorType_DELETE = 3,
};
enum PhxKVStatus
{
    FAIL = -1,
    SUCC = 0,
    KEY_NOTEXIST = 1,
    SERVER_BUSY=2,
    PARAM_ERROR=3,
    META_NOTEXIST=4,
    ROCKSDB_ERR=5,
    KEY_EXIST=6,
    MASTER_REDIRECT = 10,
    NO_MASTER = 101,
};


}
