cmake_minimum_required(VERSION 2.8)
project(kvtools)
SET(PROJECT_SOURCE_DIR "/home/dev/works/codes_proto//kvstore")
message(STATUS "root This is BINARY dir " ${PROJECT_BINARY_DIR})
message(STATUS "root This is SOURCE dir " ${PROJECT_SOURCE_DIR})

#设置debug
#set (CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -g")

#SET(CMAKE_BUILD_TYPE "Debug")
SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g2 -ggdb")
SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")

#添加多线程依赖
set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -march=native -O3 -pthread" )
#添加头文件路径
include_directories( ${PROJECT_SOURCE_DIR}/include)
include_directories( ${PROJECT_SOURCE_DIR}/include/phxpaxos )
include_directories( ${PROJECT_SOURCE_DIR}/plugin/include)
include_directories( ${PROJECT_SOURCE_DIR}/plugin/include/phxpaxos_plugin )
include_directories( ${PROJECT_SOURCE_DIR}/plugin/include/monitor )
include_directories( ${PROJECT_SOURCE_DIR}/src/comm)
include_directories( ${PROJECT_SOURCE_DIR}/src/algorithm)
include_directories( ${PROJECT_SOURCE_DIR}/src/checkpoint)
include_directories( ${PROJECT_SOURCE_DIR}/src/communicate)
include_directories( ${PROJECT_SOURCE_DIR}/src/communicate/tcp)
include_directories( ${PROJECT_SOURCE_DIR}/src/config)
include_directories( ${PROJECT_SOURCE_DIR}/src/logstorage)
include_directories( ${PROJECT_SOURCE_DIR}/src/master)
include_directories( ${PROJECT_SOURCE_DIR}/src/node)
include_directories( ${PROJECT_SOURCE_DIR}/src/phxkv)
include_directories( ${PROJECT_SOURCE_DIR}/src/sm-base)
include_directories( ${PROJECT_SOURCE_DIR}/src/utils)
include_directories( ${PROJECT_SOURCE_DIR}/protos)
include_directories( ${PROJECT_SOURCE_DIR}/src/test)

#依赖的第三方库头文件
include_directories( ${PROJECT_SOURCE_DIR}/third_party/google/protobuf )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/gflags )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/glog )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/grpc )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/rocksdb/include )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/prometheus )
include_directories( ${PROJECT_SOURCE_DIR}/third_party )



add_executable(kvtools     ${PROJECT_SOURCE_DIR}/protos/paxos_msg.pb.cc
                            ${PROJECT_SOURCE_DIR}/protos/paxos_msg.grpc.pb.cc
                            ${PROJECT_SOURCE_DIR}/protos/phxkv.pb.cc
                            ${PROJECT_SOURCE_DIR}/protos/phxkv.grpc.pb.cc
                            ${PROJECT_SOURCE_DIR}/protos/master_sm.pb.cc
                   
                            ${PROJECT_SOURCE_DIR}/src/utils/util.cpp 
                            ${PROJECT_SOURCE_DIR}/src/utils/kv_encode.cpp
                            ${PROJECT_SOURCE_DIR}/src/test/KVClient.cpp 
                            ${PROJECT_SOURCE_DIR}/src/test/kv_grpc_client_main.cpp)
                            
target_link_libraries( kvtools -lrocksdb -lprotobuf  -lgrpc++ -lgrpc -lgflags -lgrpc++_reflection -lprometheus-cpp-core -lprometheus-cpp-pull -ldl)
# -glog
