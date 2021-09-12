cmake_minimum_required(VERSION 2.8)
project(kvserver)
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

#依赖的第三方库头文件
include_directories( ${PROJECT_SOURCE_DIR}/third_party/google/protobuf )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/gflags )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/glog )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/grpc )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/rocksdb/include )
include_directories( ${PROJECT_SOURCE_DIR}/third_party/prometheus )
include_directories( ${PROJECT_SOURCE_DIR}/third_party )



add_executable(kvserver     protos/paxos_msg.pb.cc
                            protos/paxos_msg.grpc.pb.cc
                            protos/phxkv.pb.cc
                            protos/phxkv.grpc.pb.cc
                            protos/master_sm.pb.cc

                            plugin/monitor/monitor_bp.cpp
                            plugin/monitor/monitor.cpp

                            src/algorithm/base.cpp 
                            src/algorithm/proposer.cpp 
                            src/algorithm/acceptor.cpp 
                            src/algorithm/learner.cpp 
                            src/algorithm/learner_sender.cpp 
                            src/algorithm/instance.cpp 
                            src/algorithm/ioloop.cpp 
                            src/algorithm/commitctx.cpp 
                            src/algorithm/committer.cpp 
                            src/algorithm/checkpoint_sender.cpp 
                            src/algorithm/checkpoint_receiver.cpp 
                            src/algorithm/msg_counter.cpp
                            src/algorithm/rsync.cpp
                            src/checkpoint/cp_mgr.cpp 
                            src/checkpoint/replayer.cpp 
                            src/checkpoint/cleaner.cpp 



                            src/comm/breakpoint.cpp
                            src/comm/options.cpp 
                            src/comm/inside_options.cpp


                            src/communicate/dfnetwork.cpp 
                            src/communicate/network.cpp 
                            src/communicate/communicate.cpp 
                            src/communicate/udp.cpp



                            src/communicate/tcp/event_base.cpp 
                            src/communicate/tcp/event_loop.cpp 
                            src/communicate/tcp/message_event.cpp 
                            src/communicate/tcp/notify.cpp
                            src/communicate/tcp/tcp_acceptor.cpp
                            src/communicate/tcp/tcp_client.cpp
                            src/communicate/tcp/tcp.cpp


                            src/config/config.cpp 
                            src/config/system_v_sm.cpp


                            src/logstorage/db.cpp 
                            src/logstorage/paxos_log.cpp 
                            src/logstorage/log_store.cpp 
                            src/logstorage/system_variables_store.cpp


                            
                            src/master/master_sm.cpp 
                            src/master/master_mgr.cpp 
                            src/master/master_variables_store.cpp

                            

                            src/node/group.cpp 
                            src/node/pnode.cpp 
                            src/node/node.cpp 
                            src/node/propose_batch.cpp 
                            
                            src/sm-base/sm_base.cpp 
                            src/sm-base/sm.cpp
                            
                             
                             
                            src/utils/concurrent.cpp 
                            src/utils/socket.cpp 
                            src/utils/util.cpp 
                            src/utils/crc32.cpp 
                            src/utils/timer.cpp 
                            src/utils/bytes_buffer.cpp 
                            src/utils/serial_lock.cpp 
                            src/utils/wait_lock.cpp 
                            src/utils/notifier_pool.cpp
                            src/utils/logger.cpp
                            src/utils/kv_encode.cpp
                            
                            
                            
                            
                            src/phxkv/kv_grpc_server.cpp
                            src/phxkv/kv.cpp
                            src/phxkv/status_mng.cpp
                            src/phxkv/log.cpp
                            src/phxkv/kv_paxos.cpp
                            src/phxkv/kvsm.cpp
                            src/phxkv/metrics.cpp
                            src/phxkv/sysconfig.cpp
                            src/phxkv/kv_hash.cpp
                            src/phxkv/kv_list.cpp
                            src/phxkv/kv_set.cpp
                            src/phxkv/kv_zset.cpp
                            src/phxkv/expire.cpp
                            src/phxkv/kv_grpc_server_main.cpp)
                             


 
target_link_libraries( kvserver -lrocksdb -lprotobuf  -lgrpc++ -lgrpc -lgpr -lgflags -lgrpc++_reflection -lprometheus-cpp-core -lprometheus-cpp-pull -ldl)
# -glog
