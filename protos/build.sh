#!/bin/sh

# Golang gRPC install guide: https://grpc.io/docs/quickstart/go.html

cd $( dirname "${BASH_SOURCE[0]}")

protoc -I . --experimental_allow_proto3_optional --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` paxos_msg.proto
protoc -I . --experimental_allow_proto3_optional --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` phxkv.proto
protoc *.proto --experimental_allow_proto3_optional --cpp_out=.

