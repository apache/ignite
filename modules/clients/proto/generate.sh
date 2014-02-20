#!/bin/sh

protoc --version
protoc --java_out=$GRIDGAIN_HOME/modules/src/java ClientMessages.proto
protoc --cpp_out=$GRIDGAIN_HOME/modules/clients/cpp/main/src/impl/marshaller/protobuf ClientMessages.proto
mv $GRIDGAIN_HOME/modules/clients/cpp/main/src/impl/marshaller/protobuf/ClientMessages.pb.h $GRIDGAIN_HOME/modules/clients/cpp/main/include/gridgain/impl/marshaller/protobuf/ClientMessages.pb.h
mv $GRIDGAIN_HOME/modules/clients/cpp/main/src/impl/marshaller/protobuf/ClientMessages.pb.cc $GRIDGAIN_HOME/modules/clients/cpp/main/src/impl/marshaller/protobuf/ClientMessages.pb.cpp
patch ../cpp/main/src/impl/marshaller/protobuf/ClientMessages.pb.cpp < includepatch.patch