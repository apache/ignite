#!/bin/sh

##
# Generate GridClientMessages.cs from the protobuf messages definition.
#
# Note! Protobuf-csharp-port v. 2.4.1 or higher expected to be available on the PATH variable.
##

OUT=`dirname "$0"`
cd $OUT
OUT=`pwd`
echo "Output directory: "$OUT

cd $OUT"/../../../../../../clients/proto"
SRC=`pwd`
echo "Source directory: "$SRC

cd $OUT

protogen ${SRC}/ClientMessages.proto --proto_path=${SRC} -output_directory=${OUT} -umbrella_classname=GridClientMessages -namespace=GridGain.Client.Impl.Protobuf -public_classes=false
