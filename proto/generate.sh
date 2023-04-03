#!/bin/bash

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

CACHE_OUT_DIR=$SCRIPTPATH/cachepb
mkdir -p $CACHE_OUT_DIR
protoc --go_out=$CACHE_OUT_DIR --go-grpc_out=$CACHE_OUT_DIR -I $SCRIPTPATH $SCRIPTPATH/cache.proto