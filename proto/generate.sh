#!/bin/bash
# Copyright 2024 Nokia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

clang-format -i -style=file:clang-format.style $SCRIPTPATH/cache.proto

CACHE_OUT_DIR=$SCRIPTPATH/cachepb
mkdir -p $CACHE_OUT_DIR
protoc --go_out=$CACHE_OUT_DIR --go-grpc_out=$CACHE_OUT_DIR -I $SCRIPTPATH $SCRIPTPATH/cache.proto