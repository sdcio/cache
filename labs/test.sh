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

#/bin/bash

set -e

bin/cachectl list
bin/cachectl create -n target1
bin/cachectl create -n target2
bin/cachectl list
bin/cachectl create-candidate -n target1 --candidate cl1
bin/cachectl create-candidate -n target1 --candidate cl2 --owner me --priority 100
bin/cachectl create-candidate -n target2 --candidate cl1
bin/cachectl list
bin/cachectl get -n target1
bin/cachectl get -n target2
# read from cache
bin/cachectl modify -n target1 --update a,b,c1:::string:::1 --update a,b,c2:::string:::2
bin/cachectl modify -n target1 --update a,b,c3:::string:::3
bin/cachectl modify -n target1 --update a,b,c4:::string:::4
bin/cachectl read -n target1 -p a,b
bin/cachectl read -n target1 -p a,b,c1
bin/cachectl read -n target1 -p a,b,c2
bin/cachectl read -n target1 -p a,b,c3
bin/cachectl read -n target1 -p a,b,c4
bin/cachectl read -n target1 -p a,b,c1 -p a,b,c2 -p a,b,c3 -p a,b,c4
# write, delete and read from candidate
bin/cachectl modify -n target1/cl1 --update a,b,c2:::string:::42
bin/cachectl read -n target1/cl1 -p a,b,c1
bin/cachectl read -n target1/cl1 -p a,b,c2
bin/cachectl read -n target1/cl1 -p a,b
bin/cachectl modify -n target1/cl1 --delete a,b,c1
bin/cachectl read -n target1/cl1 -p a,b
bin/cachectl read -n target1 -p a,b # leaf c1 is still in main
# delete value
bin/cachectl modify -n target1 --delete a,b,c1
bin/cachectl read -n target1 -p a,b
bin/cachectl read -n target1/cl1 -p a,b # a,b,c1 is also deleted from candidate
# changes and discard
bin/cachectl get-changes -n target1 --candidate cl1
bin/cachectl discard -n target1 --candidate cl1
bin/cachectl get-changes -n target1 --candidate cl1
# delete caches
bin/cachectl delete -n target1
bin/cachectl delete -n target2
bin/cachectl list
