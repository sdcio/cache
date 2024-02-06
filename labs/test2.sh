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


set -e

existing_targets=$(bin/cachectl list)
echo "- existing targets:" $existing_targets

for t in $existing_targets
do
echo "- deleting $t"
bin/cachectl delete -n $t
done
echo ""
echo -e "- creating target1"
bin/cachectl create -n target1
echo ""
echo -e "- creating target2"
bin/cachectl create -n target2
echo ""
echo -e "- listing targets"
bin/cachectl list

echo ""
echo -e "- writing to config store"
# config
bin/cachectl modify -n target1 --update a,b,c1:::string:::config1 --update a,b,c2:::string:::config2
bin/cachectl modify -n target1 --update a,b,c3:::string:::config3
bin/cachectl modify -n target1 --update a,b,c4:::string:::config4
echo ""
echo -e "- reading from config store"
bin/cachectl read -n target1 -s config -p a,b --format flat
bin/cachectl read -n target1 -s config -p a,b,c1 --format flat
bin/cachectl read -n target1 -s config -p a,b,c2 --format flat
bin/cachectl read -n target1 -s config -p a,b,c3 --format flat
bin/cachectl read -n target1 -s config -p a,b,c4 --format flat
# state
echo ""
echo -e "- writing to state store"
bin/cachectl modify -n target1 -s state --update a,b,c1:::string:::state1 --update a,b,c2:::string:::state2
bin/cachectl modify -n target1 -s state --update a,b,c3:::string:::state3
bin/cachectl modify -n target1 -s state --update a,b,c4:::string:::state4
echo ""
echo -e "- reading from config store"
bin/cachectl read -n target1 -s state -p a,b --format flat
# intended

echo ""
echo -e "- writing to intended store"
bin/cachectl modify -n target1 -s intended --update a,b,c1:::string:::100intended1 --owner me --priority 100
bin/cachectl modify -n target1 -s intended --update a,b,c2:::string:::100intended2 --owner me --priority 100
bin/cachectl modify -n target1 -s intended --update a,b,c3:::string:::100intended3 --owner me --priority 100
bin/cachectl modify -n target1 -s intended --update a,b,c4:::string:::100intended4 --owner me --priority 100
bin/cachectl modify -n target1 -s intended --update a,b,c1:::string:::99intended1 --owner other --priority 99
bin/cachectl modify -n target1 -s intended --update a,b,c2:::string:::99intended2 --owner other --priority 99
bin/cachectl modify -n target1 -s intended --update a,b,c3:::string:::99intended3 --owner other --priority 99

echo ""
echo -e "- reading from intended store"
echo -e "  - reading all"
bin/cachectl read -n target1 -s intended -p a,b --priority -1 --format flat # get all 

echo ""
echo -e "  - reading priority 100, owner 'me'"
bin/cachectl read -n target1 -s intended -p a,b --owner me --priority 100 --format flat # get specific owner and priority

echo ""
echo -e "  - reading highest priority"
bin/cachectl read -n target1 -s intended -p a,b --priority 0 --format flat # get highest priority

echo ""
echo -e "  - reading highest priority individual paths"
bin/cachectl read -n target1 -s intended -p a,b,c1 --format flat # --priority 0
bin/cachectl read -n target1 -s intended -p a,b,c2 --format flat # --priority 0
bin/cachectl read -n target1 -s intended -p a,b,c3 --format flat # --priority 0
bin/cachectl read -n target1 -s intended -p a,b,c4 --format flat # --priority 0


# deleting
echo ""
echo "- deleting"
## config
echo "  - deleting from config store"
bin/cachectl modify -n target1 --delete a,b,c1
bin/cachectl modify -n target1 --delete a,b,c2
bin/cachectl modify -n target1 --delete a,b,c3
bin/cachectl modify -n target1 --delete a,b,c4

## state
echo "  - deleting from state store"
bin/cachectl modify -n target1 -s state --delete a,b,c1
bin/cachectl modify -n target1 -s state --delete a,b,c2
bin/cachectl modify -n target1 -s state --delete a,b,c3
bin/cachectl modify -n target1 -s state --delete a,b,c4

## intended
echo "  - deleting from intended store"
bin/cachectl modify -n target1 -s intended --delete a,b,c1 --priority 99 --owner other
bin/cachectl modify -n target1 -s intended --delete a,b,c1 --priority 100 --owner me
bin/cachectl modify -n target1 -s intended --delete a,b,c2 --priority 100 --owner me
bin/cachectl modify -n target1 -s intended --delete a,b,c3 --priority 100 --owner me
bin/cachectl modify -n target1 -s intended --delete a,b,c4 --priority 100 --owner me

bin/cachectl modify -n target1 -s intended --delete a,b,c2 --priority 99 --owner other
bin/cachectl modify -n target1 -s intended --delete a,b,c3 --priority 99 --owner other
