#!/bin/bash
bin/cachectl list
bin/cachectl create -n target1
bin/cachectl create -n target2
bin/cachectl list
# config
bin/cachectl modify -n target1 --update a,b,c1:::string:::config1 --update a,b,c2:::string:::config2
bin/cachectl modify -n target1 --update a,b,c3:::string:::config3
bin/cachectl modify -n target1 --update a,b,c4:::string:::config4
bin/cachectl read -n target1 -s config -p a,b --format flat
# state
bin/cachectl modify -n target1 -s state --update a,b,c1:::string:::state1 --update a,b,c2:::string:::state2
bin/cachectl modify -n target1 -s state --update a,b,c3:::string:::state3
bin/cachectl modify -n target1 -s state --update a,b,c4:::string:::state4
bin/cachectl read -n target1 -s state -p a,b --format flat
# intended
bin/cachectl modify -n target1 -s intended --update a,b,c1:::string:::100intended1 --owner me --priority 100
bin/cachectl modify -n target1 -s intended --update a,b,c2:::string:::100intended2 --owner me --priority 100
bin/cachectl modify -n target1 -s intended --update a,b,c3:::string:::100intended3 --owner me --priority 100
bin/cachectl modify -n target1 -s intended --update a,b,c4:::string:::100intended4 --owner me --priority 100

bin/cachectl modify -n target1 -s intended --update a,b,c1:::string:::99intended1 --owner other --priority 99
bin/cachectl modify -n target1 -s intended --update a,b,c2:::string:::99intended2 --owner other --priority 99
bin/cachectl modify -n target1 -s intended --update a,b,c3:::string:::99intended3 --owner other --priority 99

bin/cachectl read -n target1 -s intended -p a,b --owner me --priority 100 --format flat # get specific owner and priority
bin/cachectl read -n target1 -s intended -p a,b --priority -1 --format flat # get all 
bin/cachectl read -n target1 -s intended -p a,b --priority 0 --format flat # get highest priority

bin/cachectl read -n target1 -s intended -p a,b,c1 # --priority 0
bin/cachectl read -n target1 -s intended -p a,b,c2 # --priority 0
bin/cachectl read -n target1 -s intended -p a,b,c3 # --priority 0
bin/cachectl read -n target1 -s intended -p a,b,c4 # --priority 0