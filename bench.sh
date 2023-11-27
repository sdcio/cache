#!/bin/bash

write_read () {
    # write
    bin/cachectl modify -n $1 --update a,b,c1:::string:::1 --update a,b,c2:::string:::2
    bin/cachectl modify -n $1 --update a,b,c3:::string:::3
    bin/cachectl modify -n $1 --update a,b,c4:::string:::4
    # read
    bin/cachectl read --format flat -n $1 -p a,b
    bin/cachectl read --format flat -n $1 -p a,b,c1
    bin/cachectl read --format flat -n $1 -p a,b,c2
    bin/cachectl read --format flat -n $1 -p a,b,c3
    bin/cachectl read --format flat -n $1 -p a,b,c4
    bin/cachectl read --format flat -n $1 -p a,b,c1 -p a,b,c2 -p a,b,c3 -p a,b,c4
}

cache1=target1
cache2=target2
cache3=target3

bin/cachectl list
bin/cachectl list | xargs -rn1 bin/cachectl delete -n
bin/cachectl create -n $cache1
bin/cachectl create -n $cache2
bin/cachectl create -n $cache3
bin/cachectl list

bin/cachectl get -n $cache1
bin/cachectl get -n $cache2
bin/cachectl get -n $cache3

bin/cachectl create-candidate -n $cache1 --candidate cand1
bin/cachectl create-candidate -n $cache2 --candidate cand1
bin/cachectl create-candidate -n $cache3 --candidate cand1

bin/cachectl list
bin/cachectl get -n $cache1
bin/cachectl get -n $cache2
bin/cachectl get -n $cache3

write_read $cache1
write_read $cache2
write_read $cache3

####
numpaths=10000
bin/cachectl list | xargs -rn1 bin/cachectl delete -n
bin/cachectl bench --create --num-path $numpaths

