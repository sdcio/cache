# cache

## build

```shell
make build
```

## server

```shell
./bin/cached
```

## client

```shell
bin/cachectl list
bin/cachectl create -n target1 --cached
bin/cachectl create -n target2 --cached
bin/cachectl list
bin/cachectl create-candidate -n target1 --candidate cl1
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
```
