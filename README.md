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
bin/cachectl create -n target1
bin/cachectl create -n target2
bin/cachectl list
bin/cachectl create-candidate -n target1 --candidate cl1 --owner me --priority 100
bin/cachectl create-candidate -n target1 --candidate cl2 --owner other --priority 200
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
# bin/cachectl get-changes -n target1 --candidate cl1
# delete caches
bin/cachectl delete -n target1
bin/cachectl delete -n target2
bin/cachectl list
```

## stores

```shell
bin/cachectl list
bin/cachectl create -n target1
bin/cachectl create -n target2
bin/cachectl list
# config
bin/cachectl modify -n target1 --update a,b,c1:::string:::1 --update a,b,c2:::string:::2
bin/cachectl modify -n target1 --update a,b,c3:::string:::3
bin/cachectl modify -n target1 --update a,b,c4:::string:::4
# state
bin/cachectl modify -n target1 -s state --update a,b,c1:::string:::1 --update a,b,c2:::string:::2
bin/cachectl modify -n target1 -s state --update a,b,c3:::string:::3
bin/cachectl modify -n target1 -s state --update a,b,c4:::string:::4
# intended
bin/cachectl modify -n target1 -s intended --update a,b,c1:::string:::1 --update a,b,c2:::string:::2 --owner me --priority 100
bin/cachectl modify -n target1 -s intended --update a,b,c3:::string:::3
bin/cachectl modify -n target1 -s intended --update a,b,c4:::string:::4

bin/cachectl read -n target1 -s intended -p a,b --owner me --priority 100
```

## benchmark against the lab in labs/single

### resources

```yaml
cpu-set: 0-7
memory: 16Gib
```

config:

```yaml
grpc-server:
  ## other fields
  buffer-size: -1 # defaults to 100k
  write-workers: 16
```

### test results

```shell
bin/cachectl bench -a clab-cache-cache1:50100 \
    --create \
    --periodic \
    --concurrency 16 \
    --num-cache 16 \
    --num-path 2500000

caches          : 16
concurrency     : 16
paths per cache : 2500000
INFO[0000] cache creation:                              
INFO[0000]      min: 15.729681ms                            
INFO[0000]      max: 190.146609ms                           
INFO[0000]      avg: 99.45483ms                             
INFO[0010] writing...                                   
INFO[0141] values write:                                
INFO[0141]      min: 2m10.239277546s                        
INFO[0141]      max: 2m10.787525438s                        
INFO[0141]      avg: 2m10.570908544s                        
INFO[0141] waiting 2min before reading                  
INFO[0261] reading...                                   
INFO[0361] values read:                                 
INFO[0361]      min: 1m39.023845491s                        
INFO[0361]      max: 1m39.026122698s                        
INFO[0361]      avg: 1m39.0250051s 
```
