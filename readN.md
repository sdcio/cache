## get N highest priorities for a given path

```shell
bin/cachectl list
bin/cachectl create -n target1
bin/cachectl list
#
bin/cachectl modify -n target1 -s intended --priority 10 --owner ow1 --update a,b,c1:::string:::1 --update a,b,c2:::string:::2
bin/cachectl modify -n target1 -s intended --priority 20 --owner ow2 --update a,b,c1:::string:::1 --update a,b,c2:::string:::2
bin/cachectl modify -n target1 -s intended --priority 30 --owner ow3 --update a,b,c1:::string:::1 --update a,b,c2:::string:::2
bin/cachectl modify -n target1 -s intended --priority 40 --owner ow4 --update a,b,c1:::string:::1 --update a,b,c2:::string:::2
#
bin/cachectl read -n target1 -p a,b -s intended --format flat
bin/cachectl read -n target1 -p a,b -s intended --format flat --priority -1

# get single path
bin/cachectl read -n target1 -p a,b,c1 -s intended --format flat --priority-count 2
bin/cachectl read -n target1 -p a,b,c1 -s intended --format flat --priority-count 3
bin/cachectl read -n target1 -p a,b,c1 -s intended --format flat --priority-count 4
bin/cachectl read -n target1 -p a,b,c1 -s intended --format flat --priority-count 5
# get multiple paths
bin/cachectl read -n target1 -p a,b -s intended --format flat --priority-count 2
bin/cachectl read -n target1 -p a,b -s intended --format flat --priority-count 3
bin/cachectl read -n target1 -p a,b -s intended --format flat --priority-count 4
bin/cachectl read -n target1 -p a,b -s intended --format flat --priority-count 5
```
