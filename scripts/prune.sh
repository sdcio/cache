bin/cachectl create -n target1

bin/cachectl modify -n target1 --update a,b,c1:::string:::1 --update a,b,c2:::string:::2
bin/cachectl modify -n target1 --update a,b,c3:::string:::3
bin/cachectl modify -n target1 --update a,b,c4:::string:::4
bin/cachectl read -n target1 -p a,b --format flat

# prune requests that go beyond the uint8 prune index
for i in {0..300}
do
    echo ""
    echo "Index: $i"
    
    id=$(bin/cachectl prune -n target1)
    bin/cachectl modify -n target1 --update a,b,c4:::string:::$i
    if [ $((i % 2)) -eq 0 ]; then
        bin/cachectl modify -n target1 --update a,b,c5:::string:::$i
    fi
    bin/cachectl prune -n target1 --id $id
    bin/cachectl read -n target1 -p a,b --format flat
    sleep 0.1
done
