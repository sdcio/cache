
#
cache_name="target1"
targets=$(bin/cachectl list)
found=0

while IFS= read -r line; do
    if [ "$line" = "$cache_name" ]; then
        found=1
        break
    fi
done <<< "$targets"

if [ $found -eq 0 ]; then
    bin/cachectl create -n target1
fi

# write n * 10k kv
for n in {0..9}
do
    echo "writing batch $n"
    updates=""
    for i in {0..9999}
    do
        updates+=" --update a,b,$n,c$i:::string:::$i"
    done
    # echo $updates
    bin/cachectl modify -n target1 $updates
done

# sleep 10
# prune all but one value
id=$(bin/cachectl prune -n target1)
bin/cachectl modify -n target1 --update a,b,0,c0:::string:::0

echo ""
echo "pruning..."
time bin/cachectl prune -n target1 --id $id

echo ""
echo "after prune read:"
bin/cachectl read -n target1 -p a,b --format flat


