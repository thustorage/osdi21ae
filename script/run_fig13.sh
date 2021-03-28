#!/bin/bash

bash ./check_dax_fs.sh

cd ../build

rm CMakeCache.txt
cmake .. && make -j

threads=(2 6 12 18 24 30 36 42 48 54 60 66 71)

echo "start NR WI"
file_name=Fig13_NR_WI
echo "" > $file_name

for t in ${threads[@]}
do  
    sleep 5
    echo "running... #thread=${t}"
    ./nr_clht /mnt/pm0/ycsb ../dataset/load ../dataset/run-read50-zipfan99-space192 ${t} > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done

echo "start NR RI"
file_name=Fig13_NR_RI
echo "" > $file_name

for t in ${threads[@]}
do  
    sleep 5
    echo "running... #thread=${t}"
    ./nr_clht /mnt/pm0/ycsb ../dataset/load ../dataset/run-read95-zipfan99-space192 ${t} > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done

