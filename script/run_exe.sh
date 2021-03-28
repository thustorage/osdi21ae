#!/bin/bash

bash ./check_dax_fs.sh


cd ../build

## Disable Nap
rm CMakeCache.txt
cmake -DENABLE_NAP_FLAG=OFF .. && make -j

threads=(2 6 12 18 24 30 36 42 48 54 60 66 71)

exe=./$1_nap

echo "start raw_index WI - $1"
file_name=${1}_WI_Raw
echo "" > $file_name
for t in ${threads[@]}
do  
    sleep 5
    echo "running... #thread=${t}"
    ./$exe /mnt/pm0/ycsb ../dataset/load ../dataset/run-read50-zipfan99-space192 ${t} > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done

echo "start raw_index RI - $1"
file_name=${1}_RI_Raw
echo "" > $file_name
for t in ${threads[@]}
do
    sleep 5
    echo "running... #thread=${t}"
    ./$exe /mnt/pm0/ycsb ../dataset/load ../dataset/run-read95-zipfan99-space192 ${t} > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done


## Enable Nap

rm CMakeCache.txt
cmake  -DENABLE_NAP_FLAG=ON .. && make -j

echo "start nap_index WI - $1"
file_name=${1}_WI_Nap
echo "" > $file_name
for t in ${threads[@]}
do
    echo "running... #thread=${t}"
    ./$exe /mnt/pm0/ycsb ../dataset/load ../dataset/run-read50-zipfan99-space192 ${t} > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done

echo "start nap_index RI - $1"
file_name=${1}_RI_Nap
echo "" > $file_name
for t in ${threads[@]}
do
    echo "running... #thread=${t}"

    ./$exe /mnt/pm0/ycsb ../dataset/load ../dataset/run-read95-zipfan99-space192 ${t} > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done