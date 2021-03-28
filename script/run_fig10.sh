#!/bin/bash

bash ./check_dax_fs.sh

cd ../build

## Disable Nap
rm CMakeCache.txt
cmake -DENABLE_NAP_FLAG=OFF -DTEST_LATENCY_FLAG=ON .. && make -j


echo "start raw_index latency"
file_name=Fig10_lat_Raw
rm -rf latency.txt

./clht_nap /mnt/pm0/ycsb ../dataset/load ../dataset/run-read50-zipfan99-space192 71 > output

mv latency.txt ${file_name}

## Enable Nap
rm CMakeCache.txt
cmake  -DENABLE_NAP_FLAG=ON -DTEST_LATENCY_FLAG=ON .. && make -j

echo "start nap_index latency"
file_name=Fig10_lat_Nap
rm -rf latency.txt

./clht_nap /mnt/pm0/ycsb ../dataset/load ../dataset/run-read50-zipfan99-space192 71 > output

mv latency.txt ${file_name}
