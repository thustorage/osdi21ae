#!/bin/bash

bash ./check_dax_fs.sh

cd ../build

## 3-phase Switch
rm CMakeCache.txt
cmake -DENABLE_NAP_FLAG=ON -DSWITCH_TEST_FLAG=ON -DUSE_GLOBAL_LOCK_FLAG=OFF .. && make -j


echo "start 3-phase switch"
file_name=Fig12_3_phase
rm -rf throughput.txt 

./masstree_nap /mnt/pm0/ycsb ../dataset/load ../dataset/1-dy-1read50-zipfan99-space192 70 > output

mv throughput.txt  ${file_name}



## global-lock Switch
rm CMakeCache.txt
cmake -DENABLE_NAP_FLAG=ON -DSWITCH_TEST_FLAG=ON -DUSE_GLOBAL_LOCK_FLAG=ON .. && make -j


echo "start global-lock switch"
file_name=Fig12_global_lock
rm -rf throughput.txt 

./masstree_nap /mnt/pm0/ycsb ../dataset/load ../dataset/1-dy-1read50-zipfan99-space192 70 > output

mv throughput.txt  ${file_name}