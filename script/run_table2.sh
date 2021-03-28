#!/bin/bash

bash ./check_dax_fs.sh

cd ../build

rm CMakeCache.txt
cmake  -DENABLE_NAP_FLAG=ON  -DRECOVERY_TEST_FLAG=ON .. && make -j

exe=(cceh clevel clht masstree fastfair)

file_name=Table2_recovery
rm -rf $file_name

for e in ${exe[@]}
do  
    sleep 5
    echo "running... ${e}"
    ./${e}_nap /mnt/pm0/ycsb ../dataset/load ../dataset/run-read95-zipfan99-space192 71 > output
    res=`cat output | grep "recovery"`
    echo "============ ${e} ============" >> $file_name
    echo $res >> $file_name
done