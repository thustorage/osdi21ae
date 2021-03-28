#!/bin/bash

bash ./check_dax_fs.sh

cd ../build

rm CMakeCache.txt
cmake  -DENABLE_NAP_FLAG=ON .. && make -j


# Figure 14 (a)
hotset=(10000 50000 100000 500000 1000000)

echo "start Nap (Hot Set Size)"
file_name=Fig14_hotset_Nap
rm -rf $file_name

for size in ${hotset[@]}
do
    sleep 5
    echo "running... #size_hot_size=${size}"
    ./clht_nap /mnt/pm0/ycsb ../dataset/load ../dataset/run-read50-zipfan99-space192 71 ${size} > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done

# Figure 14 (b) - nap
keyspace=(10 50 100 500 1000)

echo "start Nap (Key Space)"
file_name=Fig14_keyspace_Nap
rm -rf  $file_name

for space in ${keyspace[@]}
do
    sleep 5
    echo "running... #key_space_size=${space}"
    ./clht_nap /mnt/pm0/ycsb ../dataset/load ../dataset/sen-read50-zipfan99-space${space} 71 > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done


# Figure 14 (c) - nap
zipfan=(90 94 96 98 99)

echo "start Nap (Zipfan)"
file_name=Fig14_zipfan_Nap
rm -rf $file_name

for zv in ${zipfan[@]}
do
    sleep 5
    echo "running... #zipfan=${zv}"
    ./clht_nap /mnt/pm0/ycsb ../dataset/load ../dataset/sen-read50-zipfan${zv}-space192 71 > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done


### Disable Nap
rm CMakeCache.txt
cmake  -DENABLE_NAP_FLAG=OFF .. && make -j


# Figure 14 (b) - raw
keyspace=(10 50 100 500 1000)

echo "start raw (Key Space)"
file_name=Fig14_keyspace_Raw
rm -rf $file_name

for space in ${keyspace[@]}
do
    sleep 5
    echo "running... #key_space_size=${space}"
    ./clht_nap /mnt/pm0/ycsb ../dataset/load ../dataset/sen-read50-zipfan99-space${space} 71 > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done


# Figure 14 (c) - raw
zipfan=(90 94 96 98 99)

echo "start raw (Zipfan)"
file_name=Fig14_zipfan_Raw
rm -rf $file_name

for zv in ${zipfan[@]}
do
    sleep 5
    echo "running... #zipfan=${zv}"
    ./clht_nap /mnt/pm0/ycsb ../dataset/load ../dataset/sen-read50-zipfan${zv}-space192 71 > output
    res=`cat output | grep "reqs per second"`
    echo $res >> $file_name
done