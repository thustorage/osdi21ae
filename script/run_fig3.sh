#!/bin/bash

bash ./check_dax_fs.sh

cd ../build

rm CMakeCache.txt
cmake .. && make -j

keyspace=(10 100 200 400 800 1000 2000)

for t in ${keyspace[@]}
do
   echo === KeySpace: ${t} M === 
  ./test_zipfan ${t}
done