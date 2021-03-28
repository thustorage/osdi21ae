#!/bin/bash

if [ -c "/dev/dax0.0" ]; then
  bash ./setup_eval.sh 
fi

fs=`mount`
for i in 0 1 2 3
do
str=`echo $fs | grep pm$i`
if [[ -z $str ]]; then
    bash ./setup_eval.sh 
fi
done