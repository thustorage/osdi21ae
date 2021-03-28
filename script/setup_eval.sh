#!/bin/bash

umount /mnt/pm0
umount /mnt/pm1
umount /mnt/pm2
umount /mnt/pm3

ndctl create-namespace --mode=fsdax -e namespace0.0 -f

yes | mkfs.ext4 /dev/pmem0
yes | mkfs.ext4 /dev/pmem1
yes | mkfs.ext4 /dev/pmem2
yes | mkfs.ext4 /dev/pmem3

mount -o dax /dev/pmem0 /mnt/pm0
mount -o dax /dev/pmem1 /mnt/pm1
mount -o dax /dev/pmem2 /mnt/pm2
mount -o dax /dev/pmem3 /mnt/pm3