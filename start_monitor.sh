#!/bin/sh

source /home/avramenko-da/.bash_profile
cd /home/avramenko-da/od_monitoring
date +%d-%m-%y/%H:%M:%S 
python main.py -db $1 -recievers $2 $3 $4 $5 $6 $7
