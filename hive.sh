#!/bin/sh



echo "hdfs dfs -ls /data/core/external/$1/stg/$2:\n"
hdfs dfs -ls /data/core/external/$1/stg/$2
echo "hdfs dfs -ls /data/core/external/$1/stg/$2/pa/*/*:\n"
hdfs dfs -ls /data/core/external/$1/stg/$2/pa/*/*

hive -f /home/avramenko-da/od_monitoring/hive_tmp.hql
