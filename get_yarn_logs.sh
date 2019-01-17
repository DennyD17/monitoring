#!/bin/sh



hdfs dfs -get /tmp/logs/$1/logs/$2 $3
