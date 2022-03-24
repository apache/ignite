#!/bin/bash


GLOBALS='{
  "max_data_region_size":7500000000
}'
PARAMETERS='{
  "jvm_opts": ["-Xmx1G"],
  "entry_count":600000,
  "entry_size":300,
  "batch_size": 100,
  "job_size": 5,
  "preloaders":8,
  "preloader_threads":2,
  "clients":16,
  "client_threads":4,
  "ignite_version":"dev"
}'
bash ./run_tests.sh -n 23 -gj "$GLOBALS" -pj "$PARAMETERS" -t ./ignitetest/tests/stress


#GLOBALS='{
#    "max_data_region_size":10000000000,
#    "cluster_size":4
#}'

#PARAMETERS='{
#  "backups":1,
#  "cache_count":1,
#  "preloaders":2,
#  "threads":4,
#  "jvm_opts": ["-Xmx1G"],
#  "entry_count":1000000,
#  "entry_size":1024,
#  "ignite_version":"dev"
#}'
# bash ./run_tests.sh -gj "$GLOBALS" -pj "$PARAMETERS" -t ./ignitetest/tests/streamer/simple_load_test.py
