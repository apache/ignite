#!/bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -----------------------------------------------------------------------------------------------
# This file specifies environment specific settings to bootstrap required infrastructure for:
# -----------------------------------------------------------------------------------------------
#
#   1) Cassandra cluster
#   2) Ignite cluster
#   3) Tests cluster
#   4) Ganglia agents to be installed on each clusters machine
#   5) Ganglia master to collect metrics from agent and show graphs on Ganglia Web dashboard
#
# -----------------------------------------------------------------------------------------------

# EC2 tagging related settings
export EC2_OWNER_TAG=ignite@apache.org
export EC2_PROJECT_TAG=ignite
export EC2_CASSANDRA_TAG=CASSANDRA
export EC2_IGNITE_TAG=IGNITE
export EC2_TEST_TAG=TEST
export EC2_GANGLIA_TAG=GANGLIA

# Tests summary settings
export CASSANDRA_NODES_COUNT=3
export IGNITE_NODES_COUNT=3
export TEST_NODES_COUNT=2
export TESTS_TYPE="ignite"

# Time (in minutes) to wait for Cassandra/Ignite node up and running and register it in S3
export SERVICE_STARTUP_TIME=10

# Number of attempts to start Cassandra/Ignite daemon
export SERVICE_START_ATTEMPTS=3

# Root S3 folder
export S3_ROOT=s3://<bucket>/<folder>

# S3 folder for downloads. You should put here ignite load tests jar archive
# (you can also download here other required artifacts like Cassandra, Ignite and etc)
export S3_DOWNLOADS=$S3_ROOT/test

# S3 root system folders where to store all infrastructure info
export S3_SYSTEM=$S3_ROOT/test1

# S3 system folders to store cluster specific info
export S3_CASSANDRA_SYSTEM=$S3_SYSTEM/cassandra
export S3_IGNITE_SYSTEM=$S3_SYSTEM/ignite
export S3_TESTS_SYSTEM=$S3_SYSTEM/tests
export S3_GANGLIA_SYSTEM=$S3_SYSTEM/ganglia

# Logs related settings
export S3_LOGS_TRIGGER=$S3_SYSTEM/logs-trigger
export S3_LOGS_ROOT=$S3_SYSTEM/logs
export S3_CASSANDRA_LOGS=$S3_LOGS_ROOT/cassandra
export S3_IGNITE_LOGS=$S3_LOGS_ROOT/ignite
export S3_TESTS_LOGS=$S3_LOGS_ROOT/tests
export S3_GANGLIA_LOGS=$S3_LOGS_ROOT/ganglia

# Cassandra related settings
export CASSANDRA_DOWNLOAD_URL=http://archive.apache.org/dist/cassandra/3.5/apache-cassandra-3.5-bin.tar.gz
export S3_CASSANDRA_BOOTSTRAP_SUCCESS=$S3_CASSANDRA_SYSTEM/success
export S3_CASSANDRA_BOOTSTRAP_FAILURE=$S3_CASSANDRA_SYSTEM/failure
export S3_CASSANDRA_NODES_DISCOVERY=$S3_CASSANDRA_SYSTEM/discovery
export S3_CASSANDRA_FIRST_NODE_LOCK=$S3_CASSANDRA_SYSTEM/first-node-lock
export S3_CASSANDRA_NODES_JOIN_LOCK=$S3_CASSANDRA_SYSTEM/join-lock

# Ignite related settings
export IGNITE_DOWNLOAD_URL=$S3_DOWNLOADS/apache-ignite-fabric-1.8.0-SNAPSHOT-bin.zip
export S3_IGNITE_BOOTSTRAP_SUCCESS=$S3_IGNITE_SYSTEM/success
export S3_IGNITE_BOOTSTRAP_FAILURE=$S3_IGNITE_SYSTEM/failure
export S3_IGNITE_NODES_DISCOVERY=$S3_IGNITE_SYSTEM/discovery
export S3_IGNITE_FIRST_NODE_LOCK=$S3_IGNITE_SYSTEM/first-node-lock
export S3_IGNITE_NODES_JOIN_LOCK=$S3_IGNITE_SYSTEM/i-join-lock

# Tests related settings
export TESTS_PACKAGE_DONLOAD_URL=$S3_DOWNLOADS/ignite-cassandra-tests-1.8.0-SNAPSHOT.zip
export S3_TESTS_TRIGGER=$S3_SYSTEM/tests-trigger
export S3_TESTS_NODES_DISCOVERY=$S3_TESTS_SYSTEM/discovery
export S3_TESTS_SUCCESS=$S3_TESTS_SYSTEM/success
export S3_TESTS_FAILURE=$S3_TESTS_SYSTEM/failure
export S3_TESTS_IDLE=$S3_TESTS_SYSTEM/idle
export S3_TESTS_PREPARING=$S3_TESTS_SYSTEM/preparing
export S3_TESTS_WAITING=$S3_TESTS_SYSTEM/waiting
export S3_TESTS_RUNNING=$S3_TESTS_SYSTEM/running
export S3_TESTS_FIRST_NODE_LOCK=$S3_TESTS_SYSTEM/first-node-lock
export S3_TESTS_SUMMARY=$S3_SYSTEM/t-summary.zip

# Ganglia related settings
export GANGLIA_CORE_DOWNLOAD_URL=https://github.com/ganglia/monitor-core.git
export GANGLIA_WEB_DOWNLOAD_URL=https://github.com/ganglia/ganglia-web.git
export RRD_DOWNLOAD_URL=http://oss.oetiker.ch/rrdtool/pub/rrdtool-1.3.1.tar.gz
export GPERF_DOWNLOAD_URL=http://ftp.gnu.org/gnu/gperf/gperf-3.0.3.tar.gz
export EPEL_DOWNLOAD_URL=https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
export S3_GANGLIA_BOOTSTRAP_SUCCESS=$S3_GANGLIA_SYSTEM/success
export S3_GANGLIA_BOOTSTRAP_FAILURE=$S3_GANGLIA_SYSTEM/failure
export S3_GANGLIA_MASTER_DISCOVERY=$S3_GANGLIA_SYSTEM/discovery