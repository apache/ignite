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

#
# Configuration to compare mvcc on/off. This benchmark creates load on mvcc processor.
#

now0=`date +'%H%M%S'`

# JVM options.
JVM_OPTS=${JVM_OPTS}" -DIGNITE_QUIET=false"

# Uncomment to enable concurrent garbage collection (GC) if you encounter long GC pauses.
JVM_OPTS=${JVM_OPTS}" \
-Xms8g \
-Xmx8g \
-Xloggc:./gc${now0}.log \
-XX:+PrintGCDetails \
-verbose:gc \
-XX:+UseParNewGC \
-XX:+UseConcMarkSweepGC \
-XX:+PrintGCDateStamps \
"

#Ignite version
ver="RELEASE-"

# List of default probes.
# Add DStatProbe or VmStatProbe if your OS supports it (e.g. if running on Linux).
BENCHMARK_DEFAULT_PROBES=ThroughputLatencyProbe,PercentileProbe,DStatProbe

# Packages where the specified benchmark is searched by reflection mechanism.
BENCHMARK_PACKAGES=org.yardstickframework,org.apache.ignite.yardstick

# Flag which indicates to restart the servers before every benchmark execution.
RESTART_SERVERS=true

# Probe point writer class name.
# BENCHMARK_WRITER=

# The benchmark is designed to run with 4 client nodes (drivers) and 1 server node.
SERVER_HOSTS=localhost
DRIVER_HOSTS=localhost,localhost,localhost,localhost

# Remote username.
# REMOTE_USER=

# Number of nodes, used to wait for the specified number of nodes to start.
nodesNum=$((`echo ${SERVER_HOSTS} | tr ',' '\n' | wc -l` + `echo ${DRIVER_HOSTS} | tr ',' '\n' | wc -l`))

# Warmup.
w=30

# Duration.
d=300

# Threads count.
t=16

# Sync mode.
sm=FULL_SYNC

# Parameters that should be the same across all the benchmarks launches.
commonParams="-cfg ${SCRIPT_DIR}/../config/ignite-localhost-config.xml -nn ${nodesNum} -w ${w} -d ${d} \
  -jdbc jdbc:ignite:thin://auto.find/ -t ${t} -sm ${sm} \
  --clientNodesAfterId 100 \
  -sn IgniteNode -cl --range 1000000 \
  --atomic-mode TRANSACTIONAL"

# Run configuration which contains all benchmarks.
# Note that each benchmark is set to run for 300 seconds (5 min) with warm-up set to 60 seconds (1 minute).
CONFIGS="\
${commonParams} -dn MvccProcessorBenchmark -ds ${ver}sql-update-batch-1-backup-0-mvcc-off -b 0 --sqlRange 1, \
${commonParams} -dn MvccProcessorBenchmark -ds ${ver}sql-update-batch-1-backup-0-mvcc-on -b 0 --sqlRange 1 --mvcc, \
  \
${commonParams} -dn MvccProcessorBenchmark -ds ${ver}sql-update-batch-25-backup-0-mvcc-off -b 0 --sqlRange 25, \
${commonParams} -dn MvccProcessorBenchmark -ds ${ver}sql-update-batch-25-backup-0-mvcc-on -b 0 --sqlRange 25 --mvcc, \
  \
${commonParams} -dn MvccProcessorBenchmark -ds ${ver}sql-update-batch-1000-backup-0-mvcc-off -b 0 --sqlRange 1000, \
${commonParams} -dn MvccProcessorBenchmark -ds ${ver}sql-update-batch-1000-backup-0-mvcc-on -b 0 --sqlRange 1000 --mvcc \
"
