#
#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.
#

#
# Configuration to measure performance of concurrent sql updates with contention.
# Update keys are shared among the threads/hosts.
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

# The benchmark is designed to run with 4 client node (drivers) and several (2 for instance) server nodes
SERVER_HOSTS=localhost,localhost
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
  -sn IgniteNode -cl \
  --range 1000000 --mvcc-contention-range 10000"

# Run configuration which contains all benchmarks.
# Note that each benchmark is set to run for 300 seconds (5 min) with warm-up set to 60 seconds (1 minute).
CONFIGS="\
${commonParams} -dn MvccUpdateContentionBenchmark -ds ${ver}sql-update-batch-1-backup-0-mvcc-off -b 0 --sqlRange 1 --atomic-mode TRANSACTIONAL, \
${commonParams} -dn MvccUpdateContentionBenchmark -ds ${ver}sql-update-batch-1-backup-0-mvcc-on -b 0 --sqlRange 1 --atomic-mode TRANSACTIONAL_SNAPSHOT, \
  \
${commonParams} -dn MvccUpdateContentionBenchmark -ds ${ver}sql-update-batch-25-backup-0-mvcc-off -b 0 --sqlRange 25 --atomic-mode TRANSACTIONAL, \
${commonParams} -dn MvccUpdateContentionBenchmark -ds ${ver}sql-update-batch-25-backup-0-mvcc-on -b 0 --sqlRange 25 --atomic-mode TRANSACTIONAL_SNAPSHOT, \
  \
${commonParams} -dn MvccUpdateContentionBenchmark -ds ${ver}sql-update-batch-1000-backup-0-mvcc-off -b 0 --sqlRange 1000 --atomic-mode TRANSACTIONAL, \
${commonParams} -dn MvccUpdateContentionBenchmark -ds ${ver}sql-update-batch-1000-backup-0-mvcc-on -b 0 --sqlRange 1000 --atomic-mode TRANSACTIONAL_SNAPSHOT \
"
