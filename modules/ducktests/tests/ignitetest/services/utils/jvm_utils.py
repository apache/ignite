# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module contains JVM utilities.
"""

JVM_PARAMS_GC_CMS = "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSIncrementalMode " \
                    "-XX:ConcGCThreads=$(((`nproc`/4)>1?(`nproc`/4):1)) " \
                    "-XX:ParallelGCThreads=$(((`nproc`*3/4)>1?(`nproc`*3/4):1)) " \
                    "-XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly " \
                    "-XX:+CMSParallelRemarkEnabled -XX:+CMSClassUnloadingEnabled"

JVM_PARAMS_GC_G1 = "-XX:+UseG1GC -XX:MaxGCPauseMillis=100 " \
                   "-XX:ConcGCThreads=$(((`nproc`/3)>1?(`nproc`/3):1)) " \
                   "-XX:ParallelGCThreads=$(((`nproc`*3/4)>1?(`nproc`*3/4):1)) "

JVM_PARAMS_GENERIC = "-server -da -XX:+DisableExplicitGC -XX:+AggressiveOpts -XX:+AlwaysPreTouch " \
                     "-XX:+ParallelRefProcEnabled -XX:+DoEscapeAnalysis " \
                     "-XX:+OptimizeStringConcat -XX:+UseStringDeduplication"


def jvm_settings(heap_size="768M", gc_settings=JVM_PARAMS_GC_G1, generic_params=JVM_PARAMS_GENERIC, gc_dump_path=None,
                 oom_path=None):
    """Provides settings string for JVM process."""
    gc_dump = "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=32M " \
              "-XX:+PrintGCDateStamps -verbose:gc -XX:+PrintGCDetails -Xloggc:" + gc_dump_path \
        if gc_dump_path else ""

    out_of_mem_dump = "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=" + oom_path \
        if oom_path else ""

    return f"-Xmx{heap_size} -Xms{heap_size} {gc_settings} {gc_dump} {out_of_mem_dump} {generic_params}".strip()
