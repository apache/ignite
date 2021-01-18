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
                    "-XX:ParallelGCThreads=$(((`nproc`/2)>1?(`nproc`/2):1)) " \
                    "-XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly " \
                    "-XX:+CMSParallelRemarkEnabled -XX:+CMSClassUnloadingEnabled"

JVM_PARAMS_GC_G1 = "-XX:+UseG1GC -XX:MaxGCPauseMillis=100 " \
                   "-XX:ConcGCThreads=$(((`nproc`/3)>1?(`nproc`/3):1)) " \
                   "-XX:ParallelGCThreads=$(((`nproc`*3/4)>1?(`nproc`*3/4):1)) "

JVM_PARAMS_GENERIC = "-server -da -XX:+DisableExplicitGC -XX:+AggressiveOpts -XX:+AlwaysPreTouch " \
                     "-XX:+ParallelRefProcEnabled -XX:+DoEscapeAnalysis " \
                     "-XX:+OptimizeStringConcat -XX:+UseStringDeduplication"


def create_jvm_settings(heap_size="768M", gc_settings=JVM_PARAMS_GC_CMS, generic_params=JVM_PARAMS_GENERIC,
                        gc_dump_path=None, oom_path=None, **kwargs):
    """
    Provides settings string for JVM process.
    param as_list: Represent JVM params as list.
    param as_map: Represent JVM params as dict.
    param opts: JVM options to merge. Adds new or rewrites default values. Can be dict, list or string.
    """
    gc_dump = ""
    if gc_dump_path:
        gc_dump = "-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=32M -XX:+PrintGCDateStamps " \
                  "-verbose:gc -XX:+PrintGCDetails -Xloggc:" + gc_dump_path

    out_of_mem_dump = ""
    if oom_path:
        out_of_mem_dump = "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=" + oom_path

    as_string = f"-Xmx{heap_size} -Xms{heap_size} {gc_settings} {gc_dump} {out_of_mem_dump} {generic_params}".strip()

    to_merge = kwargs.get("opts")

    if to_merge or kwargs.get("as_map"):
        return merge_jvm_settings(as_string, to_merge if to_merge else {}, **kwargs)

    if kwargs.get("as_list"):
        return as_string.split()

    return as_string


def merge_jvm_settings(src_settings, additionals, **kwargs):
    """
    Merges two JVM settings.
    :param src_settings: base settings. Can be dict, list or string.
    :param additionals: params to add to or overwrite in src_settings. Can be dict, list or string.
    param as_list: If True, represents result as list.
    param as_map: If True, represents result as dict.
    :return merged JVM settings. By default as string.
    """
    mapped = _to_map(src_settings)

    mapped.update(_to_map(additionals))

    _remove_duplicates(mapped)

    if kwargs.get("as_map"):
        return mapped

    as_list = []
    for param, value in mapped.items():
        if value:
            as_list.append(f"{param}={value}")
        else:
            as_list.append(param)

    if kwargs.get("as_list"):
        return as_list

    return ' '.join(as_list)


def _to_map(params):
    """"""
    if isinstance(params, dict):
        return params

    if isinstance(params, str):
        params = params.split()

    as_map = {}

    for elem in params:
        param_val = elem.split(sep="=", maxsplit=1)
        as_map[param_val[0]] = param_val[1] if len(param_val) > 1 else None

    return as_map


def _remove_duplicates(params: dict):
    """Removes specific duplicates"""
    duplicates = {"-Xmx": False, "-Xms": False}

    for param_key in list(params.keys()):
        for dup_key in duplicates.keys():
            if param_key.startswith(dup_key):
                if duplicates[dup_key]:
                    del params[param_key]
                else:
                    duplicates[dup_key] = True
