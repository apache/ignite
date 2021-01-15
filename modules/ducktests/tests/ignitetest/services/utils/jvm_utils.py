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

    if kwargs.get("as_list", False):
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
    as_map = _to_map(src_settings)

    as_map.update(_to_map(additionals))

    _remove_duplicates(as_map)

    if kwargs.get("as_map"):
        return as_map

    as_list = [e[0] + '=' + e[1] if len(e) > 0 and e[1] else e[0] for e in as_map.items()]

    if kwargs.get("as_list", False):
        return as_list

    return ' '.join(as_list)


def _to_map(params):
    """"""
    if isinstance(params, dict):
        return params

    if isinstance(params, list):
        params = iter(params)
    else:
        params = str(params).split()

    as_map = {}

    for key_val in [entry.split(sep='=', maxsplit=1) for entry in params]:
        as_map[str(key_val[0])] = str(key_val[1]) if len(key_val) > 1 else None

    return as_map


def _remove_duplicates(params: dict):
    """Removes specific duplicates"""
    duplicates = {"-xmx": [], "-xmn": []}

    for param_key in params.keys():
        for dbl in duplicates.items():
            if param_key.lower().startswith(dbl[0]):
                dbl[1].append(param_key)

    for dbl_lst in duplicates.values():
        for to_remove in dbl_lst[:-1]:
            params.pop(to_remove)
