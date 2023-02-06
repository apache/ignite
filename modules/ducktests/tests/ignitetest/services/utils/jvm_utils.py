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
DEFAULT_HEAP = "768M"

JVM_PARAMS_GC_G1 = "-XX:+UseG1GC -XX:MaxGCPauseMillis=100 " \
                   "-XX:ConcGCThreads=$(((`nproc`/3)>1?(`nproc`/3):1)) " \
                   "-XX:ParallelGCThreads=$(((`nproc`*3/4)>1?(`nproc`*3/4):1)) "

JVM_PARAMS_GENERIC = "-server -XX:+DisableExplicitGC -XX:+AlwaysPreTouch " \
                     "-XX:+ParallelRefProcEnabled -XX:+DoEscapeAnalysis " \
                     "-XX:+OptimizeStringConcat -XX:+UseStringDeduplication"


def create_jvm_settings(heap_size=DEFAULT_HEAP, gc_settings=JVM_PARAMS_GC_G1, generic_params=JVM_PARAMS_GENERIC,
                        gc_dump_path=None, oom_path=None):
    """
    Provides settings string for JVM process.
    param opts: JVM options to merge. Adds new or rewrites default values. Can be list or string.
    """
    gc_dump = ""
    if gc_dump_path:
        gc_dump = "-verbose:gc -Xloggc:" + gc_dump_path

    out_of_mem_dump = ""
    if oom_path:
        out_of_mem_dump = "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=" + oom_path

    as_string = f"-Xmx{heap_size} -Xms{heap_size} {gc_settings} {gc_dump} {out_of_mem_dump} {generic_params}".strip()

    return as_string.split()


def merge_jvm_settings(src_settings, additionals):
    """
    Merges two JVM settings.
    :param src_settings: base settings. Can be list or string.
    :param additionals: params to add to or overwrite in src_settings. Can be list or string.
    :return merged JVM settings. By default as string.
    """
    mapped = _to_map(src_settings)

    mapped.update(_to_map(additionals))

    _remove_duplicates(mapped)

    listed = []
    for param, value in mapped.items():
        if value:
            listed.append(f"{param}={value}")
        else:
            listed.append(param)

    return listed


def _to_map(params):
    """"""
    assert isinstance(params, (str, list)), "JVM params an be string or list only."

    if isinstance(params, str):
        params = params.split()

    mapped = {}

    for elem in params:
        param_val = elem.split(sep="=", maxsplit=1)
        mapped[param_val[0]] = param_val[1] if len(param_val) > 1 else None

    return mapped


def _remove_duplicates(params: dict):
    """Removes specific duplicates"""
    duplicates = {"-Xmx": False, "-Xms": False, "-Xss": False, "-Xmn": False}

    for param_key in reversed(list(params.keys())):
        for dup_key, _ in duplicates.items():
            if param_key.startswith(dup_key):
                if duplicates[dup_key]:
                    del params[param_key]
                else:
                    duplicates[dup_key] = True


class JvmProcessMixin:
    """
    Mixin to work with JVM processes
    """
    @staticmethod
    def pids(node, java_class):
        """
        Return pids of jvm processes running this java class on service node.
        :param node: Service node.
        :param java_class: Java class name
        :return: List of service's pids.
        """
        cmd = "pgrep -ax java | awk '/%s/ {print $1}'" % java_class

        return [int(pid) for pid in node.account.ssh_capture(cmd, allow_fail=True)]
