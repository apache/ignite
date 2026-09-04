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

import re

from ignitetest.services.utils.decorators import memoize

DEFAULT_HEAP = "768M"

GC_G1 = "G1"
GC_PARALLEL = "PARALLEL"
GC_SERIAL = "SERIAL"
GC_Z = "ZGC"
GC_SHENANDOAH = "SHENANDOAH"

DEFAULT_GC = GC_G1

# NOTE: these strings are interpolated into a shell command that is evaluated on the remote
# node (see IgniteSpec._jvm_opts and IgniteNodeSpec.command), which is what makes the `nproc`
# substitutions work. Consequently NO option here may contain spaces or quotes.
_NPROC_THIRD = "$(((`nproc`/3)>1?(`nproc`/3):1))"
_NPROC_THREE_QUARTERS = "$(((`nproc`*3/4)>1?(`nproc`*3/4):1))"

# Garbage collector profiles. A profile is a mutually exclusive group: it both selects the collector
# and carries the tuning flags that are meaningful for it. Never mix flags across profiles.
GC_PROFILES = {
    GC_G1: [
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=100",
        f"-XX:ConcGCThreads={_NPROC_THIRD}",
        f"-XX:ParallelGCThreads={_NPROC_THREE_QUARTERS}",
        "-XX:+UseStringDeduplication",  # G1-only until JDK 18, hence part of the profile
    ],
    GC_PARALLEL: [
        "-XX:+UseParallelGC",
        f"-XX:ParallelGCThreads={_NPROC_THREE_QUARTERS}",
        # deliberately NO MaxGCPauseMillis: it flips ParallelGC into adaptive pause-goal sizing
    ],
    GC_SERIAL: [
        "-XX:+UseSerialGC",
    ],
    GC_Z: [
        "-XX:+UseZGC",  # product feature since JDK 15, no unlock flag needed
        f"-XX:ConcGCThreads={_NPROC_THIRD}",
        f"-XX:ParallelGCThreads={_NPROC_THREE_QUARTERS}",
    ],
    GC_SHENANDOAH: [
        "-XX:+UseShenandoahGC",  # product feature since JDK 15; OpenJDK only, not Oracle JDK
        f"-XX:ConcGCThreads={_NPROC_THIRD}",
        f"-XX:ParallelGCThreads={_NPROC_THREE_QUARTERS}",
    ],
}

JVM_PARAMS_GENERIC = "-server -XX:+DisableExplicitGC -XX:+AlwaysPreTouch " \
                     "-XX:+ParallelRefProcEnabled -XX:+DoEscapeAnalysis " \
                     "-XX:+OptimizeStringConcat"

# Matches a collector selector like -XX:+UseZGC. Deliberately narrow: it must not match
# -XX:+DisableExplicitGC or -XX:+UseStringDeduplication.
_GC_SELECTOR_PATTERN = re.compile(r"^-XX:([+-])(Use\w+GC)$")


class MultipleGcSelectedError(Exception):
    """
    Raised when JVM options end up selecting more than one garbage collector.
    """


def create_jvm_settings(heap_size=DEFAULT_HEAP, gc_settings=None, generic_params=JVM_PARAMS_GENERIC,
                        gc_dump_path=None, oom_path=None, vm_error_path=None):
    """
    Provides settings string for JVM process.
    :param heap_size: value for both -Xmx and -Xms.
    :param gc_settings: garbage collector options, see GC_PROFILES. Can be list or string.
                        Defaults to the DEFAULT_GC profile.
    :param generic_params: collector-independent options. Can be list or string.
    """
    gc_settings = GC_PROFILES[DEFAULT_GC] if gc_settings is None else gc_settings

    if isinstance(gc_settings, str):
        gc_settings = gc_settings.split()

    gc_dump = ""
    if gc_dump_path:
        gc_dump = "-Xlog:gc*=debug,gc+stats*=debug,gc+ergo*=debug:" + gc_dump_path + ":uptime,time,level,tags"

    out_of_mem_dump = ""
    if oom_path:
        out_of_mem_dump = "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=" + oom_path

    vm_error_dump = ""
    if vm_error_path:
        vm_error_dump = "-XX:ErrorFile=" + vm_error_path

    as_string = f"-Xmx{heap_size} -Xms{heap_size} {' '.join(gc_settings)} {gc_dump} " \
                f"{out_of_mem_dump} {vm_error_dump} {generic_params}".strip()

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

    validate_gc_settings(listed)

    return listed


def validate_gc_settings(jvm_opts):
    """
    Checks that at most one garbage collector is selected.

    GC selection is a mutually exclusive group, but merge_jvm_settings is a per-option overwrite model
    keyed on the substring before the first '=' -- so -XX:+UseG1GC and -XX:+UseZGC are different keys and
    both survive a merge. The resulting JVM aborts at startup with "Multiple garbage collectors selected",
    which surfaces on the Python side as an unexplained node startup timeout. Fail here instead.

    :param jvm_opts: JVM options to check. Can be list or string.
    :raise MultipleGcSelectedError: if more than one collector is enabled.
    """
    if isinstance(jvm_opts, str):
        jvm_opts = jvm_opts.split()

    # Last occurrence wins, matching how the JVM itself resolves repeated flags.
    selectors = {}

    for opt in jvm_opts:
        match = _GC_SELECTOR_PATTERN.match(opt)
        if match:
            selectors[match.group(2)] = (match.group(1) == "+", opt)

    enabled = sorted(opt for is_enabled, opt in selectors.values() if is_enabled)

    if len(enabled) > 1:
        raise MultipleGcSelectedError(
            f"Multiple garbage collectors selected: {', '.join(enabled)}. "
            f"Select a collector with the 'gc' global instead of passing it via jvm_opts, "
            f"e.g. --global-json '{{\"gc\": \"ZGC\"}}'. "
            f"Valid profiles: {', '.join(sorted(GC_PROFILES))}.")

    return jvm_opts


def java_major_version(version):
    """
    :param version: Full java version
    :return: Java major version
    """
    if version:
        version = version.split('.')

        return int(version[1]) if version[0] == '1' else int(version[0])

    return -1


def java_version(node):
    """
    :param node: Ducktape cluster node
    :return: java version
    """
    cmd = r"java -version 2>&1 | awk -F[\"\-] '/version/ {print $2}'"

    raw_version = list(node.account.ssh_capture(cmd, allow_fail=False))

    return raw_version[0].strip() if raw_version else ''


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
        cmd = "ps -C java -wwo pid,args | grep '%s' | awk -F' ' '{print $1}'" % java_class

        return [int(pid) for pid in node.account.ssh_capture(cmd, allow_fail=True)]


class JvmVersionMixin:
    """
    Mixin to get java version on node.
    """
    @memoize
    def java_version(self):
        """
        :return: Full java version of service.
        """
        return java_version(self.nodes[0])
