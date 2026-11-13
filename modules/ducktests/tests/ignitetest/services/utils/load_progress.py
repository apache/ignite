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
Reads the progress that MdcContinuousLoadApplication records with progressIntervalMs set.
"""

from typing import List, NamedTuple


class LoadProgress(NamedTuple):
    """
    One progress record: its time, the successful and the failed operations so far, the time since the last success.
    """
    ts_ms: int
    ok: int
    err: int
    since_ok_ms: int


class LoadWindow(NamedTuple):
    """
    The load within a time window: successful and failed operations, the longest time without a success.
    """
    ok: int
    err: int
    max_stall_ms: int


def load_progress(app) -> List[LoadProgress]:
    """
    :return: Progress records of the load application service.
    """
    return [LoadProgress(*map(int, rec.split())) for rec in app.extract_results("Progress")]


def load_window(progress: List[LoadProgress], from_ms, to_ms) -> LoadWindow:
    """
    :return: The load between the two times, measured at the records within them, accurate to one record interval.
    """
    inside = [p for p in progress if from_ms <= p.ts_ms <= to_ms]

    assert inside, f"No load progress recorded between {from_ms} and {to_ms}"

    before = [p for p in progress if p.ts_ms < from_ms]
    start = before[-1] if before else LoadProgress(from_ms, 0, 0, 0)

    return LoadWindow(ok=inside[-1].ok - start.ok, err=inside[-1].err - start.err,
                      max_stall_ms=max(p.since_ok_ms for p in inside))
