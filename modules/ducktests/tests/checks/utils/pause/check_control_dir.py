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
Checks the control directory itself - the files a paused test and the host exchange, and the
polling that waits for them - with no breakpoint on top of it.
"""

import os
import time

from ignitetest.utils.pause_control import ABORT, CONTINUE_ALL, ControlDir, STATUS_JSON, STATUS_TXT, continue_file


def check_consuming_a_file_removes_it(tmp_path):
    """
    Check that consuming a control file reports it and removes it: the test is the only party
    that deletes what the host wrote, so consuming one is what acknowledges it.
    """
    control = ControlDir(tmp_path)

    assert not control.consume(ABORT), "nothing was written yet"

    control.resume(ABORT)

    assert control.exists(ABORT)
    assert control.consume(ABORT)
    assert not control.exists(ABORT), "a consumed file must not be left for the next breakpoint"
    assert not control.consume(ABORT)


def check_sweep_spares_a_held_breakpoint(tmp_path):
    """
    Check the asymmetry the two sides need: both clear the resume files an earlier run left,
    but only the test drops the banner as well. The console is just as likely to have been
    started against a test that is already holding one.
    """
    control = ControlDir(tmp_path)

    control.publish(["PAUSED 1   split-brain"], {"seq": 1})
    control.resume(continue_file(3))
    control.resume(CONTINUE_ALL)

    control.sweep()

    assert not control.exists(continue_file(3))
    assert not control.exists(CONTINUE_ALL)
    assert control.read_status() is not None, "the console must not withdraw a breakpoint it did not hold"

    control.sweep(status=True)

    assert control.read_status() is None
    assert not control.exists(STATUS_TXT)


def check_sweeping_a_directory_that_is_not_there(tmp_path):
    """
    Check that sweeping is safe before anything has been created: without the global no
    breakpoint ever makes the directory, and the console may well be started first.
    """
    control = ControlDir(tmp_path / "never-created")

    control.sweep(status=True)

    assert control.read_status() is None
    assert not os.path.exists(control.path), "sweeping must not create what it was asked to clean"


def check_sweep_clears_what_an_interrupted_write_left(tmp_path):
    """
    Check that the temporary name a write goes through is swept too, whichever file it
    belonged to. Every control file is matched by prefix for this reason: one matched exactly
    would leave its ".tmp" in a directory that nothing else ever visits.
    """
    control = ControlDir(tmp_path)

    for interrupted in (continue_file(1), CONTINUE_ALL, ABORT, STATUS_TXT, STATUS_JSON):
        control.write(interrupted + ".tmp", "")

    control.sweep(status=True)

    assert os.listdir(control.path) == []


def check_a_resume_file_lands_without_a_temporary(tmp_path):
    """
    Check that resuming creates the file and nothing besides: a resume file is empty, so an
    atomic replace would buy nothing and leave a transient behind for the sweep to know about.
    """
    control = ControlDir(tmp_path)

    control.resume(ABORT)

    assert os.listdir(control.path) == [ABORT]


def check_publishing_round_trips(tmp_path):
    """
    Check that what is published is what a reader gets back, and that withdrawing it leaves
    nothing of either file behind.
    """
    control = ControlDir(tmp_path)

    control.publish(["PAUSED 1   split-brain", "  test  some.Test"], {"seq": 1, "name": "split-brain"})

    assert control.read_status() == {"seq": 1, "name": "split-brain"}

    with open(control.file(STATUS_TXT), encoding="utf-8") as file:
        assert file.read() == "PAUSED 1   split-brain\n  test  some.Test\n"

    control.clear_status()

    assert control.read_status() is None
    assert not control.exists(STATUS_TXT)
    assert not control.exists(STATUS_JSON)


def check_unreadable_status_reads_as_nothing_published(tmp_path):
    """
    Check that a status file caught half written reads as "not paused" rather than raising:
    the host polls this in a loop and simply comes back.
    """
    control = ControlDir(tmp_path)

    control.write(STATUS_JSON, '{"seq": 1')

    assert control.read_status() is None


def check_awaiting_takes_the_file_that_arrived(tmp_path):
    """
    Check the wait ends on the file that appears, and hands back which one it was.
    """
    control = ControlDir(tmp_path)

    control.resume(continue_file(1))

    assert control.await_any([ABORT, continue_file(1)], 5) == continue_file(1)
    assert not control.exists(continue_file(1)), "the wait must consume what ended it"


def check_awaiting_honours_the_order_it_was_given(tmp_path):
    """
    Check that the first name wins when several are already there: abort has to beat a
    continue that landed in the same interval, or a demo would be resumed instead of ended.
    """
    control = ControlDir(tmp_path)

    control.resume(CONTINUE_ALL)
    control.resume(ABORT)

    assert control.await_any([ABORT, CONTINUE_ALL], 5) == ABORT
    assert control.exists(CONTINUE_ALL), "only the file that ended the wait may be consumed"


def check_awaiting_gives_up(tmp_path):
    """
    Check that a wait nobody answers ends on its own rather than holding the scenario until
    ducktape kills it.
    """
    control = ControlDir(tmp_path)

    started_at = time.monotonic()

    assert control.await_any([ABORT], .3) is None
    assert time.monotonic() - started_at >= .3, "it must have waited for what it was given"


def check_awaiting_ticks_without_being_told_how_often(tmp_path):
    """
    Check that a caller who wants to hear that the wait is still running need not also pick an
    interval - asking for one and getting a TypeError out of a held breakpoint would be a poor
    way to find out that the two arguments go together.
    """
    control = ControlDir(tmp_path)

    ticks = []

    assert control.await_any([ABORT], .6, tick=ticks.append) is None
    assert ticks, "a wait longer than one poll must report itself"


def check_awaiting_reports_that_it_is_still_waiting(tmp_path):
    """
    Check that the caller is ticked while the wait runs, which is how a held breakpoint says
    in the test log that it is paused rather than stuck - without this class having to know
    what a log is.
    """
    control = ControlDir(tmp_path)

    ticks = []

    assert control.await_any([ABORT], 1.2, tick=ticks.append, tick_sec=.01) is None
    assert ticks, "a wait longer than the tick interval must report itself"
    assert all(0 < left <= 1.2 for left in ticks), ticks
    assert ticks == sorted(ticks, reverse=True), "each tick must report less time left than the last"
