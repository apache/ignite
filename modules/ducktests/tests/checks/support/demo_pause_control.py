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
The host side of a demo breakpoint, for checks that drive one.

A held breakpoint blocks the thread that reached it, so everything the host does - reading the
published banner, dropping a resume file - has to happen from another one while the check
itself sits inside :meth:`ignitetest.utils.pause.DemoPause.pause`.

The protocol itself is spoken through :class:`ignitetest.utils.pause_control.ControlDir`, the
same class the test and ``docker/demo_console.py`` use - a check that hand rolled the file
names would stop checking the protocol and start checking its own copy of it.
"""

import threading
import time
from contextlib import contextmanager

from ignitetest.utils.pause import DEMO_PAUSE_TIMEOUT_SEC, DemoPause
from ignitetest.utils.pause_control import ControlDir

from checks.support.ducktape_doubles import FakeLogger

# Stands for the test a breakpoint was reached in; breakpoints report it to the host.
TEST_NAME = "check.CheckPause.check_something"

# Far longer than the fraction of a second a check actually holds a breakpoint for, and far
# shorter than the framework's own default: a resume that never arrives has to fail the check
# that expected it rather than hold the suite for ten minutes.
RESUME_TIMEOUT_SEC = 30


def new_demo_pause(control_dir, started_at=None, runner_timeout_sec=None, **test_globals):
    """
    :return: A DemoPause over the given control directory, logging into a FakeLogger its
             ``logger`` attribute hands back to the check.
    """
    test_globals.setdefault(DEMO_PAUSE_TIMEOUT_SEC, RESUME_TIMEOUT_SEC)

    return DemoPause(FakeLogger(), test_globals, TEST_NAME, control_dir=str(control_dir),
                     started_at=started_at, runner_timeout_sec=runner_timeout_sec)


def resume_with(control_dir, name, delay_sec=.05):
    """
    Creates a resume file from another thread, the way the host does while the test blocks.
    """
    control = ControlDir(control_dir)

    timer = threading.Timer(delay_sec, lambda: control.resume(name))
    timer.daemon = True
    timer.start()


@contextmanager
def published_status(control_dir, resume=None, timeout_sec=30):
    """
    Reads the published breakpoint while the check blocks on it, the way the host console
    does, and optionally resumes it.

    Polls for the file rather than reading it once after a fixed delay: a breakpoint that is
    only held for a fraction of a second - which is what these checks hold them for - would
    otherwise be a race against the machine the checks happen to run on.

    :param resume: Name of the resume file to create once the breakpoint has been read, None
           to leave it held.
    :return: A dict, empty on entry and filled with the published breakpoint by the time the
             block is left.
    """
    control = ControlDir(control_dir)
    published = {}

    def read():
        deadline = time.monotonic() + timeout_sec

        while time.monotonic() < deadline:
            status = control.read_status()

            if status is not None:
                published.update(status)

                break

            time.sleep(.01)

        if resume:
            control.resume(resume)

    reader = threading.Thread(target=read, daemon=True)
    reader.start()

    try:
        yield published
    finally:
        reader.join(timeout_sec)
