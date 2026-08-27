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
The directory a paused test and the host talk over, and the file protocol they talk in.

The protocol is one sided while a breakpoint is held - the host only ever creates files, the
test is the only party that deletes them - so no step of a held breakpoint can race with the
host:

    - the test publishes ``paused.txt`` (a rendered banner) and ``paused.json`` (the same
      content as data) and then blocks;
    - the host creates ``continue-<seq>``, ``continue-all`` or ``abort``;
    - the test consumes that file, removes it along with its own status files, and proceeds.

Both sides drive it through :class:`ControlDir`, which owns the mechanics alone - paths,
atomic writes, sweeping, polling. What a file *means* is the caller's business:
:mod:`ignitetest.utils.pause` decides that ``abort`` fails the scenario, and
``docker/demo_console.py`` decides which one to write.

Standard library only, and deliberately free of every ignitetest import: the console loads
this module by path, on a host that has neither ducktape nor ignitetest installed.
"""

import json
import os
import time

CONTROL_DIR_NAME = ".ducktests-demo"

STATUS_TXT = "paused.txt"
STATUS_JSON = "paused.json"
CONTINUE_PREFIX = "continue-"
CONTINUE_ALL = "continue-all"
ABORT = "abort"

# The control directory is polled rather than watched: it is a bind mount shared with the
# host, where inotify is not dependable.
POLL_SEC = .5


def repo_root():
    """
    :return: Path of the Ignite repository root, derived from this module's own location
             (``<root>/modules/ducktests/tests/ignitetest/utils/pause_control.py``), so that a
             fork checked out elsewhere resolves its own root.
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), *[os.pardir] * 5))


def default_control_dir():
    """
    :return: Path of the control directory shared between the test and the host.
    """
    return os.path.join(repo_root(), CONTROL_DIR_NAME)


def continue_file(seq):
    """
    :return: Name of the file that resumes the breakpoint with the given sequence number.
    """
    return f"{CONTINUE_PREFIX}{seq}"


class ControlDir:
    """
    One directory, shared between a paused test and the host driving it.

    Every method here is mechanics. Nothing in this class knows what a breakpoint is, which
    is what lets the test, the console and the checks all speak the protocol through the same
    code rather than through three copies of it that have to agree.
    """
    def __init__(self, path):
        self._path = str(path)

    @property
    def path(self):
        """
        :return: Path of the directory itself, as both sides name it.
        """
        return self._path

    def file(self, name):
        """
        :return: Path of a control file, for reporting it to someone who will type it.
        """
        return os.path.join(self._path, name)

    def exists(self, name):
        """
        :return: Whether the control file is there.
        """
        return os.path.exists(self.file(name))

    def consume(self, name):
        """
        Removes a control file if it is present, returning whether it was there.

        The test is the only party that deletes what the host writes, so consuming
        a file is what acknowledges the host's command.

        :return: True if the file existed and was removed, False otherwise.
        """
        try:
            os.remove(self.file(name))
        except OSError:
            return False

        return True

    def remove(self, name):
        """
        Removes a control file, if it is still there - the other side may have just swept it.
        """
        try:
            os.remove(self.file(name))
        except OSError:
            pass

    def write(self, name, content):
        """
        Writes a control file whole: it lands through a temporary name, so the other side
        never reads a half written one.
        """
        path = self.file(name)
        tmp = path + ".tmp"

        with open(tmp, "w", encoding="utf-8") as file:
            file.write(content)

        os.replace(tmp, path)

    def prepare(self):
        """
        Creates the directory and clears anything an earlier run left in it: a stale resume
        file would skip the very first breakpoint of this one.
        """
        os.makedirs(self._path, exist_ok=True)

        self.sweep(status=True)

    def sweep(self, status=False):
        """
        Removes what an earlier run left behind.

        :param status: Whether to drop a published banner as well. The test does, at its first
               breakpoint. The console must not: it is just as likely to have been started
               against a test that is already holding one.
        """
        if not os.path.isdir(self._path):
            return

        # Matched by prefix rather than by name: an interrupted write leaves the temporary
        # ``<name>.tmp`` it goes through behind, and a sweep is the only thing that ever
        # visits this directory without knowing what it expects to find.
        for name in os.listdir(self._path):
            stale = name.startswith(CONTINUE_PREFIX) or name.startswith(ABORT)

            if status:
                stale = stale or name.startswith(STATUS_TXT) or name.startswith(STATUS_JSON)

            if stale:
                self.remove(name)

    def publish(self, banner, payload):
        """
        Publishes a held breakpoint, banner first as text and then as data, so that whoever
        polls for the data never finds it ahead of the text it describes.
        """
        self.write(STATUS_TXT, "\n".join(banner) + "\n")
        self.write(STATUS_JSON, json.dumps(payload, indent=2))

    def read_status(self):
        """
        :return: The published breakpoint, or None when nothing is published. A missing or
                 half written file simply reads as "not paused" and is retried by the caller.
        """
        try:
            with open(self.file(STATUS_JSON), encoding="utf-8") as file:
                return json.load(file)
        except (OSError, ValueError):
            return None

    def clear_status(self):
        """
        Withdraws the published breakpoint, so a stale banner never outlives the pause it
        describes.
        """
        for name in (STATUS_TXT, STATUS_JSON):
            self.remove(name)

    def resume(self, name):
        """
        Writes a resume file. The test consumes and removes it.

        Created outright rather than through :meth:`write`: a resume file is empty, so there
        is no half written state for an atomic replace to hide, and the temporary name that
        replace goes through would be one more transient to leave lying about.
        """
        with open(self.file(name), "w", encoding="utf-8"):
            pass

    def await_any(self, names, timeout_sec, tick=None, tick_sec=POLL_SEC):
        """
        Polls until one of the named files appears, and takes it.

        :param names: Names to watch, in priority order - the first one present wins when
               several land between two polls.
        :param tick: Called with the seconds left, every ``tick_sec`` that passes without a
               file. This is how a caller reports that it is still waiting without this class
               having to know what it would report to.
        :param tick_sec: How often to do that, by default as often as the directory is looked
               at - which is as often as there is anything new to say.
        :return: The name that ended the wait, None when the timeout ran out first.
        """
        deadline = time.monotonic() + timeout_sec
        next_tick = time.monotonic() + tick_sec if tick else None

        while True:
            for name in names:
                if self.consume(name):
                    return name

            now = time.monotonic()

            if now >= deadline:
                return None

            if next_tick is not None and now >= next_tick:
                next_tick = now + tick_sec

                tick(deadline - now)

            time.sleep(POLL_SEC)
