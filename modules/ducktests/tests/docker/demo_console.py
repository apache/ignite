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
Host side of the demo breakpoints - run it in a second terminal, next to the one running
``run_tests.sh``, when a test is started with the ``demo_pause`` global:

    ./docker/run_tests.sh -gj '{"demo_pause": "*"}' -t ./ignitetest/tests/<some_test.py>

    python docker/demo_console.py

Ducktape runs the test with stdin on /dev/null inside the ``ducker01`` container, so this is
where the keyboard lives. The console itself is deliberately dumb: the test renders the
banner and this only prints it and writes back a resume file. Everything it does can be done
by hand instead - ``cat .ducktests-demo/paused.txt``, then ``touch .ducktests-demo/continue-3``.

Standard library only: it runs on the host, outside the ducktests virtualenv.
"""

import argparse
import importlib.util
import os
import sys
import time

# Speak the protocol through the framework's own ControlDir rather than restating it here:
# the two sides of a shared directory have to agree file for file, and a second copy of it
# is a second thing to keep in step.
#
# The host has no ducktape and no installed ignitetest, so the module is loaded by path -
# importing ignitetest.utils.pause_control would pull in the package __init__ chain and its
# ducktape imports. pause_control itself is standard library only, which is what makes it
# loadable like this; ignitetest.utils.pause, which holds what the files mean, is not.
_TESTS_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
_PAUSE_CONTROL_PY = os.path.join(_TESTS_DIR, "ignitetest", "utils", "pause_control.py")

_SPEC = importlib.util.spec_from_file_location("ignitetest_pause_control", _PAUSE_CONTROL_PY)
pause = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pause)

POLL_SEC = .3

KEYS = """
  [Enter] continue        [c] continue, skipping the rest        [a] abort the test
  [q] leave the console (the test stays paused)
"""


def breakpoint_key(status):
    """
    :return: What identifies the published breakpoint. Not the sequence number on its own:
             that one is per test, so it restarts at 1 for every test of a session, and a
             run that died while paused leaves behind a banner numbered like a live one.
    """
    return status.get("run"), status.get("seq")


def clear_stale(control):
    """
    Removes resume files left behind by an earlier run, which would otherwise skip the first
    breakpoint of this one. The test clears them too, on its side, at its first breakpoint.

    Only ever done while nothing is published: a console is just as likely to be started
    against a test that is already holding a breakpoint, and a resume file that was written
    for that one - by hand, or by a console that has just been closed - is the host's answer
    to it rather than a leftover. The published breakpoint itself is left alone either way,
    a stale banner is told apart by its run id.

    :return: Whether the sweep was performed.
    """
    if control.read_status() is not None:
        return False

    control.sweep()

    return True


def prompt(control, seq):
    """
    Asks what to do with the breakpoint that is currently published.

    :return: False when the console should stop, True to wait for the next breakpoint.
    """
    while True:
        try:
            answer = input("  > ").strip().lower()
        except EOFError:
            return False

        if answer in ("", "n", "next"):
            control.resume(pause.continue_file(seq))

            return True

        if answer in ("c", "continue", "all"):
            control.resume(pause.CONTINUE_ALL)

            print("  continuing, remaining breakpoints skipped")

            return False

        if answer in ("a", "abort"):
            control.resume(pause.ABORT)

            print("  aborting the test")

            return False

        if answer in ("q", "quit", "exit"):
            print(f"  leaving the test paused, resume it with:\n"
                  f"    touch {control.file(pause.continue_file(seq))}")

            return False

        print(KEYS)


def main():
    """
    Waits for breakpoints and drives them until the test is resumed for good.
    """
    parser = argparse.ArgumentParser(description="Drives the ducktests demo breakpoints.")
    parser.add_argument("-d", "--control-dir", default=pause.default_control_dir(),
                        help="control directory shared with the test, defaults to "
                             f"<repository root>/{pause.CONTROL_DIR_NAME}")

    args = parser.parse_args()
    control = pause.ControlDir(args.control_dir)

    swept = clear_stale(control)

    print(f"Demo console, watching {control.path}")

    if swept:
        print("Waiting for the first breakpoint... (Ctrl-C to leave)")
    else:
        print("A breakpoint is already held, joining it as it is (Ctrl-C to leave)")

    last_key, resumed_at = None, None

    while True:
        status = control.read_status()

        if status is None:
            # The test removes its status files as it resumes, so this is also what tells the
            # console that the breakpoint it has just driven is over and the next one - which
            # may well repeat its number, in the next test of the session - is a new one.
            last_key = None

            time.sleep(POLL_SEC)

            continue

        if breakpoint_key(status) == last_key:
            time.sleep(POLL_SEC)

            continue

        last_key = breakpoint_key(status)

        print()

        if resumed_at is not None:
            print(f"  ({time.monotonic() - resumed_at:.0f}s since the previous breakpoint)")

        print("\n".join(status.get("banner", [])))
        print(KEYS)

        if not prompt(control, status.get("seq")):
            return

        resumed_at = time.monotonic()

        print("  resumed, waiting for the next breakpoint...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
