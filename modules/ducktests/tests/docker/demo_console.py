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
import json
import os
import sys
import time

# Reach into the framework for the protocol constants rather than restating them. The host
# has no ducktape and no installed ignitetest, so the module is loaded by path: importing
# ignitetest.utils.pause would pull in the package __init__ chain and its ducktape imports.
_TESTS_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
_PAUSE_PY = os.path.join(_TESTS_DIR, "ignitetest", "utils", "pause.py")

_SPEC = importlib.util.spec_from_file_location("ignitetest_pause", _PAUSE_PY)
pause = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pause)

POLL_SEC = .3

KEYS = """
  [Enter] continue        [c] continue, skipping the rest        [a] abort the test
  [q] leave the console (the test stays paused)
"""


def read_status(control_dir):
    """
    :return: The published breakpoint, or None when the scenario is not paused. A missing or
             half written file simply reads as "not paused" and is retried.
    """
    path = os.path.join(control_dir, pause.STATUS_JSON)

    try:
        with open(path, encoding="utf-8") as file:
            return json.load(file)
    except (OSError, ValueError):
        return None


def clear_stale(control_dir):
    """
    Removes resume files left behind by an earlier run, which would otherwise skip the first
    breakpoint of this one. The test clears them too, on its side, at its first breakpoint.
    """
    if not os.path.isdir(control_dir):
        return

    for name in os.listdir(control_dir):
        if name.startswith(pause.CONTINUE_PREFIX) or name == pause.ABORT:
            try:
                os.remove(os.path.join(control_dir, name))
            except OSError:
                pass


def resume(control_dir, name):
    """
    Writes a resume file. The test consumes and removes it.
    """
    with open(os.path.join(control_dir, name), "w", encoding="utf-8") as file:
        file.write("")


def prompt(control_dir, seq):
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
            resume(control_dir, pause.continue_file(seq))

            return True

        if answer in ("c", "continue", "all"):
            resume(control_dir, pause.CONTINUE_ALL)

            print("  continuing, remaining breakpoints skipped")

            return False

        if answer in ("a", "abort"):
            resume(control_dir, pause.ABORT)

            print("  aborting the test")

            return False

        if answer in ("q", "quit", "exit"):
            print(f"  leaving the test paused, resume it with:\n"
                  f"    touch {os.path.join(control_dir, pause.continue_file(seq))}")

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
    control_dir = args.control_dir

    clear_stale(control_dir)

    print(f"Demo console, watching {control_dir}")
    print("Waiting for the first breakpoint... (Ctrl-C to leave)")

    last_seq, resumed_at = 0, None

    while True:
        status = read_status(control_dir)

        if status is None or status.get("seq") == last_seq:
            time.sleep(POLL_SEC)

            continue

        last_seq = status.get("seq")

        print()

        if resumed_at is not None:
            print(f"  ({time.monotonic() - resumed_at:.0f}s since the previous breakpoint)")

        print("\n".join(status.get("banner", [])))
        print(KEYS)

        if not prompt(control_dir, last_seq):
            return

        resumed_at = time.monotonic()

        print("  resumed, waiting for the next breakpoint...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
