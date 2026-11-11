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
Checks reading the progress of the continuous load within a time window.
"""

import pytest

from ignitetest.services.utils.load_progress import LoadProgress, LoadWindow, load_progress, load_window


class FakeApp:
    """
    Load application service that recorded the given results.
    """
    def __init__(self, results):
        self.results = results

    def extract_results(self, name):
        """Returns the values recorded under the name."""
        return self.results.get(name, [])


# Successes every second, then none for about 4 seconds while 8 operations fail, then successes again.
PROGRESS = [
    LoadProgress(1000, 10, 0, 50),
    LoadProgress(2000, 20, 0, 40),
    LoadProgress(3000, 25, 2, 900),
    LoadProgress(4000, 25, 5, 1900),
    LoadProgress(5000, 25, 8, 2900),
    LoadProgress(6000, 35, 8, 30),
    LoadProgress(7000, 45, 8, 20),
]


class CheckLoadProgress:
    """
    Checks the load progress helpers.
    """
    @staticmethod
    def check_parses_records():
        """The records the application wrote become progress points, in order."""
        app = FakeApp({"Progress": ["1000 10 0 50", "2000 20 1 40"]})

        assert load_progress(app) == [LoadProgress(1000, 10, 0, 50), LoadProgress(2000, 20, 1, 40)]

    @staticmethod
    def check_counts_within_window():
        """Operations are counted from the last record before the window to the last record within it."""
        assert load_window(PROGRESS, 2500, 5500) == LoadWindow(ok=5, err=8, max_stall_ms=2900)

    @staticmethod
    def check_window_from_start():
        """A window opening before the first record counts from zero."""
        assert load_window(PROGRESS, 0, 2000) == LoadWindow(ok=20, err=0, max_stall_ms=50)

    @staticmethod
    def check_stall_before_window():
        """A stall that ended before the window does not count in it."""
        assert load_window(PROGRESS, 6500, 7000) == LoadWindow(ok=10, err=0, max_stall_ms=20)

    @staticmethod
    def check_empty_window():
        """A window without records fails rather than reports a load that did nothing."""
        with pytest.raises(AssertionError):
            load_window(PROGRESS, 7500, 8000)
