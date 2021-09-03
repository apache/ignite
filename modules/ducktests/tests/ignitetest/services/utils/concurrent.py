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
This module contains concurrent utils.
"""

import threading


class CountDownLatch:
    """
    A count-down latch.
    """
    def __init__(self, count=1):
        self.count = count
        self.cond_var = threading.Condition()

    def count_down(self):
        """
        Decreases the latch counter.
        """
        with self.cond_var:
            if self.count > 0:
                self.count -= 1
            if self.count == 0:
                self.cond_var.notifyAll()

    def wait(self):
        """
        Blocks current thread if the latch is not free.
        """
        with self.cond_var:
            while self.count > 0:
                self.cond_var.wait()


class AtomicValue:
    """
    An atomic reference holder.
    """
    def __init__(self, value=None):
        self.value = value
        self.lock = threading.Lock()

    def set(self, value):
        """
        Sets new value to hold.
        :param value: New value to hold.
        """
        with self.lock:
            self.value = value

    def get(self):
        """
        Gives current value.
        """
        with self.lock:
            return self.value

    def compare_and_set(self, expected, value):
        """
        Sets new value to hold if current one equals expected.
        :param expected: The value to compare with.
        :param value: New value to hold.
        """
        return self.check_and_set(lambda: self.value == expected, value)

    def check_and_set(self, condition, value):
        """
        Sets new value to hold by condition.
        :param condition: The condition to check.
        :param value: New value to hold.
        """
        with self.lock:
            if condition():
                self.value = value
            return self.value
