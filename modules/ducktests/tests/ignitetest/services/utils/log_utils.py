#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
This module contains log utils.
"""

from contextlib import contextmanager

from ducktape.cluster.remoteaccount import LogMonitor


class IgniteLogMonitor(LogMonitor):
    """
    Extends ducktape's LogMonitor with a one-shot presence check.
    """
    def found(self, pattern):
        """
        Check once whether the pattern is present in the log after the initial
        offset recorded when the monitor was created.
        """
        return self.acct.ssh("tail -c +%d %s | grep '%s'" % (self.offset + 1, self.log, pattern), allow_fail=True) == 0


@contextmanager
def monitor_log(node, log, from_the_beginning=False):
    """
    Context manager that returns an object that helps you wait for events to
    occur in a log. This checks the size of the log at the beginning of the
    block and makes a helper object available with convenience methods for
    checking or waiting for a pattern to appear in the log. This will commonly
    be used to start a process, then wait for a log message indicating the
    process is in a ready state.

    See ``LogMonitor`` for more usage information.
    """
    try:
        offset = 0 if from_the_beginning else int(node.account.ssh_output("wc -c %s" % log).split()[0])
    except Exception:
        offset = 0
    yield IgniteLogMonitor(node.account, log, offset)
