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

import re


class Report:
    start_time = -1
    end_time = -1
    tx_count = -1
    min_latency = -1
    avg_latency = -1
    max_latency = -1
    percentile99 = -1

    TIME_END_REG = r'<end_t>\d*<'
    TX_REG = r'<tx_c>\d*<'
    MIN_L_REG = r'<min_l>\d*<'
    AVG_L_REG = r'<avg_l>\d*<'
    MAX_L_REG = r'<max_l>\d*<'
    P99_REG = r'<percentile>\d*<'

    # pylint: disable=R0913
    def __init__(self, string):
        self.end_time = self.grep(string, self.TIME_END_REG)
        self.tx_count = self.grep(string, self.TX_REG)
        self.min_latency = self.grep(string, self.MIN_L_REG)
        self.avg_latency = self.grep(string, self.AVG_L_REG)
        self.max_latency = self.grep(string, self.MIN_L_REG)
        self.percentile99 = self.grep(string, self.P99_REG)

    @staticmethod
    def grep(string, reg):
        """
        grep by regular expg
        :param reg: reg exp.
        :param string: search string
        """
        temp = re.search(reg, string)
        target = re.search(r'[0-9]+', temp.group(0))
        return target.group(0)

    def get_report_array(self, index):
        """
        :param index: serial number
        """
        array = [
            index,
            self.end_time,
            self.tx_count,
            self.min_latency,
            self.avg_latency,
            self.max_latency,
            self.percentile99
        ]
        return array
