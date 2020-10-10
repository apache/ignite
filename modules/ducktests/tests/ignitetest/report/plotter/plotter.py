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
class for plotting operation execution time based on launch logs
"""

import matplotlib.pyplot as plt


class Plotter:
    REPORTS = []

    # pylint: disable=R0913
    def __init__(self, data):
        self.REPORTS = data

    def plot_avg_latency_data(self, name):
        """
        Generate graph
        :param name: image name.
        """
        axis_x_res = self.time_axios_prepare()
        axis_y = []

        for report in self.REPORTS:
            y = int(int(report.avg_latency))
            axis_y.append(y)

        plt.figure(figsize=(20, 20))
        plt.title("average latency (ns)")
        plt.xlabel("time (s)")
        plt.ylabel("latency (ns)")
        plt.plot(axis_x_res, axis_y)
        plt.savefig(str(name) + ".png")

    def plot_avg_percentile(self, name):
        """
        Generate graph
        :param name: image name.
        """
        axis_x_res = self.time_axios_prepare()
        axis_avg = []
        axios_percentile = []

        for report in self.REPORTS:
            axios_percentile.append(int(int(report.percentile99)))
            axis_avg.append(int(int(report.avg_latency)))

        plt.figure(figsize=(20, 10))
        plt.title("percentile 99% and avg_latency (ns)")
        plt.xlabel("time (s)")
        plt.ylabel("latency (ns)")
        plt.plot(axis_x_res, axis_avg, label='average latency')
        plt.plot(axis_x_res, axios_percentile, label='percentile 99%')
        plt.legend()
        plt.savefig(str(name) + ".png")

    def plot_percentile_latency_data(self, name):
        """
        Generate graph
        :param name: image name.
        """
        axis_x_res = self.time_axios_prepare()
        axis_y = []

        for report in self.REPORTS:
            y = int(int(report.percentile99))
            axis_y.append(y)

        plt.figure(figsize=(20, 20))
        plt.title("percentile 99% latency (ns)")
        plt.xlabel("time (s)")
        plt.ylabel("latency (ns)")
        plt.plot(axis_x_res, axis_y)

        plt.savefig(str(name) + ".png")

    def time_axios_prepare(self):
        """
        prepare array for plot graph
        """
        axis_x = []
        axis_x_res = []
        min_l = -1
        for report in self.REPORTS:
            x = int(int(report.end_time))
            if min_l == -1:
                min_l = x
            if min_l > x:
                min_l = x
            axis_x.append(x)
        i = 0
        while i < len(axis_x):
            temp = (axis_x[i] - min_l) / 1000
            axis_x_res.append(temp)
            i = i + 1

        return axis_x_res
