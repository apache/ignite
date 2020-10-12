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

import os

import jinja2

from ignitetest.report.plotter.plotter import Plotter
from ignitetest.report.plotter.report import Report


class ReportBuilder:

    LOG_FILE = "../log/console.log"
    REPORT_FOLDER = "result"
    REPORT_NAME = "report.html"
    TEMPLATE_FILE = "../report/report.j2"

    START_REPORT_MARKER = "<report start>\n"
    DATA_START_MARKER = "<data>\n"
    DATA_STOP_MARKER = "</data>\n"
    END_REPORT_MARKER = "<report end>\n"

    data_s_marker = 0
    data_end_marker = 0

    data_s_not_trigger = True
    final_report_data = []

    # pylint: disable=R0913
    def __init__(self, log, report_folder_name):
        self.REPORT_FOLDER = report_folder_name
        self.LOG_FILE = log

    # pylint: disable=R0913
    def extract_data_from_log(self):
        f = open(self.LOG_FILE)
        report_data = []

        index = 0
        for line in f:
            if self.data_s_not_trigger:
                if line == self.DATA_START_MARKER:
                    self.data_s_marker = index
                    self.data_s_not_trigger = False
            else:
                if line == self.DATA_STOP_MARKER:
                    self.data_end_marker = index
                    self.data_s_not_trigger = True
                else:
                    report_data.append(line)

            index = index + 1
        f.close()

        for report in report_data:
            item = Report(report)
            self.final_report_data.append(item)

    def plot_graph(self):
        """
        build image
        """
        plotter = Plotter(self.final_report_data)
        plotter.plot_avg_percentile(self.REPORT_FOLDER + "/" + "latency_plot")

    def create_report_folder(self):
        """
        create target folder
        """
        os.mkdir(self.REPORT_FOLDER)

    def build_html_report(self):
        """
        build html report
        """
        self.extract_data_from_log()
        self.create_report_folder()
        self.plot_graph()
        relations_table = []

        i = 0

        for report in self.final_report_data:
            i = i + 1
            relations_table.append(report.get_report_array(i))

        template_loader = jinja2.FileSystemLoader(searchpath="report")
        template_env = jinja2.Environment(loader=template_loader)

        self.TEMPLATE_FILE = "report.j2"
        template = template_env.get_template(self.TEMPLATE_FILE)

        output_text = template.render(
            relations_table=relations_table)  # this is where to put args to the template renderer

        f = open(self.REPORT_FOLDER + '/report.html', 'w')
        f.write(output_text)
        f.close()
