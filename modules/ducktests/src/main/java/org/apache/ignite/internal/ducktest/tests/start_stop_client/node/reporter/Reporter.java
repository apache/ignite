/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.ducktest.tests.start_stop_client.node.reporter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * report writer thread
 */
public class Reporter implements Runnable {
    /**
     * Queue with data for report.
     * */
    private ConcurrentLinkedDeque<Report> queue;

    /**
     * Target report file path.
     * */
    private String reportFile;

    /** */
    private String supportInfoTemplate = "Current action name: %s \n" +
            "Number of threads in the test: %d \n" +
            "Start time: %d \n";

    /** */
    private FileWriter fileWriter;

    /**
     * Title for csv report.
     * */
    private String reportHeader = "start_time," +
            "end_time," +
            "tx_count," +
            "min_latency," +
            "avg_latency," +
            "max_latency," +
            "percentile99%," +
            "dispersion";

    /**
     * Template for the final report line.
     * */
    private String reportTemplate = "" +
            "%d," +
            "%d," +
            "%d," +
            "%d," +
            "%.2f," +
            "%d," +
            "%d," +
            "%.2f";

    /** */
    public Reporter(String reportFile, ConcurrentLinkedDeque concurrentLinkedDeque, Metadata metadata) throws IOException {
        this.reportFile = reportFile;
        this.queue = concurrentLinkedDeque;
        this.fileWriter = new FileWriter(reportFile, true);
        this.fileWriter.write(supportInfoBuilder(metadata));
        this.fileWriter.write(this.reportHeader);
        this.fileWriter.flush();
    }

    /** */
    @Override public void run() {
        while (!Thread.interrupted()) {
            try {
                writeAction();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** */
    private void writeAction() throws IOException {
        try {

        } finally {
            if (!queue.isEmpty()) {
                String singleReport = buildReportString(queue.pollFirst());
                fileWriter.write(singleReport);
                fileWriter.flush();
            }
        }
    }

    /**
     * converting a report to a string for recording using the specified template
     * */
    private String buildReportString(Report report) {
        StringBuilder builder = new StringBuilder();
        builder.append("\n");
        builder.append(String.format(this.reportTemplate,
                report.getStartTime(),
                report.getEndTime(),
                report.getTxCount(),
                report.getMinLatency(),
                report.getAvgLatency(),
                report.getMaxLatency(),
                report.getPercentile99(),
                report.getDispersion()
        ));
        return builder.toString();
    }

    /** */
    private String supportInfoBuilder(Metadata metadata) {
        return String.format(this.supportInfoTemplate, metadata.getActionName(),
                metadata.getThreads(),
                metadata.getStartTime());
    }

    /* **/
    public void setReportHeader(String header) {
        this.reportHeader = header;
    }

    /* **/
    public void setReportTemplate(String template) {
        this.reportTemplate = reportTemplate;
    }

    /* **/
    public String getSupportInfoTemplate() {
        return supportInfoTemplate;
    }

    /* **/
    public void setSupportInfoTemplate(String supportInfoTemplate) {
        this.supportInfoTemplate = supportInfoTemplate;
    }
}
