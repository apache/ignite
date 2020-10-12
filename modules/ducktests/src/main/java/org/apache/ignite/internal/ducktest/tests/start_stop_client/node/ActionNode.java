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

package org.apache.ignite.internal.ducktest.tests.start_stop_client.node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * The base class agent
 */
public abstract class ActionNode extends IgniteAwareApplication implements Action {
    /** Report Queue. */
    protected ConcurrentLinkedQueue queue = new ConcurrentLinkedQueue();

    /** The unique name of the agent. */
    private String nodeId = UUID.randomUUID().toString();

    /** Start test time. */
    protected Date startTime;

    /** End test time. */
    protected Date endTime;

    /** Thread count. */
    private int threads;

    /** The unique name of the agent. */
    private final String LOG_TX_REPORT = "Report from thread=%s action=%s \n" +
            "<st_time>%d</st_time> \n" +
            "<end_time>%d</end_time>\n" +
            "<total_tx>%d</total_tx>\n" +
            "<min_latency>%d</min_latency>\n" +
            "<max_latency>%d</max_latency>\n" +
            "<avg_latency>%d</avg_latency>\n" +
            "<percentile99>%d</percentile99>\n" +
            "<dispersion>%.2f</dispersion>\n";

    /** Title of the final report. */
    private final String FINAL_REPORT_HEADER = "\n" +
            "<start_time>end_time</start_time>" +
            "<end_t>end_time</end_t>" +
            "<tx_c>tx_count</tx_c>" +
            "<min_l>min_latency</min_l>" +
            "<avg_l>avg_latency</avg_l>" +
            "<max_l>max_latency</max_l>" +
            "<percentile>percentile99</percentile>" +
            "<disp>dispersion</disp>" +
            "\n";

    /** Template for the final report line. */
    private final String FINAL_REPORT_TEMPLATE = "" +
            "<start_time>%d</start_time>" +
            "<end_t>%d</end_t>" +
            "<tx_c>%d</tx_c>" +
            "<min_l>%d</min_l>" +
            "<avg_l>%d</avg_l>" +
            "<max_l>%d</max_l>" +
            "<percentile>%d</percentile>" +
            "<disp>%.2f</disp>";

    /** The delay between iterations of the action. */
    protected long pacing;

    /** Current action name. */
    protected String actionName;

    /** Thread executer. */
    protected ExecutorService executor;

    /** Threads. */
    private ArrayList<OperationThread> operationThreads = new ArrayList();

    /**  */
    private Logger log = LogManager.getLogger(ActionNode.class.getName());

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        init(jsonNode);

        markInitialized();

        for (int i = 0; i < threads; i++) {
            operationThreads.add(new OperationThread(this, log, pacing));
        }
        for (OperationThread thread : operationThreads) {
            executor.execute(thread);
        }

        while (!terminated()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        executor.shutdownNow();
        if (!executor.isShutdown()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        endTime = new Date();
        calculateFinalReport();
        markFinished();
    }

    /** */
    protected void init(JsonNode jsonNode) {
        threads = Optional.ofNullable(jsonNode.get("threads")).map(JsonNode::asInt).orElse(1);
        pacing = Optional.ofNullable(jsonNode.get("pacing")).map(JsonNode::asLong).orElse(0l);
        actionName = Optional.ofNullable(jsonNode.get("action")).map(JsonNode::asText).orElse("default-action-name");
        startTime = new Date();

        log.info(
                "test props:" +
                " action=" + actionName +
                " pacing=" + pacing +
                " threads=" + threads
        );

        executor = Executors.newFixedThreadPool(threads + 2);
        scriptInit(jsonNode);
    }

    /** Init method. */
    protected abstract void scriptInit(JsonNode jsonNode);

    /** {@inheritDoc} */
    @Override public void publishInterimReport(Report report) {
        if (!executor.isShutdown()) {
            executor.execute(new Runnable() {
                @Override public void run() {
                    queue.add(report);
                    printReportIntoLog(report);
                }
            });
        }
    }

    /** */
    private void printReportIntoLog(Report report) {
        String message = String.format(LOG_TX_REPORT,
                report.getThreadName(),
                this.actionName,
                report.getStartTime(),
                report.getEndTime(),
                report.getTxCount(),
                report.getMinLatency(),
                report.getMaxLatency(),
                report.getAvgLatency(),
                report.getPercentile99(),
                report.getDispersion()
        );
        log.info(message);
    }

    /** Publishing the final report in the log.  */
    private void calculateFinalReport() {
        ArrayList<Report> reports = new ArrayList(Arrays.asList(queue.stream().toArray()));

        StringBuilder builder = new StringBuilder();
        builder.append("\n<report start>\n");
        builder.append("<meansured agent-name>" + this.nodeId + "</meansured agent-name>" + "\n");
        builder.append("<action name>" + actionName + "</action name>" + "\n");
        builder.append("<thread count>" + threads + "<thread count>" + "\n");
        builder.append("<active baseline>" + ignite.cluster().currentBaselineTopology().size() + "</active baseline>" + '\n');
        builder.append("<start agent time>" + startTime.toString() + "</start agent time>" + '\n');
        builder.append("<end agent work time>" + endTime.toString() + "</end agent work time>" + '\n');
        builder.append("<total work>" + ((endTime.getTime() - startTime.getTime()) / (1000)) + "</total work>");
        builder.append(FINAL_REPORT_HEADER);
        builder.append("<data>\n");

        for (int i = 0; i < reports.size() - 1; i++) {
            Report report = reports.get(i);
            builder.append(String.format(FINAL_REPORT_TEMPLATE,
                    report.getStartTime(),
                    report.getEndTime(),
                    report.getTxCount(),
                    report.getMinLatency(),
                    report.getAvgLatency(),
                    report.getMaxLatency(),
                    report.getPercentile99(),
                    report.getDispersion()
            ));
            builder.append("\n");
        }
        builder.append("</data>\n");
        builder.append("<report end>\n");

        log.info(builder.toString());
    }
}
