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
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
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
    /** The unique name of the agent. */
    private String nodeId = UUID.randomUUID().toString();

    /** Start test time. */
    protected Date startTime;

    /** End test time. */
    protected Date endTime;

    /** Thread count. */
    private int threads;

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
                    printReportIntoLog(report);
                }
            });
        }
    }

    /** */
    private void printReportIntoLog(Report report) {
        StringBuilder builder = new StringBuilder();
        builder.append("\n");
        builder.append("<data>\n");
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
        builder.append("</data>\n");
        log.info(builder.toString());
    }
}
