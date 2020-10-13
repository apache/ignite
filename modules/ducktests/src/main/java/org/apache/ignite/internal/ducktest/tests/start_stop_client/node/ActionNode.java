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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.tests.start_stop_client.node.reporter.Metadata;
import org.apache.ignite.internal.ducktest.tests.start_stop_client.node.reporter.Report;
import org.apache.ignite.internal.ducktest.tests.start_stop_client.node.reporter.Reporter;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * The base class agent
 */
public abstract class ActionNode extends IgniteAwareApplication implements Action {
    /** The unique name of the agent. */
    private String nodeId = UUID.randomUUID().toString();

    /**
     * Queue with data for report.
     * */
    private ConcurrentLinkedDeque<Report> queue = new ConcurrentLinkedDeque<Report>();

    /* **/
    Reporter reporter;

    /* **/
    Metadata testInfo = new Metadata();

    /** Start test time. */
    protected Date startTime;

    /** End test time. */
    protected Date endTime;

    /** Thread count. */
    private int threads;

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
    protected void init(JsonNode jsonNode) throws IOException {
        threads = Optional.ofNullable(jsonNode.get("threads")).map(JsonNode::asInt).orElse(1);
        pacing = Optional.ofNullable(jsonNode.get("pacing")).map(JsonNode::asLong).orElse(0l);
        actionName = Optional.ofNullable(jsonNode.get("action")).map(JsonNode::asText).orElse("default-action-name");
        String reportFolder = jsonNode.get("report_folder").asText();

        startTime = new Date();

        log.info(
                "test props:" +
                " action=" + actionName +
                " pacing=" + pacing +
                " threads=" + threads
        );

        testInfo.setActionName(actionName);
        testInfo.setThreads(threads);
        testInfo.setStartTime(System.currentTimeMillis());

        File folder = new File(reportFolder);
        if (!folder.exists()) {
            folder.mkdir();
        }

        String reportPath = reportFolder + "/report-" + System.currentTimeMillis() + ".csv";
        reporter = new Reporter(reportPath, queue, testInfo);

        executor = Executors.newFixedThreadPool(threads + 3);
        executor.execute(reporter);
        scriptInit(jsonNode);
    }

    /** Init method. */
    protected abstract void scriptInit(JsonNode jsonNode);

    /** {@inheritDoc} */
    @Override public void publishInternalReport(Report report) {
        if (!executor.isShutdown()) {
            executor.execute(new Runnable() {
                @Override public void run() {
                    queue.push(report);
                }
            });
        }
    }
}
