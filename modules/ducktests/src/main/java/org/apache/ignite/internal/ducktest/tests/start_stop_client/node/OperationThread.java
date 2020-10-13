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
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.internal.ducktest.tests.start_stop_client.node.reporter.Report;
import org.apache.log4j.Logger;

/**
 * Single action thread.
 */
public class OperationThread implements Runnable {
    /**  */
    private Action actionNode;

    /** */
    private Logger logger;

    /** */
    private long pacing;

    /** batch*/
    private static final long count = 1000;

    /** */
    public Action getActionNode() {
        return actionNode;
    }

    /** */
    public void setActionNode(ActionNode actionNode) {
        this.actionNode = actionNode;
        this.logger = logger;
    }

    /** */
    public OperationThread(Action actionNode, Logger logger, long pacing) {
        this.actionNode = actionNode;
        this.logger = logger;
        this.pacing = pacing;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        Report report;
        while (!Thread.currentThread().isInterrupted()) {
            report = calculateInternalReport();
            if (report != null) {
                actionNode.publishInternalReport(report);
            }
        }
    }

    /** */
    private Report calculateInternalReport() {
        long endTime = 0;
        long txCount = 0;
        long minLatency = -1;
        long maxLatency = -1;
        double avgLatency = 0;
        long percentile99 = 0;

        ArrayList<Long> reports = new ArrayList();

        long latency = 0;
        long batch = 0;
        long startTime = System.nanoTime();

        while (batch < (count)) {
            try {
                latency = actionNode.singleAction();
            } catch (Exception e) {
                logger.info("Fail put operation!");
                return null;
            }

            txCount++;

            if (minLatency == -1 || minLatency > latency) {
                minLatency = latency;
            }

            if (maxLatency < latency) {
                maxLatency = latency;
            }
            reports.add(latency);
            batch++;
        }
        endTime = System.nanoTime();

        long sum = 0;
        for (Long l : reports) {
            sum += l;
        }

        double sum_d = sum;

        if (!reports.isEmpty()) {
            avgLatency = sum_d / reports.size();
            Collections.sort(reports);

            percentile99 = reports.get((int) (reports.size() * 0.99));
        }

        //calc dispersion
        double x = 0;
        for (Long report : reports)
            x += Math.pow(report - avgLatency, 2);

        double variance = 0;
        if (reports.size() > 1)
            variance = x / (reports.size() - 1);

        Report report = new Report();
        report.setAvgLatency(Math.round(avgLatency));
        report.setStartTime(startTime);
        report.setEndTime(endTime);
        report.setMinLatency(minLatency);
        report.setMaxLatency(maxLatency);
        report.setTxCount(txCount);
        report.setThreadName(Thread.currentThread().getName());
        report.setPercentile99(percentile99);
        report.setVariance(Math.round(variance));

        try {
            TimeUnit.MILLISECONDS.sleep(pacing);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return report;
    }
}
