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
        while (!Thread.interrupted()) {
            try {
                actionNode.publishInterimReport(calculateInterimStatement());
                Thread.sleep(pacing);
            } catch (InterruptedException e) {
            }
        }
    }

    /** Building an interim report. */
    private Report calculateInterimStatement() {
        long endTime = 0;
        long txCount = 0;
        long minLatency = -1;
        long maxLatency = -1;
        long avgLatency = 0;
        long percentile99 = 0;

        ArrayList<Long> reports = new ArrayList();

        long latency = 0;
        long batch = 0;
        long startTime = System.currentTimeMillis();

        try {
            while (batch < (count)) {
                latency = actionNode.singleAction();
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
            endTime = System.currentTimeMillis();

            long sum = 0;
            for (Long l : reports) {
                sum += l;
            }
            if (!reports.isEmpty()) {
                avgLatency = sum / reports.size();
                Collections.sort(reports);

                percentile99 = reports.get((int) (reports.size() * 0.99));
            }
        }
        finally {
            //calc dispersion
            long x = 0;
            for (Long report : reports) {
                x = x + (report - avgLatency) ^ (2);
            }
            logger.info("dispersion debug: " + x);

            double dispersion = (double) (x / (reports.size() - 1));

            Report report = new Report();
            report.setAvgLatency(avgLatency);
            report.setStartTime(startTime);
            report.setEndTime(endTime);
            report.setMinLatency(minLatency);
            report.setMaxLatency(maxLatency);
            report.setTxCount(txCount);
            report.setThreadName(Thread.currentThread().getName());
            report.setPercentile99(percentile99);
            report.setDispersion(dispersion);

            return report;
        }
    }
}
