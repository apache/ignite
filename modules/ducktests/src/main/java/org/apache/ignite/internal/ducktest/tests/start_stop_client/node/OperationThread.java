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

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Collections;

/**
 * single action thread
 */

public class OperationThread extends Thread {

    /**  */
    private Action actionNode;

    /**  */
    private boolean terminated = false;

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
    public boolean getTerminated() {
        return terminated;
    }

    /** */
    public void terminate() {
        this.terminated = true;
    }

    /** */
    public OperationThread(Action actionNode, Logger logger, long pacing, String name) {
        this.actionNode = actionNode;
        this.logger = logger;
        this.pacing = pacing;
        this.setName(name);
    }

    /** {@inheritDoc} */
    @Override public void run() {
        logger.info("run single thread name: " + this.getName());

        Report temp;
        while (!terminated) {
            actionNode.publishInterimReport(calculate_interim_statement());
            try {
                Thread.sleep(pacing);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("single thread name: " + this.getName() + " terminated");
    }

    /** building an interim report*/
    private Report calculate_interim_statement() {
        long st_time = System.nanoTime();
        long end_time;
        long tx_count = 0;
        long min_latency = -1;
        long max_latency = -1;
        long avg_latency = 0;
        long percentile99 = 0;
        String thread = this.getName();

        ArrayList<Long> reports = new ArrayList();

        long latency = 0;

        long batch = 0;
        long st_time_r = System.currentTimeMillis();
        while (!terminated && batch < (count)) {
            latency = actionNode.singleAction();
            tx_count++;

            if (min_latency == -1) {
                min_latency = latency;
            } else {
                if (min_latency > latency){
                    min_latency = latency;
                }
            }

            if (max_latency < latency) {
                max_latency = latency;
            }
            reports.add(latency);
            batch++;
        }
        end_time = System.currentTimeMillis();

        long sum=0;
        for (Long s_latency : reports){
            sum += s_latency;
        }
        if (!reports.isEmpty()) {
            avg_latency = sum / reports.size();
            Collections.sort(reports);
            percentile99 = percentile99(reports);
        }

        //calc dispersion
        long x = 0;
        for (Long report : reports) {
            x = x + (report - avg_latency) ^ (2);
        }
        logger.info("dispersion debug: " + x);
        double dispersion = (double) (x / (reports.size() - 1));

        Report report = new Report();

        report.setAvg_latency(avg_latency);
        report.setSt_time(st_time_r);
        report.setEnd_time(end_time);
        report.setMin_latency(min_latency);
        report.setMax_latency(max_latency);
        report.setTx_count(tx_count);
        report.setThreadName(thread);
        report.setPercentile99(percentile99);
        report.setDispersion(dispersion);

        return report;
    }

    /** percentile 99 calculate*/
    private long percentile99(ArrayList<Long> reports) {
        int size = reports.size();
        int index = (int) (size * 0.99);
        return reports.get(index);
    }

}
