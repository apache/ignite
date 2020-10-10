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

/**
 * Java client. Tx put operation
 */

public class Report {

    /** */
    private String threadName;

    /** measurement start time */
    private long st_time;

    /** the time of completion of measurement */
    private long end_time;

    /** transaction count */
    private long tx_count;

    /** the minimum duration of the operation */
    private long min_latency;

    /** the maximum duration of the operation */
    private long max_latency;

    /** the average duration of the operations */
    private long avg_latency;

    /** percentile 99 % */
    private long percentile99;

    /** measurement variance */
    private double dispersion;

    /** */
    public Report(){

    }

    /** */
    public double getDispersion() {
        return dispersion;
    }

    /** */
    public void setDispersion(double dispersion) {
        this.dispersion = dispersion;
    }

    /** */
    public long getPercentile99() {
        return percentile99;
    }

    /** */
    public void setPercentile99(long percentile99) {
        this.percentile99 = percentile99;
    }

    /** */
    public String getThreadName() {
        return threadName;
    }

    /** */
    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    /** */
    public long getSt_time() {
        return st_time;
    }

    /** */
    public void setSt_time(long st_time) {
        this.st_time = st_time;
    }

    /** */
    public long getEnd_time() {
        return end_time;
    }

    /** */
    public void setEnd_time(long end_time) {
        this.end_time = end_time;
    }

    /** */
    public long getTx_count() {
        return tx_count;
    }

    /** */
    public void setTx_count(long tx_count) {
        this.tx_count = tx_count;
    }

    /** */
    public long getMin_latency() {
        return min_latency;
    }

    /** */
    public void setMin_latency(long min_latency) {
        this.min_latency = min_latency;
    }

    /** */
    public long getMax_latency() {
        return max_latency;
    }

    /** */
    public void setMax_latency(long max_latency) {
        this.max_latency = max_latency;
    }

    /** */
    public long getAvg_latency() {
        return avg_latency;
    }

    /** */
    public void setAvg_latency(long avg_latency) {
        this.avg_latency = avg_latency;
    }
}
