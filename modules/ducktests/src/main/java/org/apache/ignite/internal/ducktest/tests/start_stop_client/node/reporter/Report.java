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

/**
 * Java client. Tx put operation
 */
public class Report {
    /** */
    private String threadName = "default";

    /** Measurement start time. */
    private long startTime = -1;

    /** The time of completion of measurement. */
    private long endTime = -1;

    /** Transaction count. */
    private long txCount = -1;

    /** The minimum duration of the operation. */
    private long minLatency = -1;

    /** The maximum duration of the operation. */
    private long maxLatency = -1;

    /** The average duration of the operations. */
    private long avgLatency = -1;

    /** Percentile 99 %. */
    private long percentile99 = -1;

    /** Measurement variance. */
    private double dispersion = -1;

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
    public long getStartTime() {
        return this.startTime;
    }

    /** */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /** */
    public long getEndTime() {
        return this.endTime;
    }

    /** */
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    /** */
    public long getTxCount() {
        return txCount;
    }

    /** */
    public void setTxCount(long txCount) {
        this.txCount = txCount;
    }

    /** */
    public long getMinLatency() {
        return this.minLatency;
    }

    /** */
    public void setMinLatency(long minLatency) {
        this.minLatency = minLatency;
    }

    /** */
    public long getMaxLatency() {
        return this.maxLatency;
    }

    /** */
    public void setMaxLatency(long maxLatency) {
        this.maxLatency = maxLatency;
    }

    /** */
    public long getAvgLatency() {
        return avgLatency;
    }

    /** */
    public void setAvgLatency(long avgLatency) {
        this.avgLatency = avgLatency;
    }
}
