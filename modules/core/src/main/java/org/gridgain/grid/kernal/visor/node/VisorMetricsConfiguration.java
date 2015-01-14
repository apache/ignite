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

package org.gridgain.grid.kernal.visor.node;

import org.apache.ignite.configuration.*;

import java.io.*;

/**
 * Data transfer object for node metrics configuration properties.
 */
public class VisorMetricsConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Metrics expired time. */
    private long expTime;

    /** Number of node metrics stored in memory. */
    private int historySize;

    /** Frequency of metrics log printout. */
    private long logFreq;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node metrics configuration properties.
     */
    public static VisorMetricsConfiguration from(IgniteConfiguration c) {
        VisorMetricsConfiguration cfg = new VisorMetricsConfiguration();

        cfg.expireTime(c.getMetricsExpireTime());
        cfg.historySize(c.getMetricsHistorySize());
        cfg.loggerFrequency(c.getMetricsLogFrequency());

        return cfg;
    }

    /**
     * @return Metrics expired time.
     */
    public long expireTime() {
        return expTime;
    }

    /**
     * @param expTime New metrics expire time.
     */
    public void expireTime(long expTime) {
        this.expTime = expTime;
    }

    /**
     * @return Number of node metrics stored in memory.
     */
    public int historySize() {
        return historySize;
    }

    /**
     * @param historySize New number of node metrics stored in memory.
     */
    public void historySize(int historySize) {
        this.historySize = historySize;
    }

    /**
     * @return Frequency of metrics log printout.
     */
    public long loggerFrequency() {
        return logFreq;
    }

    /**
     * @param logFreq New frequency of metrics log printout.
     */
    public void loggerFrequency(long logFreq) {
        this.logFreq = logFreq;
    }
}
