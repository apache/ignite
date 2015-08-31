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

package org.apache.ignite.internal.visor.node;

import java.io.Serializable;

/**
 * Data collector task arguments.
 */
public class VisorNodeDataCollectorTaskArg implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether task monitoring should be enabled. */
    private boolean taskMonitoringEnabled;

    /** Visor unique key to get last event order from node local storage. */
    private String evtOrderKey;

    /** Visor unique key to get lost events throttle counter from node local storage. */
    private String evtThrottleCntrKey;

    /** Cache sample size. */
    private int sample;

    /** If {@code true} then collect information about system caches. */
    private boolean sysCaches;

    /**
     * Default constructor.
     */
    public VisorNodeDataCollectorTaskArg() {
        // No-op.
    }

    /**
     * Create task arguments with given parameters.
     *
     * @param taskMonitoringEnabled If {@code true} then Visor should collect information about tasks.
     * @param evtOrderKey Event order key, unique for Visor instance.
     * @param evtThrottleCntrKey Event throttle counter key, unique for Visor instance.
     * @param sample How many entries use in sampling.
     * @param sysCaches If {@code true} then collect information about system caches.
     */
    public VisorNodeDataCollectorTaskArg(
        boolean taskMonitoringEnabled,
        String evtOrderKey,
        String evtThrottleCntrKey,
        int sample,
        boolean sysCaches
    ) {
        this.taskMonitoringEnabled = taskMonitoringEnabled;
        this.evtOrderKey = evtOrderKey;
        this.evtThrottleCntrKey = evtThrottleCntrKey;
        this.sample = sample;
        this.sysCaches = sysCaches;
    }

    /**
     * @return {@code true} if Visor should collect information about tasks.
     */
    public boolean taskMonitoringEnabled() {
        return taskMonitoringEnabled;
    }

    /**
     * @param taskMonitoringEnabled If {@code true} then Visor should collect information about tasks.
     */
    public void taskMonitoringEnabled(boolean taskMonitoringEnabled) {
        this.taskMonitoringEnabled = taskMonitoringEnabled;
    }

    /**
     * @return Key for store and read last event order number.
     */
    public String eventsOrderKey() {
        return evtOrderKey;
    }

    /**
     * @param evtOrderKey Key for store and read last event order number.
     */
    public void eventsOrderKey(String evtOrderKey) {
        this.evtOrderKey = evtOrderKey;
    }

    /**
     * @return Key for store and read events throttle counter.
     */
    public String eventsThrottleCounterKey() {
        return evtThrottleCntrKey;
    }

    /**
     * @param evtThrottleCntrKey Key for store and read events throttle counter.
     */
    public void eventsThrottleCounterKey(String evtThrottleCntrKey) {
        this.evtThrottleCntrKey = evtThrottleCntrKey;
    }

    /**
     * @return Number of items to evaluate cache size.
     */
    public int sample() {
        return sample;
    }

    /**
     * @param sample Number of items to evaluate cache size.
     */
    public void sample(int sample) {
        this.sample = sample;
    }

    /**
     * @return {@code true} if Visor should collect metrics for system caches.
     */
    public boolean systemCaches() {
        return sysCaches;
    }

    /**
     * @param sysCaches {@code true} if Visor should collect metrics for system caches.
     */
    public void systemCaches(boolean sysCaches) {
        this.sysCaches = sysCaches;
    }
}