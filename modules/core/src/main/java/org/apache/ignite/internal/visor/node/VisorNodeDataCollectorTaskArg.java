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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data collector task arguments.
 */
public class VisorNodeDataCollectorTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether task monitoring should be enabled. */
    private boolean taskMonitoringEnabled;

    /** Visor unique key to get last event order from node local storage. */
    private String evtOrderKey;

    /** Visor unique key to get lost events throttle counter from node local storage. */
    private String evtThrottleCntrKey;

    /** If {@code true} then collect information about system caches. */
    private boolean sysCaches;

    /** If {@code false} then cache metrics will not be collected. */
    private boolean collectCacheMetrics;

    /** Optional Set of cache groups, if provided, then caches only from that groups will be collected. */
    private Set<String> cacheGrps;

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
     * @param sysCaches If {@code true} then collect information about system caches.
     * @param collectCacheMetrics If {@code false} then cache metrics will not be collected.
     * @param cacheGrps Optional Set of cache groups, if provided, then caches only from that groups will be collected.
     */
    public VisorNodeDataCollectorTaskArg(
        boolean taskMonitoringEnabled,
        String evtOrderKey,
        String evtThrottleCntrKey,
        boolean sysCaches,
        boolean collectCacheMetrics,
        Set<String> cacheGrps
    ) {
        this.taskMonitoringEnabled = taskMonitoringEnabled;
        this.evtOrderKey = evtOrderKey;
        this.evtThrottleCntrKey = evtThrottleCntrKey;
        this.sysCaches = sysCaches;
        this.collectCacheMetrics = collectCacheMetrics;
        this.cacheGrps = cacheGrps;
    }

    /**
     * Create task arguments with given parameters.
     *
     * @param taskMonitoringEnabled If {@code true} then Visor should collect information about tasks.
     * @param evtOrderKey Event order key, unique for Visor instance.
     * @param evtThrottleCntrKey Event throttle counter key, unique for Visor instance.
     * @param sysCaches If {@code true} then collect information about system caches.
     * @param collectCacheMetrics If {@code false} then cache metrics will not be collected.
     */
    public VisorNodeDataCollectorTaskArg(
        boolean taskMonitoringEnabled,
        String evtOrderKey,
        String evtThrottleCntrKey,
        boolean sysCaches,
        boolean collectCacheMetrics
    ) {
        this(taskMonitoringEnabled, evtOrderKey, evtThrottleCntrKey, sysCaches, collectCacheMetrics, null);
    }

    /**
     * Create task arguments with given parameters.
     *
     * @param taskMonitoringEnabled If {@code true} then Visor should collect information about tasks.
     * @param evtOrderKey Event order key, unique for Visor instance.
     * @param evtThrottleCntrKey Event throttle counter key, unique for Visor instance.
     * @param sysCaches If {@code true} then collect information about system caches.
     */
    public VisorNodeDataCollectorTaskArg(
        boolean taskMonitoringEnabled,
        String evtOrderKey,
        String evtThrottleCntrKey,
        boolean sysCaches
    ) {
        this(taskMonitoringEnabled, evtOrderKey, evtThrottleCntrKey, sysCaches, true, null);
    }

    /**
     * @return {@code true} if Visor should collect information about tasks.
     */
    public boolean isTaskMonitoringEnabled() {
        return taskMonitoringEnabled;
    }

    /**
     * @param taskMonitoringEnabled If {@code true} then Visor should collect information about tasks.
     */
    public void setTaskMonitoringEnabled(boolean taskMonitoringEnabled) {
        this.taskMonitoringEnabled = taskMonitoringEnabled;
    }

    /**
     * @return Key for store and read last event order number.
     */
    public String getEventsOrderKey() {
        return evtOrderKey;
    }

    /**
     * @param evtOrderKey Key for store and read last event order number.
     */
    public void setEventsOrderKey(String evtOrderKey) {
        this.evtOrderKey = evtOrderKey;
    }

    /**
     * @return Key for store and read events throttle counter.
     */
    public String getEventsThrottleCounterKey() {
        return evtThrottleCntrKey;
    }

    /**
     * @param evtThrottleCntrKey Key for store and read events throttle counter.
     */
    public void setEventsThrottleCounterKey(String evtThrottleCntrKey) {
        this.evtThrottleCntrKey = evtThrottleCntrKey;
    }

    /**
     * @return {@code true} if Visor should collect metrics for system caches.
     */
    public boolean getSystemCaches() {
        return sysCaches;
    }

    /**
     * @param sysCaches {@code true} if Visor should collect metrics for system caches.
     */
    public void setSystemCaches(boolean sysCaches) {
        this.sysCaches = sysCaches;
    }

    /**
     * @return If {@code false} then cache metrics will not be collected.
     */
    public boolean isCollectCacheMetrics() {
        return collectCacheMetrics;
    }

    /**
     * @param collectCacheMetrics If {@code false} then cache metrics will not be collected.
     */
    public void setCollectCacheMetrics(boolean collectCacheMetrics) {
        this.collectCacheMetrics = collectCacheMetrics;
    }

    /**
     * @return Optional cache group, if provided, then caches only from that group will be collected.
     */
    public Set<String> getCacheGroups() {
        return cacheGrps;
    }

    /**
     * @param cacheGrps Optional Set of cache groups, if provided, then caches only from that groups will be collected.
     */
    public void setCacheGroups(Set<String> cacheGrps) {
        this.cacheGrps = cacheGrps;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(taskMonitoringEnabled);
        U.writeString(out, evtOrderKey);
        U.writeString(out, evtThrottleCntrKey);
        out.writeBoolean(sysCaches);
        out.writeBoolean(collectCacheMetrics);
        U.writeCollection(out, cacheGrps);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        taskMonitoringEnabled = in.readBoolean();
        evtOrderKey = U.readString(in);
        evtThrottleCntrKey = U.readString(in);
        sysCaches = in.readBoolean();

        collectCacheMetrics = protoVer < V2 || in.readBoolean();

        cacheGrps = protoVer < V3 ? null : U.readSet(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeDataCollectorTaskArg.class, this);
    }
}
