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
     * @return Number of items to evaluate cache size.
     */
    public int getSample() {
        return sample;
    }

    /**
     * @param sample Number of items to evaluate cache size.
     */
    public void setSample(int sample) {
        this.sample = sample;
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

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(taskMonitoringEnabled);
        U.writeString(out, evtOrderKey);
        U.writeString(out, evtThrottleCntrKey);
        out.writeInt(sample);
        out.writeBoolean(sysCaches);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        taskMonitoringEnabled = in.readBoolean();
        evtOrderKey = U.readString(in);
        evtThrottleCntrKey = U.readString(in);
        sample = in.readInt();
        sysCaches = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeDataCollectorTaskArg.class, this);
    }
}
