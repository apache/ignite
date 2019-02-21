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

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

import org.apache.ignite.events.Event;

/**
 * Container of required data for changing baseline.
 */
class BaselineAutoAdjustData {
    /** Event with which this data correspond to. For statistic only. */
    private final Event reasonEvent;
    /** Topology version nodes of which should be set to baseline by this task. */
    private final long targetTopologyVersion;

    /** {@code true} If this data don't actual anymore and it setting should be skipped. */
    private volatile boolean invalidated = false;

    /**
     * @param evt Event with which this data correspond to. For statistic only.
     * @param targetTopologyVersion Topology version nodes of which should be set by this task.
     */
    BaselineAutoAdjustData(Event evt, long targetTopologyVersion) {
        reasonEvent = evt;
        this.targetTopologyVersion = targetTopologyVersion;
    }

    /**
     * Mark that this data are invalid.
     */
    private void onInvalidate() {
        invalidated = true;
    }

    /**
     * @return Topology version nodes of which should be set to baseline by this task.
     */
    public long getTargetTopologyVersion() {
        return targetTopologyVersion;
    }

    /**
     * @return {@code true} If this data still actual and can be set.
     */
    public boolean isInvalidated() {
        return invalidated;
    }

    /**
     * Produce next set baseline data based on this data.
     *
     * @param evt New triggired event.
     * @return New set baseline data.
     */
    public BaselineAutoAdjustData next(Event evt, long targetTopologyVersion) {
        onInvalidate();

        return new BaselineAutoAdjustData(evt, targetTopologyVersion);
    }
}
