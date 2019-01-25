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

import java.util.Collection;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.events.Event;

/**
 * Container of required data for changing baseline.
 */
class BaselineAutoAdjustData {
    /** Event with which this data correspond to. For statistic only. */
    private final Event reasonEvent;
    /** Baseline nodes which should be set by this task. */
    private final Collection<BaselineNode> targetBaselineNodes;
    private final long targetTopologyVersion;
    /** Time when was created first task which was declined by next task. */
    private final long firstUnfinishedTaskCreatedTime;

    /** {@code true} If this data don't actual anymore and it setting should be skipped. */
    private volatile boolean isInvalidate = false;
    /** {@code true} If this data was set to grid. */
    private volatile boolean isSet = false;

    /**
     * @param evt Event with which this data correspond to. For statistic only.
     * @param nodes Baseline nodes which should be set by this task.
     * @param targetTopologyVersion
     * @param time Time when was created first task which was declined by next task.
     */
    BaselineAutoAdjustData(Event evt, Collection<BaselineNode> nodes, long targetTopologyVersion, long time) {
        reasonEvent = evt;
        targetBaselineNodes = nodes;
        this.targetTopologyVersion = targetTopologyVersion;
        firstUnfinishedTaskCreatedTime = time;
    }

    /**
     * Mark that this data are invalid.
     */
    private void onInvalidate() {
        isInvalidate = true;
    }

    /**
     * @return {@code true} If this data was set to grid.
     */
    private boolean isSet() {
        return isSet;
    }

    /**
     * Mark that this data was set.
     */
    public void onSet() {
        isSet = true;
    }

    /**
     * @return Time when was created first task which was declined by next task.
     */
    public long getFirstUnfinishedTaskCreatedTime() {
        return firstUnfinishedTaskCreatedTime == -1
            ? System.currentTimeMillis()
            : firstUnfinishedTaskCreatedTime;
    }

    /**
     * @return Baseline nodes which should be set by this task.
     */
    public Collection<BaselineNode> getTargetBaselineNodes() {
        return targetBaselineNodes;
    }

    /**
     * @return {@code true} If this data still actual and can be set.
     */
    public boolean isInvalidate() {
        return isInvalidate;
    }

    /**
     * Produce next set baseline data based on this data.
     *
     * @param evt New triggired event.
     * @param newTargetBaseline New baseline.
     * @return New set baseline data.
     */
    public BaselineAutoAdjustData next(Event evt, long targetTopologyVersion, Collection<BaselineNode> newTargetBaseline) {
        onInvalidate();

        return new BaselineAutoAdjustData(
            evt,
            newTargetBaseline,
            targetTopologyVersion, isSet() ? System.currentTimeMillis() : getFirstUnfinishedTaskCreatedTime()
        );
    }
}
