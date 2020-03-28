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

/**
 * Container of required data for changing baseline.
 */
class BaselineAutoAdjustData {
    /** Task represented NULL value is using when normal task can not be created. */
    public static final BaselineAutoAdjustData NULL_BASELINE_DATA = nullValue();

    /** Topology version nodes of which should be set to baseline by this task. */
    private final long targetTopologyVersion;

    /** {@code true} If this data don't actual anymore and it setting should be skipped. */
    private volatile boolean invalidated = false;

    /** {@code true} If this data was adjusted. */
    private volatile boolean adjusted = false;

    /**
     * @param targetTopologyVersion Topology version nodes of which should be set by this task.
     */
    BaselineAutoAdjustData(long targetTopologyVersion) {
        this.targetTopologyVersion = targetTopologyVersion;
    }

    /**
     * @return New null value.
     */
    private static BaselineAutoAdjustData nullValue() {
        BaselineAutoAdjustData data = new BaselineAutoAdjustData(-1);

        data.onInvalidate();
        data.onAdjust();

        return data;
    }

    /**
     * Mark that this data are invalid.
     */
    private void onInvalidate() {
        invalidated = true;
    }

    /**
     * Mark that this data was adjusted.
     */
    public void onAdjust() {
        adjusted = true;
    }

    /**
     * @return Topology version nodes of which should be set to baseline by this task.
     */
    public long getTargetTopologyVersion() {
        return targetTopologyVersion;
    }

    /**
     * @return {@code true} If this data already invalidated and can not be set.
     */
    public boolean isInvalidated() {
        return invalidated;
    }

    /**
     * @return {@code true} If this data already adjusted.
     */
    public boolean isAdjusted() {
        return adjusted;
    }

    /**
     * Produce next set baseline data based on this data.
     *
     * @return New set baseline data.
     */
    public BaselineAutoAdjustData next(long targetTopologyVersion) {
        onInvalidate();

        return new BaselineAutoAdjustData(targetTopologyVersion);
    }
}
