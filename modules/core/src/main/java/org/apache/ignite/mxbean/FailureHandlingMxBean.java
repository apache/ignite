/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.mxbean;

/**
 * MBean that controls critical failure handling.
 */
@MXBeanDescription("MBean that controls critical failure handling.")
public interface FailureHandlingMxBean {
    /** */
    @MXBeanDescription("Enable/disable critical workers liveness checking.")
    public boolean getLivenessCheckEnabled();

    /** */
    public void setLivenessCheckEnabled(boolean val);

    /** */
    @MXBeanDescription("Maximum inactivity period for system worker. Critical failure handler fires if exceeded. " +
        "Nonpositive value denotes infinite timeout.")
    public long getSystemWorkerBlockedTimeout();

    /** */
    public void setSystemWorkerBlockedTimeout(long val);

    /** */
    @MXBeanDescription("Timeout for checkpoint read lock acquisition. Critical failure handler fires if exceeded. " +
        "Nonpositive value denotes infinite timeout.")
    public long getCheckpointReadLockTimeout();

    /** */
    public void setCheckpointReadLockTimeout(long val);
}
