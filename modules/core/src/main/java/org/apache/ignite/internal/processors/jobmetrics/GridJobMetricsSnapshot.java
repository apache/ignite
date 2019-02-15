/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.jobmetrics;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Job metrics snapshot.
 */
public class GridJobMetricsSnapshot {
    /** */
    private final long ts = U.currentTimeMillis();

    /** */
    private int started;

    /** */
    private int activeJobs;

    /** */
    private int passiveJobs;

    /** */
    private int cancelJobs;

    /** */
    private int rejectJobs;

    /** */
    private long execTime;

    /** */
    private long waitTime;

    /** */
    private long maxExecTime;

    /** */
    private long maxWaitTime;

    /** */
    private int finished;

    /** */
    private double cpuLoad;

    /**
     * @return The activeJobs.
     */
    public int getActiveJobs() {
        return activeJobs;
    }

    /**
     * @param activeJobs The activeJobs to set.
     */
    public void setActiveJobs(int activeJobs) {
        this.activeJobs = activeJobs;
    }

    /**
     * @return The passiveJobs.
     */
    public int getPassiveJobs() {
        return passiveJobs;
    }

    /**
     * @param passiveJobs The passiveJobs to set.
     */
    public void setPassiveJobs(int passiveJobs) {
        this.passiveJobs = passiveJobs;
    }

    /**
     * @return The cancelJobs.
     */
    public int getCancelJobs() {
        return cancelJobs;
    }

    /**
     * @param cancelJobs The cancelJobs to set.
     */
    public void setCancelJobs(int cancelJobs) {
        this.cancelJobs = cancelJobs;
    }

    /**
     * @return The rejectJobs.
     */
    public int getRejectJobs() {
        return rejectJobs;
    }

    /**
     * @param rejectJobs The rejectJobs to set.
     */
    public void setRejectJobs(int rejectJobs) {
        this.rejectJobs = rejectJobs;
    }

    /**
     * @return The execTime.
     */
    public long getExecutionTime() {
        return execTime;
    }

    /**
     * @param execTime The execTime to set.
     */
    public void setExecutionTime(long execTime) {
        this.execTime = execTime;
    }

    /**
     * @return The waitTime.
     */
    public long getWaitTime() {
        return waitTime;
    }

    /**
     * @param waitTime The waitTime to set.
     */
    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    /**
     * @return The maxExecTime.
     */
    public long getMaximumExecutionTime() {
        return maxExecTime;
    }

    /**
     * @param maxExecTime The maxExecTime to set.
     */
    public void setMaximumExecutionTime(long maxExecTime) {
        this.maxExecTime = maxExecTime;
    }

    /**
     * @return The maxWaitTime.
     */
    public long getMaximumWaitTime() {
        return maxWaitTime;
    }

    /**
     * @param maxWaitTime The maxWaitTime to set.
     */
    public void setMaximumWaitTime(long maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    /**
     * @return The timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @return Number of finished jobs for this snapshot.
     */
    public int getFinishedJobs() {
        return finished;
    }

    /**
     * @param finished Number of finished jobs for this snapshot.
     */
    public void setFinishedJobs(int finished) {
        this.finished = finished;
    }

    /**
     * @return Started jobs.
     */
    public int getStartedJobs() {
        return started;
    }

    /**
     * @param started Started jobs.
     */
    public void setStartedJobs(int started) {
        this.started = started;
    }

    /**
     * @return Current CPU load.
     */
    public double getCpuLoad() {
        return cpuLoad;
    }

    /**
     * @param cpuLoad Current CPU load.
     */
    public void setCpuLoad(double cpuLoad) {
        this.cpuLoad = cpuLoad;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobMetricsSnapshot.class, this);
    }
}