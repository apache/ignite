/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.jobmetrics;

import org.gridgain.grid.util.typedef.internal.*;

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
