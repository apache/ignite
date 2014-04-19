/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * TODO make externalizable.
 *
 * Hadoop job metadata. Internal object used for distributed job state tracking.
 */
public class GridHadoopJobMetadata implements Serializable {
    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Job info. */
    private GridHadoopJobInfo jobInfo;

    /** Map-reduce plan. */
    private GridHadoopMapReducePlan mrPlan;

    /** Pending blocks for which mapper should be executed. */
    private Collection<GridHadoopFileBlock> pendingBlocks;

    /** Pending reducers. */
    private Collection<Integer> pendingReducers;

    /** Job phase. */
    private GridHadoopJobPhase phase = GridHadoopJobPhase.PHASE_MAP;

    /**
     * @param jobId Job ID.
     * @param jobInfo Job info.
     */
    public GridHadoopJobMetadata(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        this.jobId = jobId;
        this.jobInfo = jobInfo;
    }

    /**
     * Copy constructor.
     *
     * @param src Metadata to copy.
     */
    public GridHadoopJobMetadata(GridHadoopJobMetadata src) {
        // Make sure to preserve alphabetic order.
        jobId = src.jobId;
        jobInfo = src.jobInfo;
        mrPlan = src.mrPlan;
        pendingBlocks = src.pendingBlocks;
        pendingReducers = src.pendingReducers;
        phase = src.phase;
    }

    /**
     * @param phase Job phase.
     */
    public void phase(GridHadoopJobPhase phase) {
        this.phase = phase;
    }

    /**
     * @return Job phase.
     */
    public GridHadoopJobPhase phase() {
        return phase;
    }

    /**
     * Sets collection of pending blocks.
     *
     * @param pendingBlocks Collection of pending blocks.
     */
    public void pendingBlocks(Collection<GridHadoopFileBlock> pendingBlocks) {
        this.pendingBlocks = pendingBlocks;
    }

    /**
     * Gets collection of pending blocks.
     *
     * @return Collection of pending blocks.
     */
    public Collection<GridHadoopFileBlock> pendingBlocks() {
        return pendingBlocks;
    }

    /**
     * Sets collection of pending reducers.
     *
     * @param pendingReducers Collection of pending reducers.
     */
    public void pendingReducers(Collection<Integer> pendingReducers) {
        this.pendingReducers = pendingReducers;
    }

    /**
     * Gets collection of pending reducers.
     *
     * @return Collection of pending reducers.
     */
    public Collection<Integer> pendingReducers() {
        return pendingReducers;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @param mrPlan Map-reduce plan.
     */
    public void mapReducePlan(GridHadoopMapReducePlan mrPlan) {
        this.mrPlan = mrPlan;
    }

    /**
     * @return Map-reduce plan.
     */
    public GridHadoopMapReducePlan mapReducePlan() {
        return mrPlan;
    }

    /**
     * @return Job info.
     */
    public GridHadoopJobInfo jobInfo() {
        return jobInfo;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridHadoopJobMetadata.class, this, "pendingMaps", pendingBlocks.size(),
            "pendingReduces", pendingReducers.size());
    }
}
