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

import static org.gridgain.grid.hadoop.GridHadoopJobPhase.*;

/**
 * Hadoop job metadata. Internal object used for distributed job state tracking.
 */
public class GridHadoopJobMetadata implements Externalizable {
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

    /** Task number map. */
    private Map<Object, Integer> taskNumMap = new HashMap<>();

    /** Next task number. */
    private int nextTaskNum;

    /** Job phase. */
    private GridHadoopJobPhase phase = PHASE_MAP;

    /** Fail cause. */
    private Throwable failCause;

    /** Total block count. */
    private int totalBlockCnt;

    /** Total reducer count. */
    private int totalReducerCnt;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridHadoopJobMetadata() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param jobId Job ID.
     * @param jobInfo Job info.
     * @param pendingBlocks Pending blocks.
     * @param pendingReducers Pending reducers.
     */
    public GridHadoopJobMetadata(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo,
        Collection<GridHadoopFileBlock> pendingBlocks, Collection<Integer> pendingReducers) {
        this.jobId = jobId;
        this.jobInfo = jobInfo;
        this.pendingBlocks = pendingBlocks;
        this.pendingReducers = pendingReducers;

        totalBlockCnt = pendingBlocks == null ? 0 : pendingBlocks.size();
        totalReducerCnt = pendingReducers == null ? 0 : pendingReducers.size();
    }

    /**
     * Copy constructor.
     *
     * @param src Metadata to copy.
     */
    public GridHadoopJobMetadata(GridHadoopJobMetadata src) {
        // Make sure to preserve alphabetic order.
        failCause = src.failCause;
        jobId = src.jobId;
        jobInfo = src.jobInfo;
        mrPlan = src.mrPlan;
        pendingBlocks = src.pendingBlocks;
        pendingReducers = src.pendingReducers;
        phase = src.phase;
        taskNumMap = src.taskNumMap;
        totalBlockCnt = src.totalBlockCnt;
        totalReducerCnt = src.totalReducerCnt;
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
     * @return Total block count.
     */
    public int totalBlockCount() {
        return totalBlockCnt;
    }

    /**
     * @return Total reducer count.
     */
    public int totalReducerCount() {
        return totalReducerCnt;
    }

    /**
     * @param mrPlan Map-reduce plan.
     */
    public void mapReducePlan(GridHadoopMapReducePlan mrPlan) {
        this.mrPlan = mrPlan;

        // Initialize task numbers.
        for (UUID nodeId : mrPlan.mapperNodeIds()) {
            for (GridHadoopFileBlock block : mrPlan.mappers(nodeId))
                assignTaskNumber(block);

            // Combiner task.
            assignTaskNumber(nodeId);
        }

        for (UUID nodeId : mrPlan.reducerNodeIds()) {
            for (int rdc : mrPlan.reducers(nodeId))
                assignTaskNumber(rdc);
        }
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

    /**
     * @param failCause Fail cause.
     */
    public void failCause(Throwable failCause) {
        this.failCause = failCause;
    }

    /**
     * @return Fail cause.
     */
    public Throwable failCause() {
        return failCause;
    }

    /**
     * @param src Task source.
     * @return Task number.
     */
    public int taskNumber(Object src) {
        Integer res = taskNumMap.get(src);

        if (res == null)
            throw new IllegalArgumentException("Failed to find task number for source [src=" + src +
                ", map=" + taskNumMap + ']');

        return res;
    }

    /**
     * Assigns next available task number to a task source if was not assigned yet.
     *
     * @param src Task source to assign number for.
     */
    private void assignTaskNumber(Object src) {
        Integer num = taskNumMap.get(src);

        if (num == null)
            taskNumMap.put(src, nextTaskNum++);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(jobId);
        out.writeObject(jobInfo);
        out.writeObject(mrPlan);
        out.writeObject(pendingBlocks);
        out.writeObject(pendingReducers);
        out.writeObject(taskNumMap);
        out.writeInt(nextTaskNum);
        out.writeObject(phase);
        out.writeObject(failCause);
        out.writeInt(totalBlockCnt);
        out.writeInt(totalReducerCnt);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (GridHadoopJobId)in.readObject();
        jobInfo = (GridHadoopJobInfo)in.readObject();
        mrPlan = (GridHadoopMapReducePlan)in.readObject();
        pendingBlocks = (Collection<GridHadoopFileBlock>)in.readObject();
        pendingReducers = (Collection<Integer>)in.readObject();
        taskNumMap = (Map<Object, Integer>)in.readObject();
        nextTaskNum = in.readInt();
        phase = (GridHadoopJobPhase)in.readObject();
        failCause = (Throwable)in.readObject();
        totalBlockCnt = in.readInt();
        totalReducerCnt = in.readInt();
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridHadoopJobMetadata.class, this, "pendingMaps", pendingBlocks.size(),
            "pendingReduces", pendingReducers.size());
    }
}
