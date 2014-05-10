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

import static org.gridgain.grid.kernal.processors.hadoop.jobtracker.GridHadoopJobPhase.*;

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

    /** Pending splits for which mapper should be executed. */
    private Collection<GridHadoopInputSplit> pendingSplits;

    /** Pending reducers. */
    private Collection<Integer> pendingReducers;

    /** Task number map. */
    private Map<Object, Integer> taskNumMap = new HashMap<>();

    /** Next task number. */
    private int nextTaskNum;

    /** External execution flag. */
    private boolean externalExec;

    /** Job phase. */
    private GridHadoopJobPhase phase = PHASE_MAP;

    /** Fail cause. */
    private Throwable failCause;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridHadoopJobMetadata() {
        // No-op.
    }

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
        externalExec = src.externalExec;
        failCause = src.failCause;
        jobId = src.jobId;
        jobInfo = src.jobInfo;
        mrPlan = src.mrPlan;
        pendingSplits = src.pendingSplits;
        pendingReducers = src.pendingReducers;
        phase = src.phase;
        taskNumMap = src.taskNumMap;
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
     * @return External execution flag.
     */
    public boolean externalExecution() {
        return externalExec;
    }

    /**
     * @param externalExec External execution flag.
     */
    public void externalExecution(boolean externalExec) {
        this.externalExec = externalExec;
    }

    /**
     * Sets collection of pending splits.
     *
     * @param pendingSplits Collection of pending splits.
     */
    public void pendingSplits(Collection<GridHadoopInputSplit> pendingSplits) {
        this.pendingSplits = pendingSplits;
    }

    /**
     * Gets collection of pending splits.
     *
     * @return Collection of pending splits.
     */
    public Collection<GridHadoopInputSplit> pendingSplits() {
        return pendingSplits;
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

        // Initialize task numbers.
        for (UUID nodeId : mrPlan.mapperNodeIds()) {
            for (GridHadoopInputSplit split : mrPlan.mappers(nodeId))
                assignTaskNumber(split);

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
        out.writeObject(pendingSplits);
        out.writeObject(pendingReducers);
        out.writeObject(taskNumMap);
        out.writeInt(nextTaskNum);
        out.writeObject(phase);
        out.writeBoolean(externalExec);
        out.writeObject(failCause);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (GridHadoopJobId)in.readObject();
        jobInfo = (GridHadoopJobInfo)in.readObject();
        mrPlan = (GridHadoopMapReducePlan)in.readObject();
        pendingSplits = (Collection<GridHadoopInputSplit>)in.readObject();
        pendingReducers = (Collection<Integer>)in.readObject();
        taskNumMap = (Map<Object, Integer>)in.readObject();
        nextTaskNum = in.readInt();
        phase = (GridHadoopJobPhase)in.readObject();
        externalExec = in.readBoolean();
        failCause = (Throwable)in.readObject();
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridHadoopJobMetadata.class, this, "pendingMaps", pendingSplits.size(),
                "pendingReduces", pendingReducers.size());
    }
}
