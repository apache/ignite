/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.counter.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.hadoop.GridHadoopJobPhase.*;

/**
 * Hadoop job metadata. Internal object used for distributed job state tracking.
 */
public class GridHadoopJobMetadata implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Job info. */
    private GridHadoopJobInfo jobInfo;

    /** Node submitted job. */
    private UUID submitNodeId;

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

    /** Reducers addresses. */
    @GridToStringInclude
    private Map<Integer, GridHadoopProcessDescriptor> reducersAddrs;

    /** Job phase. */
    private GridHadoopJobPhase phase = PHASE_SETUP;

    /** Fail cause. */
    @GridToStringExclude
    private Throwable failCause;

    /** Version. */
    private long ver;

    /** Start time. */
    private long startTs;

    /** Setup phase complete time. */
    private long setupCompleteTs;

    /** Map phase complete time. */
    private long mapCompleteTs;

    /** Job complete time. */
    private long completeTs;

    /** Job counters */
    private GridHadoopCounters counters = new GridHadoopCountersImpl();

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridHadoopJobMetadata() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param submitNodeId Submit node ID.
     * @param jobId Job ID.
     * @param jobInfo Job info.
     */
    public GridHadoopJobMetadata(UUID submitNodeId, GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        this.jobId = jobId;
        this.jobInfo = jobInfo;
        this.submitNodeId = submitNodeId;

        startTs = System.currentTimeMillis();
    }

    /**
     * Copy constructor.
     *
     * @param src Metadata to copy.
     */
    public GridHadoopJobMetadata(GridHadoopJobMetadata src) {
        // Make sure to preserve alphabetic order.
        completeTs = src.completeTs;
        counters = src.counters;
        failCause = src.failCause;
        jobId = src.jobId;
        jobInfo = src.jobInfo;
        mapCompleteTs = src.mapCompleteTs;
        mrPlan = src.mrPlan;
        pendingSplits = src.pendingSplits;
        pendingReducers = src.pendingReducers;
        phase = src.phase;
        reducersAddrs = src.reducersAddrs;
        setupCompleteTs = src.setupCompleteTs;
        startTs = src.startTs;
        submitNodeId = src.submitNodeId;
        taskNumMap = src.taskNumMap;
        ver = src.ver + 1;
    }

    /**
     * @return Submit node ID.
     */
    public UUID submitNodeId() {
        return submitNodeId;
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
     * Gets reducers addresses for external execution.
     *
     * @return Reducers addresses.
     */
    public Map<Integer, GridHadoopProcessDescriptor> reducersAddresses() {
        return reducersAddrs;
    }

    /**
     * Sets reducers addresses for external execution.
     *
     * @param reducersAddrs Map of addresses.
     */
    public void reducersAddresses(Map<Integer, GridHadoopProcessDescriptor> reducersAddrs) {
        this.reducersAddrs = reducersAddrs;
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
     * @return Job start time.
     */
    public long startTimestamp() {
        return startTs;
    }

    /**
     * @return Setup complete time.
     */
    public long setupCompleteTimestamp() {
        return setupCompleteTs;
    }

    /**
     * @return Map complete time.
     */
    public long mapCompleteTimestamp() {
        return mapCompleteTs;
    }

    /**
     * @return Complete time.
     */
    public long completeTimestamp() {
        return completeTs;
    }

    /**
     * @param setupCompleteTs Setup complete timestamp.
     */
    public void setupCompleteTimestamp(long setupCompleteTs) {
        this.setupCompleteTs = setupCompleteTs;
    }

    /**
     * @param mapCompleteTs Map complete time.
     */
    public void mapCompleteTimestamp(long mapCompleteTs) {
        this.mapCompleteTs = mapCompleteTs;
    }

    /**
     * @param completeTs Complete time.
     */
    public void completeTimestamp(long completeTs) {
        this.completeTs = completeTs;
    }

    /**
     * @return Setup time in milliseconds.
     */
    public long setupTime() {
        return setupCompleteTs = startTs;
    }

    /**
     * @return Map time in milliseconds.
     */
    public long mapTime() {
        return mapCompleteTs - setupCompleteTs;
    }

    /**
     * @return Reduce time in milliseconds.
     */
    public long reduceTime() {
        return completeTs - mapCompleteTs;
    }

    /**
     * @return Total execution time in milliseconds.
     */
    public long totalTime() {
        return completeTs - startTs;
    }

    /**
     * @param mrPlan Map-reduce plan.
     */
    @SuppressWarnings("ConstantConditions")
    public void mapReducePlan(GridHadoopMapReducePlan mrPlan) {
        assert this.mrPlan == null : "Map-reduce plan can only be initialized once.";

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
     * Returns job counters.
     *
     * @return Collection of counters.
     */
    public GridHadoopCounters counters() {
        return counters;
    }

    /**
     * Sets counters.
     *
     * @param counters Collection of counters.
     */
    public void counters(GridHadoopCounters counters) {
        this.counters = counters;
    }

    /**
     * @param failCause Fail cause.
     */
    public void failCause(Throwable failCause) {
        assert failCause != null;

        if (this.failCause == null) // Keep the first error.
            this.failCause = failCause;
    }

    /**
     * @return Fail cause.
     */
    public Throwable failCause() {
        return failCause;
    }

    /**
     * @return Version.
     */
    public long version() {
        return ver;
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
        U.writeUuid(out, submitNodeId);
        out.writeObject(jobId);
        out.writeObject(jobInfo);
        out.writeObject(mrPlan);
        out.writeObject(pendingSplits);
        out.writeObject(pendingReducers);
        out.writeObject(taskNumMap);
        out.writeInt(nextTaskNum);
        out.writeObject(phase);
        out.writeObject(failCause);
        out.writeLong(ver);
        out.writeLong(startTs);
        out.writeLong(setupCompleteTs);
        out.writeLong(mapCompleteTs);
        out.writeLong(completeTs);
        out.writeObject(reducersAddrs);
        out.writeObject(counters);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        submitNodeId = U.readUuid(in);
        jobId = (GridHadoopJobId)in.readObject();
        jobInfo = (GridHadoopJobInfo)in.readObject();
        mrPlan = (GridHadoopMapReducePlan)in.readObject();
        pendingSplits = (Collection<GridHadoopInputSplit>)in.readObject();
        pendingReducers = (Collection<Integer>)in.readObject();
        taskNumMap = (Map<Object, Integer>)in.readObject();
        nextTaskNum = in.readInt();
        phase = (GridHadoopJobPhase)in.readObject();
        failCause = (Throwable)in.readObject();
        ver = in.readLong();
        startTs = in.readLong();
        setupCompleteTs = in.readLong();
        mapCompleteTs = in.readLong();
        completeTs = in.readLong();
        reducersAddrs = (Map<Integer, GridHadoopProcessDescriptor>)in.readObject();
        counters = (GridHadoopCounters)in.readObject();
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridHadoopJobMetadata.class, this, "pendingMaps", pendingSplits.size(),
            "pendingReduces", pendingReducers.size(), "failCause", failCause == null ? null :
                failCause.getClass().getName());
    }
}
