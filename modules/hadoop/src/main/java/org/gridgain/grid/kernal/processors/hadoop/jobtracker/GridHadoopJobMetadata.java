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
    private Map<GridHadoopInputSplit, Integer> pendingSplits;

    /** Pending reducers. */
    private Collection<Integer> pendingReducers;

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
    }

    /**
     * Copy constructor.
     *
     * @param src Metadata to copy.
     */
    public GridHadoopJobMetadata(GridHadoopJobMetadata src) {
        // Make sure to preserve alphabetic order.
        counters = src.counters;
        failCause = src.failCause;
        jobId = src.jobId;
        jobInfo = src.jobInfo;
        mrPlan = src.mrPlan;
        pendingSplits = src.pendingSplits;
        pendingReducers = src.pendingReducers;
        phase = src.phase;
        reducersAddrs = src.reducersAddrs;
        submitNodeId = src.submitNodeId;
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
    public void pendingSplits(Map<GridHadoopInputSplit, Integer> pendingSplits) {
        this.pendingSplits = pendingSplits;
    }

    /**
     * Gets collection of pending splits.
     *
     * @return Collection of pending splits.
     */
    public Map<GridHadoopInputSplit, Integer> pendingSplits() {
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
        assert this.mrPlan == null : "Map-reduce plan can only be initialized once.";

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
     * @param split Split.
     * @return Task number.
     */
    public int taskNumber(GridHadoopInputSplit split) {
        return pendingSplits.get(split);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, submitNodeId);
        out.writeObject(jobId);
        out.writeObject(jobInfo);
        out.writeObject(mrPlan);
        out.writeObject(pendingSplits);
        out.writeObject(pendingReducers);
        out.writeObject(phase);
        out.writeObject(failCause);
        out.writeLong(ver);
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
        pendingSplits = (Map<GridHadoopInputSplit,Integer>)in.readObject();
        pendingReducers = (Collection<Integer>)in.readObject();
        phase = (GridHadoopJobPhase)in.readObject();
        failCause = (Throwable)in.readObject();
        ver = in.readLong();
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
