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

package org.apache.ignite.internal.processors.hadoop.jobtracker;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopJobPhase;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlan;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCountersImpl;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopProcessDescriptor;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.hadoop.HadoopJobPhase.PHASE_SETUP;

/**
 * Hadoop job metadata. Internal object used for distributed job state tracking.
 */
public class HadoopJobMetadata implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    private HadoopJobId jobId;

    /** Job info. */
    private HadoopJobInfo jobInfo;

    /** Node submitted job. */
    private UUID submitNodeId;

    /** Map-reduce plan. */
    private HadoopMapReducePlan mrPlan;

    /** Pending splits for which mapper should be executed. */
    private Map<HadoopInputSplit, Integer> pendingSplits;

    /** Pending reducers. */
    private Collection<Integer> pendingReducers;

    /** Reducers addresses. */
    @GridToStringInclude
    private Map<Integer, HadoopProcessDescriptor> reducersAddrs;

    /** Job phase. */
    private HadoopJobPhase phase = PHASE_SETUP;

    /** Fail cause. */
    @GridToStringExclude
    private Throwable failCause;

    /** Version. */
    private long ver;

    /** Job counters */
    private HadoopCounters counters = new HadoopCountersImpl();

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public HadoopJobMetadata() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param submitNodeId Submit node ID.
     * @param jobId Job ID.
     * @param jobInfo Job info.
     */
    public HadoopJobMetadata(UUID submitNodeId, HadoopJobId jobId, HadoopJobInfo jobInfo) {
        this.jobId = jobId;
        this.jobInfo = jobInfo;
        this.submitNodeId = submitNodeId;
    }

    /**
     * Copy constructor.
     *
     * @param src Metadata to copy.
     */
    public HadoopJobMetadata(HadoopJobMetadata src) {
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
    public void phase(HadoopJobPhase phase) {
        this.phase = phase;
    }

    /**
     * @return Job phase.
     */
    public HadoopJobPhase phase() {
        return phase;
    }

    /**
     * Gets reducers addresses for external execution.
     *
     * @return Reducers addresses.
     */
    public Map<Integer, HadoopProcessDescriptor> reducersAddresses() {
        return reducersAddrs;
    }

    /**
     * Sets reducers addresses for external execution.
     *
     * @param reducersAddrs Map of addresses.
     */
    public void reducersAddresses(Map<Integer, HadoopProcessDescriptor> reducersAddrs) {
        this.reducersAddrs = reducersAddrs;
    }

    /**
     * Sets collection of pending splits.
     *
     * @param pendingSplits Collection of pending splits.
     */
    public void pendingSplits(Map<HadoopInputSplit, Integer> pendingSplits) {
        this.pendingSplits = pendingSplits;
    }

    /**
     * Gets collection of pending splits.
     *
     * @return Collection of pending splits.
     */
    public Map<HadoopInputSplit, Integer> pendingSplits() {
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
    public HadoopJobId jobId() {
        return jobId;
    }

    /**
     * @param mrPlan Map-reduce plan.
     */
    public void mapReducePlan(HadoopMapReducePlan mrPlan) {
        assert this.mrPlan == null : "Map-reduce plan can only be initialized once.";

        this.mrPlan = mrPlan;
    }

    /**
     * @return Map-reduce plan.
     */
    public HadoopMapReducePlan mapReducePlan() {
        return mrPlan;
    }

    /**
     * @return Job info.
     */
    public HadoopJobInfo jobInfo() {
        return jobInfo;
    }

    /**
     * Returns job counters.
     *
     * @return Collection of counters.
     */
    public HadoopCounters counters() {
        return counters;
    }

    /**
     * Sets counters.
     *
     * @param counters Collection of counters.
     */
    public void counters(HadoopCounters counters) {
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
    public int taskNumber(HadoopInputSplit split) {
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
        jobId = (HadoopJobId)in.readObject();
        jobInfo = (HadoopJobInfo)in.readObject();
        mrPlan = (HadoopMapReducePlan)in.readObject();
        pendingSplits = (Map<HadoopInputSplit,Integer>)in.readObject();
        pendingReducers = (Collection<Integer>)in.readObject();
        phase = (HadoopJobPhase)in.readObject();
        failCause = (Throwable)in.readObject();
        ver = in.readLong();
        reducersAddrs = (Map<Integer, HadoopProcessDescriptor>)in.readObject();
        counters = (HadoopCounters)in.readObject();
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(HadoopJobMetadata.class, this, "pendingMaps", pendingSplits.size(),
            "pendingReduces", pendingReducers.size(), "failCause", failCause == null ? null :
                failCause.getClass().getName());
    }
}