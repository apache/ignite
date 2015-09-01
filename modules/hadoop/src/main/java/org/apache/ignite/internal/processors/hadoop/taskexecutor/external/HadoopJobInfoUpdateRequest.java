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

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobPhase;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Job info update request.
 */
public class HadoopJobInfoUpdateRequest implements HadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    @GridToStringInclude
    private HadoopJobId jobId;

    /** Job phase. */
    @GridToStringInclude
    private HadoopJobPhase jobPhase;

    /** Reducers addresses. */
    @GridToStringInclude
    private HadoopProcessDescriptor[] reducersAddrs;

    /**
     * Constructor required by {@link Externalizable}.
     */
    public HadoopJobInfoUpdateRequest() {
        // No-op.
    }

    /**
     * @param jobId Job ID.
     * @param jobPhase Job phase.
     * @param reducersAddrs Reducers addresses.
     */
    public HadoopJobInfoUpdateRequest(HadoopJobId jobId, HadoopJobPhase jobPhase,
        HadoopProcessDescriptor[] reducersAddrs) {
        assert jobId != null;

        this.jobId = jobId;
        this.jobPhase = jobPhase;
        this.reducersAddrs = reducersAddrs;
    }

    /**
     * @return Job ID.
     */
    public HadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Job phase.
     */
    public HadoopJobPhase jobPhase() {
        return jobPhase;
    }

    /**
     * @return Reducers addresses.
     */
    public HadoopProcessDescriptor[] reducersAddresses() {
        return reducersAddrs;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        jobId.writeExternal(out);

        out.writeObject(jobPhase);
        U.writeArray(out, reducersAddrs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = new HadoopJobId();
        jobId.readExternal(in);

        jobPhase = (HadoopJobPhase)in.readObject();
        reducersAddrs = (HadoopProcessDescriptor[])U.readArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopJobInfoUpdateRequest.class, this);
    }
}