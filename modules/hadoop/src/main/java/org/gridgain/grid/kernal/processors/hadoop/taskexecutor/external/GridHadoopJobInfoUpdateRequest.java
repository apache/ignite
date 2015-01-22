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

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.apache.ignite.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Job info update request.
 */
public class GridHadoopJobInfoUpdateRequest implements GridHadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    @GridToStringInclude
    private GridHadoopJobId jobId;

    /** Job phase. */
    @GridToStringInclude
    private GridHadoopJobPhase jobPhase;

    /** Reducers addresses. */
    @GridToStringInclude
    private GridHadoopProcessDescriptor[] reducersAddrs;

    /**
     * Constructor required by {@link Externalizable}.
     */
    public GridHadoopJobInfoUpdateRequest() {
        // No-op.
    }

    /**
     * @param jobId Job ID.
     * @param jobPhase Job phase.
     * @param reducersAddrs Reducers addresses.
     */
    public GridHadoopJobInfoUpdateRequest(GridHadoopJobId jobId, GridHadoopJobPhase jobPhase,
        GridHadoopProcessDescriptor[] reducersAddrs) {
        assert jobId != null;

        this.jobId = jobId;
        this.jobPhase = jobPhase;
        this.reducersAddrs = reducersAddrs;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Job phase.
     */
    public GridHadoopJobPhase jobPhase() {
        return jobPhase;
    }

    /**
     * @return Reducers addresses.
     */
    public GridHadoopProcessDescriptor[] reducersAddresses() {
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
        jobId = new GridHadoopJobId();
        jobId.readExternal(in);

        jobPhase = (GridHadoopJobPhase)in.readObject();
        reducersAddrs = (GridHadoopProcessDescriptor[])U.readArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopJobInfoUpdateRequest.class, this);
    }
}
