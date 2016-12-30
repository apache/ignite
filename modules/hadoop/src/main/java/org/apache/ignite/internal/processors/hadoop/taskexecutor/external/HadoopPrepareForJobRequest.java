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
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Child process initialization request.
 */
public class HadoopPrepareForJobRequest implements HadoopMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    @GridToStringInclude
    private HadoopJobId jobId;

    /** Job info. */
    @GridToStringInclude
    private HadoopJobInfo jobInfo;

    /** Total amount of reducers in the job. */
    @GridToStringInclude
    private int totalReducersCnt;

    /** Reducers to be executed on current node. */
    @GridToStringInclude
    private int[] locReducers;

    /**
     * Constructor required by {@link Externalizable}.
     */
    public HadoopPrepareForJobRequest() {
        // No-op.
    }

    /**
     * @param jobId Job ID.
     * @param jobInfo Job info.
     * @param totalReducersCnt Number of reducers in the job.
     * @param locReducers Reducers to be executed on current node.
     */
    public HadoopPrepareForJobRequest(HadoopJobId jobId, HadoopJobInfo jobInfo, int totalReducersCnt,
        int[] locReducers) {
        assert jobId != null;

        this.jobId = jobId;
        this.jobInfo = jobInfo;
        this.totalReducersCnt = totalReducersCnt;
        this.locReducers = locReducers;
    }

    /**
     * @return Job info.
     */
    public HadoopJobInfo jobInfo() {
        return jobInfo;
    }

    /**
     * @return Job ID.
     */
    public HadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Reducers to be executed on current node.
     */
    public int[] localReducers() {
        return locReducers;
    }

    /**
     * @return Number of reducers in job.
     */
    public int totalReducerCount() {
        return totalReducersCnt;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        jobId.writeExternal(out);

        out.writeObject(jobInfo);
        out.writeInt(totalReducersCnt);

        U.writeIntArray(out, locReducers);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = new HadoopJobId();
        jobId.readExternal(in);

        jobInfo = (HadoopJobInfo)in.readObject();
        totalReducersCnt = in.readInt();

        locReducers = U.readIntArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopPrepareForJobRequest.class, this);
    }
}