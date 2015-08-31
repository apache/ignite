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

package org.apache.ignite.internal.processors.hadoop;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Hadoop job status.
 */
public class HadoopJobStatus implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job ID. */
    private HadoopJobId jobId;

    /** Job name. */
    private String jobName;

    /** User. */
    private String usr;

    /** Pending mappers count. */
    private int pendingMapperCnt;

    /** Pending reducers count. */
    private int pendingReducerCnt;

    /** Total mappers count. */
    private int totalMapperCnt;

    /** Total reducers count. */
    private int totalReducerCnt;
    /** Phase. */
    private HadoopJobPhase jobPhase;

    /** */
    private boolean failed;

    /** Version. */
    private long ver;

    /**
     * {@link Externalizable} support.
     */
    public HadoopJobStatus() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param jobId Job ID.
     * @param jobName Job name.
     * @param usr User.
     * @param pendingMapperCnt Pending mappers count.
     * @param pendingReducerCnt Pending reducers count.
     * @param totalMapperCnt Total mappers count.
     * @param totalReducerCnt Total reducers count.
     * @param jobPhase Job phase.
     * @param failed Failed.
     * @param ver Version.
     */
    public HadoopJobStatus(
        HadoopJobId jobId,
        String jobName,
        String usr,
        int pendingMapperCnt,
        int pendingReducerCnt,
        int totalMapperCnt,
        int totalReducerCnt,
        HadoopJobPhase jobPhase,
        boolean failed,
        long ver
    ) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.usr = usr;
        this.pendingMapperCnt = pendingMapperCnt;
        this.pendingReducerCnt = pendingReducerCnt;
        this.totalMapperCnt = totalMapperCnt;
        this.totalReducerCnt = totalReducerCnt;
        this.jobPhase = jobPhase;
        this.failed = failed;
        this.ver = ver;
    }

    /**
     * @return Job ID.
     */
    public HadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Job name.
     */
    public String jobName() {
        return jobName;
    }

    /**
     * @return User.
     */
    public String user() {
        return usr;
    }

    /**
     * @return Pending mappers count.
     */
    public int pendingMapperCnt() {
        return pendingMapperCnt;
    }

    /**
     * @return Pending reducers count.
     */
    public int pendingReducerCnt() {
        return pendingReducerCnt;
    }

    /**
     * @return Total mappers count.
     */
    public int totalMapperCnt() {
        return totalMapperCnt;
    }

    /**
     * @return Total reducers count.
     */
    public int totalReducerCnt() {
        return totalReducerCnt;
    }

    /**
     * @return Version.
     */
    public long version() {
        return ver;
    }

    /**
     * @return Job phase.
     */
    public HadoopJobPhase jobPhase() {
        return jobPhase;
    }

    /**
     * @return {@code true} If the job failed.
     */
    public boolean isFailed() {
        return failed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopJobStatus.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(jobId);
        U.writeString(out, jobName);
        U.writeString(out, usr);
        out.writeInt(pendingMapperCnt);
        out.writeInt(pendingReducerCnt);
        out.writeInt(totalMapperCnt);
        out.writeInt(totalReducerCnt);
        out.writeObject(jobPhase);
        out.writeBoolean(failed);
        out.writeLong(ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (HadoopJobId)in.readObject();
        jobName = U.readString(in);
        usr = U.readString(in);
        pendingMapperCnt = in.readInt();
        pendingReducerCnt = in.readInt();
        totalMapperCnt = in.readInt();
        totalReducerCnt = in.readInt();
        jobPhase = (HadoopJobPhase)in.readObject();
        failed = in.readBoolean();
        ver = in.readLong();
    }
}