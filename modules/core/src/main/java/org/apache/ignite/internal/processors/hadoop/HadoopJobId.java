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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Job ID.
 */
public class HadoopJobId implements GridCacheInternal, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID nodeId;

    /** */
    private int jobId;

    /**
     * For {@link Externalizable}.
     */
    public HadoopJobId() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param jobId Job ID.
     */
    public HadoopJobId(UUID nodeId, int jobId) {
        this.nodeId = nodeId;
        this.jobId = jobId;
    }

    public UUID globalId() {
        return nodeId;
    }

    public int localId() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        out.writeInt(jobId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        jobId = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        HadoopJobId that = (HadoopJobId) o;

        if (jobId != that.jobId)
            return false;

        if (!nodeId.equals(that.nodeId))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * nodeId.hashCode() + jobId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return nodeId + "_" + jobId;
    }
}
