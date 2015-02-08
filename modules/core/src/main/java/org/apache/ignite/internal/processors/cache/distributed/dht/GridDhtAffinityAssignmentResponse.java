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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;
import java.util.*;

/**
 * Affinity assignment response.
 */
public class GridDhtAffinityAssignmentResponse<K, V> extends GridCacheMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private long topVer;

    /** Affinity assignment. */
    @GridDirectTransient
    @GridToStringInclude
    private List<List<ClusterNode>> affAssignment;

    /** Affinity assignment bytes. */
    private byte[] affAssignmentBytes;

    /**
     * Empty constructor.
     */
    public GridDhtAffinityAssignmentResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment.
     */
    public GridDhtAffinityAssignmentResponse(int cacheId, long topVer, List<List<ClusterNode>> affAssignment) {
        this.cacheId = cacheId;
        this.topVer = topVer;
        this.affAssignment = affAssignment;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> affinityAssignment() {
        return affAssignment;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 29;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridDhtAffinityAssignmentResponse _clone = new GridDhtAffinityAssignmentResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtAffinityAssignmentResponse _clone = (GridDhtAffinityAssignmentResponse)_msg;

        _clone.topVer = topVer;
        _clone.affAssignment = affAssignment;
        _clone.affAssignmentBytes = affAssignmentBytes;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (affAssignment != null)
            affAssignmentBytes = ctx.marshaller().marshal(affAssignment);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (affAssignmentBytes != null)
            affAssignment = ctx.marshaller().unmarshal(affAssignmentBytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 3:
                if (!writer.writeByteArray("affAssignmentBytes", affAssignmentBytes))
                    return false;

                state++;

            case 4:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (state) {
            case 3:
                affAssignmentBytes = reader.readByteArray("affAssignmentBytes");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 4:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityAssignmentResponse.class, this);
    }
}
