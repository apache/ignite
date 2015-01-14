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

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

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
        return 80;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtAffinityAssignmentResponse _clone = new GridDhtAffinityAssignmentResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
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
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 3:
                if (!commState.putByteArray(affAssignmentBytes))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putLong(topVer))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 3:
                byte[] affAssignmentBytes0 = commState.getByteArray();

                if (affAssignmentBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                affAssignmentBytes = affAssignmentBytes0;

                commState.idx++;

            case 4:
                if (buf.remaining() < 8)
                    return false;

                topVer = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityAssignmentResponse.class, this);
    }
}
