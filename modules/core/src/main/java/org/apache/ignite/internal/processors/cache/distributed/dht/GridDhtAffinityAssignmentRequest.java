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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;

/**
 * Affinity assignment request.
 */
public class GridDhtAffinityAssignmentRequest<K, V> extends GridCacheMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version being queried. */
    private long topVer;

    /**
     * Empty constructor.
     */
    public GridDhtAffinityAssignmentRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     */
    public GridDhtAffinityAssignmentRequest(int cacheId, long topVer) {
        this.cacheId = cacheId;
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Requested topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 28;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtAffinityAssignmentRequest _clone = (GridDhtAffinityAssignmentRequest)_msg;

        _clone.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 3:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityAssignmentRequest.class, this);
    }
}
