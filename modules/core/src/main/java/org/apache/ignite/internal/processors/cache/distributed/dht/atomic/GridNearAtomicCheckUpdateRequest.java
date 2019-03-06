/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridNearAtomicCheckUpdateRequest extends GridCacheIdMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** */
    @GridDirectTransient
    private GridNearAtomicAbstractUpdateRequest updateReq;

    /** */
    private int partId;

    /** */
    private long futId;

    /**
     *
     */
    public GridNearAtomicCheckUpdateRequest() {
        // No-op.
    }

    /**
     * @param updateReq Related update request.
     */
    GridNearAtomicCheckUpdateRequest(GridNearAtomicAbstractUpdateRequest updateReq) {
        assert updateReq != null && updateReq.fullSync() : updateReq;

        this.updateReq = updateReq;
        this.cacheId = updateReq.cacheId();
        this.partId = updateReq.partition();
        this.futId = updateReq.futureId();

        assert partId >= 0;
    }

    /**
     * @return Future ID on near node.
     */
    public final long futureId() {
        return futId;
    }

    /**
     * @return Related update request.
     */
    GridNearAtomicAbstractUpdateRequest updateRequest() {
        return updateReq;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -50;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 4:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeInt("partId", partId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 4:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                partId = reader.readInt("partId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearAtomicCheckUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicCheckUpdateRequest.class, this);
    }
}
