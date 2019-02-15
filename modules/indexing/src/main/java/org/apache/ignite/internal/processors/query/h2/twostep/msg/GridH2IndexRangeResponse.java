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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Range response message.
 */
@IgniteCodeGeneratingFail
public class GridH2IndexRangeResponse implements Message {
    /** */
    public static final byte STATUS_OK = 0;

    /** */
    public static final byte STATUS_ERROR = 1;

    /** */
    public static final byte STATUS_NOT_FOUND = 2;

    /** */
    private UUID originNodeId;

    /** */
    private long qryId;

    /** */
    private int segmentId;

    /** */
    private int originSegmentId;

    /** */
    private int batchLookupId;

    /** */
    @GridDirectCollection(Message.class)
    private List<GridH2RowRange> ranges;

    /** */
    private byte status;

    /** */
    private String err;

    /**
     * @param ranges Ranges.
     */
    public void ranges(List<GridH2RowRange> ranges) {
        this.ranges = ranges;
    }

    /**
     * @return Ranges.
     */
    public List<GridH2RowRange> ranges() {
        return ranges;
    }

    /**
     * @return Origin node ID.
     */
    public UUID originNodeId() {
        return originNodeId;
    }

    /**
     * @param originNodeId Origin node ID.
     */
    public void originNodeId(UUID originNodeId) {
        this.originNodeId = originNodeId;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return qryId;
    }

    /**
     * @param qryId Query ID.
     */
    public void queryId(long qryId) {
        this.qryId = qryId;
    }

    /**
     * @param err Error message.
     */
    public void error(String err) {
        this.err = err;
    }

    /**
     * @return Error message or {@code null} if everything is ok.
     */
    public String error() {
        return err;
    }

    /**
     * @param status Status.
     */
    public void status(byte status) {
        this.status = status;
    }

    /**
     * @return Status.
     */
    public byte status() {
        return status;
    }

    /**
     * @param segmentId Index segment ID.
     */
    public void segment(int segmentId) {
        this.segmentId = segmentId;
    }

    /**
     * @return Index segment ID.
     */
    public int segment() {
        return segmentId;
    }

    /**
     * @return  Origin index segment ID.
     */
    public int originSegmentId() {
        return originSegmentId;
    }

    /**
     * @param segmentId Origin index segment ID.
     */
    public void originSegmentId(int segmentId) {
        this.originSegmentId = segmentId;
    }

    /**
     * @param batchLookupId Batch lookup ID.
     */
    public void batchLookupId(int batchLookupId) {
        this.batchLookupId = batchLookupId;
    }

    /**
     * @return Batch lookup ID.
     */
    public int batchLookupId() {
        return batchLookupId;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt("batchLookupId", batchLookupId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("err", err))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid("originNodeId", originNodeId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong("qryId", qryId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection("ranges", ranges, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByte("status", status))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeInt("originSegId", originSegmentId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeInt("segmentId", segmentId))
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

        switch (reader.state()) {
            case 0:
                batchLookupId = reader.readInt("batchLookupId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                err = reader.readString("err");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                originNodeId = reader.readUuid("originNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                qryId = reader.readLong("qryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                ranges = reader.readCollection("ranges", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                status = reader.readByte("status");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                originSegmentId = reader.readInt("originSegId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                segmentId = reader.readInt("segmentId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridH2IndexRangeResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -31;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2IndexRangeResponse.class, this, "rangesSize", ranges == null ? null : ranges.size());
    }
}
