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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridDhtTxNearPrepareResponse extends GridCacheMessage implements IgniteTxStateAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int partId;

    /** */
    private GridCacheVersion nearTxId;

    /** Future ID.  */
    private IgniteUuid futId;

    /** Mini future ID. */
    private int miniId;

    /** Transient TX state. */
    @GridDirectTransient
    private IgniteTxState txState;

    /**
     *
     */
    public GridDhtTxNearPrepareResponse() {
        // No-op.
    }

    /**
     * @param partId Partition ID.
     * @param nearTxId Near transaction ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     */
    public GridDhtTxNearPrepareResponse(int partId, GridCacheVersion nearTxId, IgniteUuid futId, int miniId) {
        assert nearTxId != null;
        assert futId != null;
        assert miniId > 0;

        this.partId = partId;
        this.nearTxId = nearTxId;
        this.futId = futId;
        this.miniId = miniId;
    }

    /**
     * @return Near transaction ID.
     */
    public GridCacheVersion nearTxId() {
        return nearTxId;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxState txState() {
        return txState;
    }

    /** {@inheritDoc} */
    @Override public void txState(IgniteTxState txState) {
        this.txState = txState;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.txPrepareMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -50;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
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
            case 3:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("nearTxId", nearTxId))
                    return false;

                writer.incrementState();

            case 6:
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
            case 3:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                nearTxId = reader.readMessage("nearTxId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                partId = reader.readInt("partId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxNearPrepareResponse.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxNearPrepareResponse.class, this);
    }
}
