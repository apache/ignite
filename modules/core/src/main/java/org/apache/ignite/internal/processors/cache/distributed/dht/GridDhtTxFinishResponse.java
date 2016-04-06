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

import java.io.Externalizable;
import java.nio.ByteBuffer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * DHT transaction finish response.
 */
public class GridDhtTxFinishResponse extends GridDistributedTxFinishResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Error. */
    @GridDirectTransient
    private Throwable checkCommittedErr;

    /** Serialized error. */
    private byte[] checkCommittedErrBytes;

    /** Flag indicating if this is a check-committed response. */
    private boolean checkCommitted;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtTxFinishResponse() {
        // No-op.
    }

    /**
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     */
    public GridDhtTxFinishResponse(GridCacheVersion xid, IgniteUuid futId, IgniteUuid miniId) {
        super(xid, futId);

        assert miniId != null;

        this.miniId = miniId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return Error for check committed backup requests.
     */
    public Throwable checkCommittedError() {
        return checkCommittedErr;
    }

    /**
     * @param checkCommittedErr Error for check committed backup requests.
     */
    public void checkCommittedError(Throwable checkCommittedErr) {
        this.checkCommittedErr = checkCommittedErr;
    }

    /**
     * @return Check committed flag.
     */
    public boolean checkCommitted() {
        return checkCommitted;
    }

    /**
     * @param checkCommitted Check committed flag.
     */
    public void checkCommitted(boolean checkCommitted) {
        this.checkCommitted = checkCommitted;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (checkCommittedErr != null && checkCommittedErrBytes == null)
            checkCommittedErrBytes = ctx.marshaller().marshal(checkCommittedErr);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr)
        throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (checkCommittedErrBytes != null && checkCommittedErr == null)
            checkCommittedErr = ctx.marshaller().unmarshal(checkCommittedErrBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxFinishResponse.class, this, super.toString());
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
            case 5:
                if (!writer.writeBoolean("checkCommitted", checkCommitted))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByteArray("checkCommittedErrBytes", checkCommittedErrBytes))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeIgniteUuid("miniId", miniId))
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
            case 5:
                checkCommitted = reader.readBoolean("checkCommitted");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                checkCommittedErrBytes = reader.readByteArray("checkCommittedErrBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxFinishResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 33;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 8;
    }
}
