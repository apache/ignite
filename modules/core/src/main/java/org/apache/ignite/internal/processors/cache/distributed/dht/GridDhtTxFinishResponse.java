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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
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

    /** Flag indicating if this is a check-committed response. */
    private static final int CHECK_COMMITTED_FLAG_MASK = 0x01;

    /** Mini future ID. */
    private int miniId;

    /** Error. */
    @GridDirectTransient
    private Throwable checkCommittedErr;

    /** Serialized error. */
    private byte[] checkCommittedErrBytes;

    /** Cache return value. */
    private GridCacheReturn retVal;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtTxFinishResponse() {
        // No-op.
    }

    /**
     * @param part Partition.
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     */
    public GridDhtTxFinishResponse(int part, GridCacheVersion xid, IgniteUuid futId, int miniId) {
        super(part, xid, futId);

        assert miniId != 0;

        this.miniId = miniId;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
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
        return isFlag(CHECK_COMMITTED_FLAG_MASK);
    }

    /**
     * @param checkCommitted Check committed flag.
     */
    public void checkCommitted(boolean checkCommitted) {
        setFlag(checkCommitted, CHECK_COMMITTED_FLAG_MASK);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (checkCommittedErr != null && checkCommittedErrBytes == null)
            checkCommittedErrBytes = U.marshal(ctx, checkCommittedErr);

        if (retVal != null && retVal.cacheId() != 0) {
            GridCacheContext cctx = ctx.cacheContext(retVal.cacheId());

            assert cctx != null : retVal.cacheId();

            retVal.prepareMarshal(cctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr)
        throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (checkCommittedErrBytes != null && checkCommittedErr == null)
            checkCommittedErr = U.unmarshal(ctx, checkCommittedErrBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        if (retVal != null && retVal.cacheId() != 0) {
            GridCacheContext cctx = ctx.cacheContext(retVal.cacheId());

            assert cctx != null : retVal.cacheId();

            retVal.finishUnmarshal(cctx, ldr);
        }
    }

    /**
     * @param retVal Return value.
     */
    public void returnValue(GridCacheReturn retVal) {
        this.retVal = retVal;
    }

    /**
     * @return Return value.
     */
    public GridCacheReturn returnValue() {
        return retVal;
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
            case 7:
                if (!writer.writeByteArray("checkCommittedErrBytes", checkCommittedErrBytes))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeMessage("retVal", retVal))
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
            case 7:
                checkCommittedErrBytes = reader.readByteArray("checkCommittedErrBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                retVal = reader.readMessage("retVal");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxFinishResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 33;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder flags = new StringBuilder();

        if (checkCommitted())
            appendFlag(flags, "checkComm");

        return S.toString(GridDhtTxFinishResponse.class, this,
            "flags", flags.toString(),
            "super", super.toString());
    }
}
