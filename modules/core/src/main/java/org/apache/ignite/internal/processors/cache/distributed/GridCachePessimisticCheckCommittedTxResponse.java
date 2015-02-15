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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Check prepared transactions response.
 */
public class GridCachePessimisticCheckCommittedTxResponse<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Committed transaction info. */
    @GridDirectTransient
    private GridCacheCommittedTxInfo<K, V> committedTxInfo;

    /** Serialized transaction info. */
    private byte[] committedTxInfoBytes;

    /** System transaction flag. */
    private boolean sys;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCachePessimisticCheckCommittedTxResponse() {
        // No-op.
    }

    /**
     * @param txId Transaction ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param committedTxInfo Committed transaction info.
     * @param sys System transaction flag.
     */
    public GridCachePessimisticCheckCommittedTxResponse(GridCacheVersion txId, IgniteUuid futId, IgniteUuid miniId,
        @Nullable GridCacheCommittedTxInfo<K, V> committedTxInfo, boolean sys) {
        super(txId, 0);

        this.futId = futId;
        this.miniId = miniId;
        this.committedTxInfo = committedTxInfo;
        this.sys = sys;
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
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return {@code True} if all remote transactions were prepared.
     */
    public GridCacheCommittedTxInfo<K, V> committedTxInfo() {
        return committedTxInfo;
    }

    /**
     * @return System transaction flag.
     */
    public boolean system() {
        return sys;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (committedTxInfo != null) {
            marshalTx(committedTxInfo.recoveryWrites(), ctx);

            committedTxInfoBytes = ctx.marshaller().marshal(committedTxInfo);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (committedTxInfoBytes != null) {
            committedTxInfo = ctx.marshaller().unmarshal(committedTxInfoBytes, ldr);

            unmarshalTx(committedTxInfo.recoveryWrites(), false, ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeMessageType(directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeField("committedTxInfoBytes", committedTxInfoBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeField("futId", futId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeField("miniId", miniId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeField("sys", sys, MessageFieldType.BOOLEAN))
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
            case 8:
                committedTxInfoBytes = reader.readField("committedTxInfoBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                futId = reader.readField("futId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 10:
                miniId = reader.readField("miniId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                sys = reader.readField("sys", MessageFieldType.BOOLEAN);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 19;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePessimisticCheckCommittedTxResponse.class, this, "super", super.toString());
    }
}
