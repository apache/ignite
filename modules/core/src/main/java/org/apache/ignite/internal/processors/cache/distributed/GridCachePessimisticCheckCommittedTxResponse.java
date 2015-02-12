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
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridCachePessimisticCheckCommittedTxResponse _clone = new GridCachePessimisticCheckCommittedTxResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridCachePessimisticCheckCommittedTxResponse _clone = (GridCachePessimisticCheckCommittedTxResponse)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.committedTxInfo = committedTxInfo;
        _clone.committedTxInfoBytes = committedTxInfoBytes;
        _clone.sys = sys;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
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
            case 8:
                if (!writer.writeByteArray("committedTxInfoBytes", committedTxInfoBytes))
                    return false;

                state++;

            case 9:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                state++;

            case 10:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                state++;

            case 11:
                if (!writer.writeBoolean("sys", sys))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (state) {
            case 8:
                committedTxInfoBytes = reader.readByteArray("committedTxInfoBytes");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 9:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 10:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 11:
                sys = reader.readBoolean("sys");

                if (!reader.isLastRead())
                    return false;

                state++;

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
