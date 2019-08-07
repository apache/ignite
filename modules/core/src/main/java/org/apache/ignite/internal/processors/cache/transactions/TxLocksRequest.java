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

package org.apache.ignite.internal.processors.cache.transactions;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Transactions lock list request.
 */
public class TxLocksRequest extends GridCacheMessage {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private long futId;

    /** Tx keys. */
    @GridToStringInclude
    @GridDirectTransient
    private Set<IgniteTxKey> txKeys;

    /** Array of txKeys from {@link #txKeys}. Used during marshalling and unmarshalling. */
    @GridToStringExclude
    private IgniteTxKey[] txKeysArr;

    /**
     * Default constructor.
     */
    public TxLocksRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param txKeys Target tx keys.
     */
    public TxLocksRequest(long futId, Set<IgniteTxKey> txKeys) {
        A.notEmpty(txKeys, "txKeys");

        this.futId = futId;
        this.txKeys = txKeys;
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean cacheGroupMessage() {
        return false;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @return Tx keys.
     */
    public Collection<IgniteTxKey> txKeys() {
        return txKeys;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxLocksRequest.class, this);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        txKeysArr = new IgniteTxKey[txKeys.size()];

        int i = 0;

        for (IgniteTxKey key : txKeys) {
            key.prepareMarshal(ctx.cacheContext(key.cacheId()));

            txKeysArr[i++] = key;
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        txKeys = U.newHashSet(txKeysArr.length);

        for (IgniteTxKey key : txKeysArr) {
            key.finishUnmarshal(ctx.cacheContext(key.cacheId()), ldr);

            txKeys.add(key);
        }

        txKeysArr = null;
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
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeObjectArray("txKeysArr", txKeysArr, MessageCollectionItemType.MSG))
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
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                txKeysArr = reader.readObjectArray("txKeysArr", MessageCollectionItemType.MSG, IgniteTxKey.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(TxLocksRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -24;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
