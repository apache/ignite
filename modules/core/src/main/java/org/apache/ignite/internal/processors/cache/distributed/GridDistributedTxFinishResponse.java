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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;

/**
 * Transaction finish response.
 */
public class GridDistributedTxFinishResponse<K, V> extends GridCacheMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCacheVersion txId;

    /** Future ID. */
    private IgniteUuid futId;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDistributedTxFinishResponse() {
        /* No-op. */
    }

    /**
     * @param txId Transaction id.
     * @param futId Future ID.
     */
    public GridDistributedTxFinishResponse(GridCacheVersion txId, IgniteUuid futId) {
        assert txId != null;
        assert futId != null;

        this.txId = txId;
        this.futId = futId;
    }

    /**
     *
     * @return Transaction id.
     */
    public GridCacheVersion xid() {
        return txId;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors",
        "OverriddenMethodCallDuringObjectConstruction"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridDistributedTxFinishResponse _clone = (GridDistributedTxFinishResponse)_msg;

        _clone.txId = txId != null ? (GridCacheVersion)txId.clone() : null;
        _clone.futId = futId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriteState state) {
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!super.writeTo(buf, state))
            return false;

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 3:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                state.increment();

            case 4:
                if (!writer.writeMessage("txId", txId))
                    return false;

                state.increment();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 3:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                txId = reader.readMessage("txId");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 24;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxFinishResponse.class, this);
    }
}
