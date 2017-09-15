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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class CoordinatorTxAckRequest implements MvccCoordinatorMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int SKIP_RESPONSE_FLAG_MASK = 0x01;

    /** */
    private long futId;

    /** */
    private GridCacheVersion txId;

    /** */
    private byte flags;

    /**
     * Required by {@link GridIoMessageFactory}.
     */
    public CoordinatorTxAckRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param txId Transaction ID.
     */
    CoordinatorTxAckRequest(long futId, GridCacheVersion txId) {
        this.futId = futId;
        this.txId = txId;
    }

    /** {@inheritDoc} */
    @Override public boolean waitForCoordinatorInit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean processedFromNioThread() {
        return true;
    }

    /**
     * @return Future ID.
     */
    long futureId() {
        return futId;
    }

    /**
     * @return {@code True} if response message is not needed.
     */
    boolean skipResponse() {
        return (flags & SKIP_RESPONSE_FLAG_MASK) != 0;
    }

    /**
     * @param val {@code True} if response message is not needed.
     */
    void skipResponse(boolean val) {
        if (val)
            flags |= SKIP_RESPONSE_FLAG_MASK;
        else
            flags &= ~SKIP_RESPONSE_FLAG_MASK;
    }

    /**
     * @return Transaction ID.s
     */
    public GridCacheVersion txId() {
        return txId;
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
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("txId", txId))
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
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                txId = reader.readMessage("txId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CoordinatorTxAckRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 131;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CoordinatorTxAckRequest.class, this);
    }
}
