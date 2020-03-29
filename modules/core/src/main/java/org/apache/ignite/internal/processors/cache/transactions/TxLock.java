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
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Corresponds to one {@link GridCacheMvccCandidate} from local MVCC candidates queue.
 * There is one exclusion: {@link TxLock} instance with {@link #OWNERSHIP_REQUESTED} corresponds to lock request
 * to remote node from near node that isn't primary node for key.
 */
public class TxLock implements Message {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Ownership owner. */
    static final byte OWNERSHIP_OWNER = 1;

    /** Ownership candidate. */
    static final byte OWNERSHIP_CANDIDATE = 2;

    /** Ownership requested. */
    static final byte OWNERSHIP_REQUESTED = 3;

    /** Near node ID. */
    private UUID nearNodeId;

    /** Tx ID. */
    private GridCacheVersion txId;

    /** Thread ID. */
    private long threadId;

    /** Ownership. */
    private byte ownership;

    /**
     * Default constructor.
     */
    public TxLock() {
        // No-op.
    }

    /**
     * @param txId Tx ID.
     * @param nearNodeId Near node ID.
     * @param threadId Thread ID.
     * @param ownership Ownership.
     */
    public TxLock(GridCacheVersion txId, UUID nearNodeId, long threadId, byte ownership) {
        this.txId = txId;
        this.nearNodeId = nearNodeId;
        this.threadId = threadId;
        this.ownership = ownership;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @return Transaction ID.
     */
    public GridCacheVersion txId() {
        return txId;
    }

    /**
     * @return Thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return {@code True} if transaction hold lock on the key, otherwise {@code false}.
     */
    public boolean owner() {
        return ownership == OWNERSHIP_OWNER;
    }

    /**
     * @return {@code True} if there is MVCC candidate for this transaction and key, otherwise {@code false}.
     */
    public boolean candiate() {
        return ownership == OWNERSHIP_CANDIDATE;
    }

    /**
     * @return {@code True} if transaction requested lock for key from primary remote node
     * but response isn't received because other transaction hold lock on the key.
     */
    public boolean requested() {
        return ownership == OWNERSHIP_REQUESTED;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxLock.class, this);
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
                if (!writer.writeUuid("nearNodeId", nearNodeId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte("ownership", ownership))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("threadId", threadId))
                    return false;

                writer.incrementState();

            case 3:
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
                nearNodeId = reader.readUuid("nearNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                ownership = reader.readByte("ownership");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                threadId = reader.readLong("threadId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                txId = reader.readMessage("txId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(TxLock.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -25;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
