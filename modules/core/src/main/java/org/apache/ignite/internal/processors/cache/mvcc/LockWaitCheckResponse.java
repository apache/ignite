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
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Response with an information what transaction is blocking a transaction from corresponding request.
 * @see LockWaitCheckRequest
 */
public class LockWaitCheckResponse implements MvccMessage {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private IgniteUuid futId;
    /** */
    private GridCacheVersion blockerTxVer;
    /** */
    private UUID blockerNodeId;

    /** */
    public static LockWaitCheckResponse waiting(
        IgniteUuid futId, UUID blockerNodeId, GridCacheVersion blockerTxVer) {
        return new LockWaitCheckResponse(futId, blockerNodeId, blockerTxVer);
    }

    /** */
    public static LockWaitCheckResponse notWaiting(IgniteUuid futId) {
        return new LockWaitCheckResponse(futId, null, null);
    }

    /** */
    public LockWaitCheckResponse() {
    }

    /** */
    private LockWaitCheckResponse(IgniteUuid futId, UUID blockerNodeId, GridCacheVersion blockerTxVer) {
        this.futId = futId;
        this.blockerTxVer = blockerTxVer;
        this.blockerNodeId = blockerNodeId;
    }

    /**
     * @return Id of a future waiting for a response.
     */
    public IgniteUuid futId() {
        return futId;
    }

    /**
     * @return Identifier of a transaction which is determined to be blocking a transaction
     * from a corresponding request.
     */
    public GridCacheVersion blockerTxVersion() {
        return blockerTxVer;
    }

    /**
     * @return Blocking transaction near node id.
     */
    public UUID blockerNodeId() {
        return blockerNodeId;
    }

    /**
     * @return {@code true} if a transaction from a corresponding request is waiting for another transaction.
     */
    public boolean isWaiting() {
        return blockerTxVer != null;
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
                if (!writer.writeUuid("blockerNodeId", blockerNodeId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("blockerTxVer", blockerTxVer))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeIgniteUuid("futId", futId))
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
                blockerNodeId = reader.readUuid("blockerNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                blockerTxVer = reader.readMessage("blockerTxVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(LockWaitCheckResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 169;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {

    }

    /** {@inheritDoc} */
    @Override public boolean waitForCoordinatorInit() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean processedFromNioThread() {
        return false;
    }
}
