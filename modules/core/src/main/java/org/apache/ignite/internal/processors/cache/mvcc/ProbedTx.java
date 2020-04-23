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
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Contains attributes of tx visited during deadlock detection.
 */
public class ProbedTx implements Message {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private UUID nodeId;

    /** */
    private GridCacheVersion xidVer;

    /** */
    private GridCacheVersion nearXidVer;

    /** */
    private long startTime;

    /** */
    private int lockCntr;

    /** */
    public ProbedTx() {
    }

    /**
     * @param nodeId Node on which probed transaction runs.
     * @param xidVer Identifier of transaction.
     * @param nearXidVer Identifier of near transaction.
     * @param startTime Transaction start time.
     * @param lockCntr Number of locks acquired by probed transaction at a time of probe handling.
     */
    public ProbedTx(UUID nodeId, GridCacheVersion xidVer, GridCacheVersion nearXidVer, long startTime,
        int lockCntr) {
        this.nodeId = nodeId;
        this.xidVer = xidVer;
        this.nearXidVer = nearXidVer;
        this.startTime = startTime;
        this.lockCntr = lockCntr;
    }

    /**
     * @return Node on which probed transaction runs.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Identifier of transaction.
     */
    public GridCacheVersion xidVersion() {
        return xidVer;
    }

    /**
     * @return Identifier of near transaction.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Transaction start time.
     */
    public long startTime() {
        return startTime;
    }

    /**
     * @return Number of locks acquired by probed transaction at a time of probe handling.
     */
    public int lockCounter() {
        return lockCntr;
    }

    /**
     * Creates a copy of this instance with modified transaction start time.
     *
     * @param updStartTime New start time value.
     * @return Instance with updated start time.
     */
    public ProbedTx withStartTime(long updStartTime) {
        return new ProbedTx(
            nodeId,
            xidVer,
            nearXidVer,
            updStartTime,
            lockCntr
        );
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
                if (!writer.writeInt("lockCntr", lockCntr))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("nearXidVer", nearXidVer))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid("nodeId", nodeId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong("startTime", startTime))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("xidVer", xidVer))
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
                lockCntr = reader.readInt("lockCntr");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                nearXidVer = reader.readMessage("nearXidVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                nodeId = reader.readUuid("nodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                startTime = reader.readLong("startTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                xidVer = reader.readMessage("xidVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(ProbedTx.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 171;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ProbedTx.class, this);
    }
}
