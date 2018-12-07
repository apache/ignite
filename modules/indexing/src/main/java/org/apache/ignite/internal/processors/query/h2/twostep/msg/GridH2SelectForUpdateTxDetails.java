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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * TX details holder for {@link GridH2QueryRequest}.
 */
public class GridH2SelectForUpdateTxDetails implements Message {
    /** */
    private static final long serialVersionUID = 8166491041528984454L;
    /** */
    private long threadId;

    /** */
    private IgniteUuid futId;

    /** */
    private int miniId;

    /** */
    private UUID subjId;

    /** */
    private GridCacheVersion lockVer;

    /** */
    private int taskNameHash;

    /** */
    private boolean clientFirst;

    /** */
    private long timeout;

    /**
     * Default constructor.
     */
    GridH2SelectForUpdateTxDetails() {
        // No-op.
    }

    /**
     * @param threadId Thread id.
     * @param futId Future id.
     * @param miniId Mini fture id.
     * @param subjId Subject id.
     * @param lockVer Lock version.
     * @param taskNameHash Task name hash.
     * @param clientFirst {@code True} if this is the first client request.
     * @param timeout Tx timeout.
     */
    public GridH2SelectForUpdateTxDetails(long threadId, IgniteUuid futId, int miniId, UUID subjId,
        GridCacheVersion lockVer, int taskNameHash, boolean clientFirst, long timeout) {
        this.threadId = threadId;
        this.futId = futId;
        this.miniId = miniId;
        this.subjId = subjId;
        this.lockVer = lockVer;
        this.taskNameHash = taskNameHash;
        this.clientFirst = clientFirst;
        this.timeout = timeout;
    }

    /**
     * @return Thread id.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return Future id.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini fture id.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @return Subject id.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Lock version.
     */
    public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return {@code True} if this is the first client request in transaction.
     */
    public boolean firstClientRequest() {
        return clientFirst;
    }

    /**
     * @return Tx timeout.
     */
    public long timeout() {
        return timeout;
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
                if (!writer.writeBoolean("clientFirst", clientFirst))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("lockVer", lockVer))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("threadId", threadId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong("timeout", timeout))
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
                clientFirst = reader.readBoolean("clientFirst");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                lockVer = reader.readMessage("lockVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                threadId = reader.readLong("threadId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2SelectForUpdateTxDetails.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -57;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
