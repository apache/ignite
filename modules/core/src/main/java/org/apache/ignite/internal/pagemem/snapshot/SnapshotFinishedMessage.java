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
 *
 */

package org.apache.ignite.internal.pagemem.snapshot;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message indicating that snapshot has been finished on a single node.
 */
public class SnapshotFinishedMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long snapshotId;

    /** */
    private boolean success;

    /**
     *
     */
    public SnapshotFinishedMessage() {
    }

    /**
     * @param snapshotId Snapshot ID.
     */
    public SnapshotFinishedMessage(long snapshotId, boolean success) {
        this.snapshotId = snapshotId;
        this.success = success;
    }

    /**
     * @return Snapshot ID.
     */
    public long snapshotId() {
        return snapshotId;
    }

    public boolean success() {
        return success;
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
                if (!writer.writeLong("snapshotId", snapshotId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeBoolean("success", success))
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
                snapshotId = reader.readLong("snapshotId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                success = reader.readBoolean("success");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(SnapshotFinishedMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -45;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op
    }
}
