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

package org.apache.ignite.internal.processors.cache.mvcc.msg;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

public class MvccRecoveryFinishedMessage implements MvccMessage {
    private static final long serialVersionUID = -505062368078979867L;

    private UUID nearNodeId;
    // t0d0 get rid
    @GridDirectMap(keyType = Long.class, valueType = Boolean.class)
    private Map<Long, Boolean> resolution;

    public MvccRecoveryFinishedMessage() {
    }

    public MvccRecoveryFinishedMessage(UUID nearNodeId, Map<Long, Boolean> resolution) {
        this.nearNodeId = nearNodeId;
        this.resolution = resolution;
    }

    public UUID nearNodeId() {
        return nearNodeId;
    }

    public Map<Long, Boolean> recoveryResolution() {
        return resolution;
    }

    @Override public boolean waitForCoordinatorInit() {
        return false;
    }

    @Override public boolean processedFromNioThread() {
        return false;
    }

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
                if (!writer.writeMap("resolution", resolution, MessageCollectionItemType.LONG, MessageCollectionItemType.BOOLEAN))
                    return false;

                writer.incrementState();

        }

        return true;
    }

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
                resolution = reader.readMap("resolution", MessageCollectionItemType.LONG, MessageCollectionItemType.BOOLEAN, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MvccRecoveryFinishedMessage.class);
    }

    @Override public short directType() {
        return 164;
    }

    @Override public byte fieldsCount() {
        return 2;
    }

    @Override public void onAckReceived() {

    }
}
