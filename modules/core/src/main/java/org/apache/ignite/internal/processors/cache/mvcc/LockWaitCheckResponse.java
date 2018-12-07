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

public class LockWaitCheckResponse implements MvccMessage {
    private static final long serialVersionUID = 0;

    private IgniteUuid futId;
    private GridCacheVersion blockerTxVersion;
    private UUID blockerNodeId;

    public static LockWaitCheckResponse waiting(
        IgniteUuid futId, UUID blockerNodeId, GridCacheVersion blockerTxVersion) {
        return new LockWaitCheckResponse(futId, blockerNodeId, blockerTxVersion);
    }

    public static LockWaitCheckResponse notWaiting(IgniteUuid futId) {
        return new LockWaitCheckResponse(futId, null, null);
    }

    public LockWaitCheckResponse() {
    }

    private LockWaitCheckResponse(IgniteUuid futId, UUID blockerNodeId, GridCacheVersion blockerTxVersion) {
        this.futId = futId;
        this.blockerTxVersion = blockerTxVersion;
        this.blockerNodeId = blockerNodeId;
    }

    public IgniteUuid futId() {
        return futId;
    }

    public GridCacheVersion blockerTxVersion() {
        return blockerTxVersion;
    }

    public UUID blockerNodeId() {
        return blockerNodeId;
    }

    public boolean isWaiting() {
        return blockerTxVersion != null;
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
                if (!writer.writeUuid("blockerNodeId", blockerNodeId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("blockerTxVersion", blockerTxVersion))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeIgniteUuid("futId", futId))
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
                blockerNodeId = reader.readUuid("blockerNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                blockerTxVersion = reader.readMessage("blockerTxVersion");

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

    @Override public short directType() {
        return 169;
    }

    @Override public byte fieldsCount() {
        return 3;
    }

    @Override public void onAckReceived() {

    }

    @Override public boolean waitForCoordinatorInit() {
        return false;
    }

    @Override public boolean processedFromNioThread() {
        return false;
    }
}
