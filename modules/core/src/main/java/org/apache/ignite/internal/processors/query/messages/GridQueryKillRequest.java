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

package org.apache.ignite.internal.processors.query.messages;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Query kill request.
 */
public class GridQueryKillRequest implements Message {
    /** */
    public static final short TYPE_CODE = 172;

    /** */
    private static final long serialVersionUID = 0L;

    /** Request id. */
    private long reqId;

    /** Query id on a node. */
    private long nodeQryId;

    /** Async response flag. */
    private boolean asyncRes;

    /**
     * Default constructor.
     */
    public GridQueryKillRequest() {
        // No-op.
    }

    /**
     * @param reqId Request id.
     * @param nodeQryId Query ID on a node.
     * @param asyncRes {@code true} in case reposnse should be send asynchronous.
     */
    public GridQueryKillRequest(long reqId, long nodeQryId, boolean asyncRes) {
        this.reqId = reqId;
        this.nodeQryId = nodeQryId;
        this.asyncRes = asyncRes;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @return Query id on a node.
     */
    public long nodeQryId() {
        return nodeQryId;
    }

    /**
     * @return {@code true} in case response should be send back asynchronous.
     */
    public boolean asyncResponse() {
        return asyncRes;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
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
                if (!writer.writeBoolean("asyncRes", asyncRes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("nodeQryId", nodeQryId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("reqId", reqId))
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
                asyncRes = reader.readBoolean("asyncRes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                nodeQryId = reader.readLong("nodeQryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                reqId = reader.readLong("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridQueryKillRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridQueryKillRequest.class, this);
    }
}
