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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class PartitionsExchangeFinishedCheckResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private boolean receivedSingleMsg;

    /**
     *
     */
    public PartitionsExchangeFinishedCheckResponse() {
    }

    /**
     * @param topVer Last finished exchange version.
     * @param receivedSingleMsg Received single message flag.
     */
    public PartitionsExchangeFinishedCheckResponse(AffinityTopologyVersion topVer, boolean receivedSingleMsg) {
        this.topVer = topVer;
        this.receivedSingleMsg = receivedSingleMsg;
    }

    /**
     * @return Last finished exchange version.
     */
    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    /**
     * @return {@code True} if during processing last finished exchange single message from requester was received.
     */
    public boolean receivedSingleMessage() {
        return receivedSingleMsg;
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
                if (!writer.writeBoolean("receivedSingleMsg", receivedSingleMsg))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMessage("topVer", topVer))
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
                receivedSingleMsg = reader.readBoolean("receivedSingleMsg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(PartitionsExchangeFinishedCheckResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 137;
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
