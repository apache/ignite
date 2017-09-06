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
package org.apache.ignite.internal.processors.cache.binary;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * As {@link DiscoveryCustomMessage} messages are delivered to client nodes asynchronously
 * it is possible that server nodes are allowed to send to clients some BinaryObjects clients don't have metadata for.
 *
 * When client detects obsolete metadata (by checking if current version of metadata has schemaId)
 * it requests up-to-date metadata using communication SPI.
 *
 * API to make a request is provided by {@link BinaryMetadataTransport#requestUpToDateMetadata(int)} method.
 */
public class MetadataRequestMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int typeId;

    /**
     * Default constructor.
     */
    public MetadataRequestMessage() {
        //No-op.
    }

    /**
     * @param typeId Type ID.
     */
    MetadataRequestMessage(int typeId) {
        this.typeId = typeId;
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
                if (!writer.writeInt("typeId", typeId))
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
                typeId = reader.readInt("typeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MetadataRequestMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 80;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        //No-op.
    }

    /** */
    public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataRequestMessage.class, this);
    }
}
