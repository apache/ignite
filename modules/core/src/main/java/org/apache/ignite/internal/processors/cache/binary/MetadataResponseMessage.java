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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Carries latest version of metadata to client as a response for {@link MetadataRequestMessage}.
 */
public class MetadataResponseMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int typeId;

    /** */
    private byte[] binaryMetadataBytes;

    /** */
    private ClientResponseStatus status;

    /** */
    public MetadataResponseMessage() {
        // No-op.
    }

    /**
     * @param typeId Type id.
     */
    MetadataResponseMessage(int typeId) {
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

            case 1:
                if (!writer.writeInt("status", status.ordinal()))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeByteArray("binMetaBytes", binaryMetadataBytes))
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

            case 1:
                status = ClientResponseStatus.values()[reader.readInt("status")];

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                binaryMetadataBytes = reader.readByteArray("binMetaBytes");

                if (!reader.isLastRead())
                    return false;
        }

        return reader.afterMessageRead(MetadataResponseMessage.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 81;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.

    }

    /**
     * @param bytes Binary metadata bytes.
     */
    void binaryMetadataBytes(byte[] bytes) {
        if (bytes != null)
            status = ClientResponseStatus.METADATA_FOUND;
        else
            status = ClientResponseStatus.METADATA_NOT_FOUND;

        binaryMetadataBytes = bytes;
    }

    /**
     * Marks message if any exception happened during preparing response.
     */
    void markErrorOnRequest() {
        status = ClientResponseStatus.ERROR;
    }

    /**
     * @return Type ID.
     */
    int typeId() {
        return typeId;
    }

    /**
     * @return Marshalled BinaryMetadata.
     */
    byte[] binaryMetadataBytes() {
        return binaryMetadataBytes;
    }

    /**
     * @return {@code true} if metadata was not found on server node replied with the response.
     */
    boolean metadataNotFound() {
        return status == ClientResponseStatus.METADATA_NOT_FOUND;
    }

    /**
     * Response statuses enum.
     */
    private enum ClientResponseStatus {
        /** */
        METADATA_FOUND,

        /** */
        METADATA_NOT_FOUND,

        /** */
        ERROR
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataResponseMessage.class, this);
    }
}
