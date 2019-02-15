/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
                if (!writer.writeByteArray("binaryMetadataBytes", binaryMetadataBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte("status", status != null ? (byte)status.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 2:
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
                binaryMetadataBytes = reader.readByteArray("binaryMetadataBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                byte statusOrd;

                statusOrd = reader.readByte("status");

                if (!reader.isLastRead())
                    return false;

                status = ClientResponseStatus.fromOrdinal(statusOrd);

                reader.incrementState();

            case 2:
                typeId = reader.readInt("typeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MetadataResponseMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
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
        ERROR;

        /** Enumerated values. */
        private static final ClientResponseStatus[] VALS = values();

        /**
         * Efficiently gets enumerated value from its ordinal.
         *
         * @param ord Ordinal value.
         * @return Enumerated value.
         */
        public static ClientResponseStatus fromOrdinal(byte ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataResponseMessage.class, this);
    }
}
