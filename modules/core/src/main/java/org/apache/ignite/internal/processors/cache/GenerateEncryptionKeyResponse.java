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

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Generate encryption key response.
 */
public class GenerateEncryptionKeyResponse implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Request message ID. */
    private IgniteUuid id;

    /** */
    @GridDirectMap(keyType = Integer.class, valueType = byte[].class)
    private Map<Integer, byte[]> encGrpKeys;

    /** */
    public GenerateEncryptionKeyResponse() {
    }

    /**
     * @param id Request id.
     * @param encGrpKeys Encryption keys.
     */
    public GenerateEncryptionKeyResponse(IgniteUuid id, Map<Integer, byte[]> encGrpKeys) {
        this.id = id;
        this.encGrpKeys = encGrpKeys;
    }

    /**
     * @return Request id.
     */
    public IgniteUuid requestId() {
        return id;
    }

    /**
     * @return Encrypted group keys.
     */
    public Map<Integer, byte[]> encGrpKeys() {
        return encGrpKeys;
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
                if (!writer.writeMap("encGrpKeys", encGrpKeys, MessageCollectionItemType.INT, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIgniteUuid("id", id))
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
                encGrpKeys = reader.readMap("encGrpKeys", MessageCollectionItemType.INT, MessageCollectionItemType.BYTE_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                id = reader.readIgniteUuid("id");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GenerateEncryptionKeyResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 159;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        //No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GenerateEncryptionKeyResponse.class, this);
    }
}
