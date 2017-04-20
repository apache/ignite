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
package org.apache.ignite.internal.processors.marshaller;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * On receiving a {@link MissingMappingRequestMessage} mapping request server node looks up class name
 * for requested platformId and typeId in its local marshaller cache and sends back
 * a {@link MissingMappingResponseMessage} mapping response with resolved class name.
 */
public class MissingMappingResponseMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private byte platformId;

    /** */
    private int typeId;

    /** */
    private String clsName;

    /**
     * Default constructor.
     */
    public MissingMappingResponseMessage() {
    }

    /**
     * @param platformId Platform id.
     * @param typeId Type id.
     * @param clsName Class name.
     */
    MissingMappingResponseMessage(byte platformId, int typeId, String clsName) {
        this.platformId = platformId;
        this.typeId = typeId;
        this.clsName = clsName;
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
                if (!writer.writeString("clsName", clsName))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte("platformId", platformId))
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
                clsName = reader.readString("clsName");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                platformId = reader.readByte("platformId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                typeId = reader.readInt("typeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(MissingMappingResponseMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 79;
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
     *
     */
    public byte platformId() {
        return platformId;
    }

    /**
     *
     */
    public int typeId() {
        return typeId;
    }

    /**
     *
     */
    public String className() {
        return clsName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MissingMappingResponseMessage.class, this);
    }
}
