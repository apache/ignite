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

package org.apache.ignite.internal.managers.communication;

import java.nio.ByteBuffer;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/** */
public class CompressedDataMessage<T> implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 517;

    /** */
    private T data;

    /** */
    private byte[] compressedData;

    /** */
    private byte[] chunk;

    /** */
    private boolean finalChunk;

    /** */
    public CompressedDataMessage() {
        // No-op.
    }

    /**
     * @param data Data to compress.
     */
    public CompressedDataMessage(T data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        if (chunk == null) {
            //chunk = compressedData.nextChunk();
            finalChunk = (chunk == null);
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeBoolean(finalChunk))
                    return false;

                writer.incrementState();

                if (finalChunk)
                    return true;

            case 1:
                if (!writer.writeByteArray(chunk))
                    return false;

                chunk = null;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                finalChunk = reader.readBoolean();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                chunk = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
