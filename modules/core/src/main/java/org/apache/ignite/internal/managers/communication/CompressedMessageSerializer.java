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
import org.apache.ignite.IgniteException;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import static org.apache.ignite.internal.managers.communication.CompressedMessage.BUFFER_CAPACITY;
import static org.apache.ignite.internal.managers.communication.CompressedMessage.CHUNK_SIZE;

/** Message serializer for compressed message. */
public class CompressedMessageSerializer implements MessageSerializer<CompressedMessage> {
    /** {@inheritDoc} */
    @Override public boolean writeTo(CompressedMessage msg, MessageWriter writer) {
        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(msg.directType()))
                return false;

            writer.onHeaderWritten();
        }

        while (true) {
            if (msg.chunk == null && msg.chunkedReader != null) {
                msg.chunk = msg.nextChunk();

                msg.finalChunk = (msg.chunk == null);
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeInt(msg.dataSize))
                        return false;

                    writer.incrementState();

                    if (msg.dataSize == 0)
                        return true;

                case 1:
                    if (!writer.writeBoolean(msg.finalChunk))
                        return false;

                    writer.incrementState();

                    if (msg.finalChunk)
                        return true;

                case 2:
                    if (!writer.writeByteArray(msg.chunk))
                        return false;

                    msg.chunk = null;

                    writer.decrementState();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(CompressedMessage msg, MessageReader reader) {
        if (msg.tmpBuf == null)
            msg.tmpBuf = ByteBuffer.allocateDirect(BUFFER_CAPACITY);

        assert msg.chunk == null : msg.chunk;

        while (true) {
            switch (reader.state()) {
                case 0:
                    msg.dataSize = reader.readInt();

                    if (!reader.isLastRead())
                        return false;

                    if (msg.dataSize == 0)
                        return true;

                    reader.incrementState();

                case 1:
                    msg.finalChunk = reader.readBoolean();

                    if (!reader.isLastRead())
                        return false;

                    if (msg.finalChunk)
                        return true;

                    reader.incrementState();

                case 2:
                    msg.chunk = reader.readByteArray();

                    if (!reader.isLastRead())
                        return false;

                    if (msg.chunk == null) {
                        throw new IgniteException("Failed to read compressed message: unexpected null chunk " +
                            "(stream is corrupted or the sender is incompatible).");
                    }

                    if (msg.tmpBuf.remaining() <= CHUNK_SIZE) {
                        ByteBuffer newTmpBuf = ByteBuffer.allocateDirect(msg.tmpBuf.capacity() * 2);

                        msg.tmpBuf.flip();

                        newTmpBuf.put(msg.tmpBuf);

                        msg.tmpBuf = newTmpBuf;
                    }

                    msg.tmpBuf.put(msg.chunk);

                    reader.decrementState();

                    msg.chunk = null;
            }
        }
    }
}
