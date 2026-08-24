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

package org.apache.ignite.spi.discovery.tcp.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.DiscoveryMarshalling;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;
import org.apache.ignite.internal.util.nio.MessageSerialization;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;

/** */
public class TcpDiscoveryMessageSerializer {
    /** Size of the intermediate buffer a message is serialized through. */
    private static final int BUFFER_SIZE = 100;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final DirectMessageWriter writer;

    /** */
    private final ByteBuffer buf = ByteBuffer.allocate(BUFFER_SIZE);

    /** @param ctx Kernal context. */
    public TcpDiscoveryMessageSerializer(GridKernalContext ctx) {
        this.ctx = ctx;

        writer = new DirectMessageWriter(ctx.messageFactory());
    }

    /**
     * Serializes a discovery message into given output stream.
     *
     * @param msg Discovery message to serialize.
     * @param out Output stream to write serialized message.
     * @throws IgniteCheckedException If serialization fails.
     * @throws IOException If serialization fails.
     */
    public void writeTo(TcpDiscoveryAbstractMessage msg, OutputStream out) throws IgniteCheckedException, IOException {
        DiscoveryMarshalling.marshal(msg, ctx, null);

        writer.reset();
        writer.setBuffer(buf);

        boolean finished;

        do {
            // Should be cleared before first operation.
            buf.clear();

            finished = MessageSerialization.writeTo(ctx.messageFactory(), msg, writer);

            out.write(buf.array(), 0, buf.position());
        }
        while (!finished);
    }

    /**
     * Serializes a discovery message into a byte array.
     *
     * @param msg Discovery message to serialize.
     * @return Serialized byte array containing the message data.
     * @throws IgniteCheckedException If serialization fails.
     */
    public byte[] serialize(TcpDiscoveryAbstractMessage msg) throws IgniteCheckedException {
        try (GridByteArrayOutputStream out = new GridByteArrayOutputStream()) {
            writeTo(msg, out);

            return out.toByteArray();
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to serialize a discovery message: " + msg, e);
        }
    }
}
