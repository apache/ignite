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

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.MessageMarshalling;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.nio.MessageSerialization;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * Class is responsible for serializing discovery messages using RU-ready {@link MessageSerializer} mechanism.
 */
public class DiscoveryMessageParser {
    /** Size for an intermediate buffer for serializing discovery messages. */
    private static final int MSG_BUFFER_SIZE = 100;

    /** */
    private final MessageFactory msgFactory;

    /** */
    private final GridKernalContext kctx;

    /**
     * @param msgFactory Message factory.
     * @param kctx Kernal context.
     */
    public DiscoveryMessageParser(MessageFactory msgFactory, GridKernalContext kctx) {
        this.msgFactory = msgFactory;
        this.kctx = kctx;
    }

    /** Marshals discovery message to bytes array. */
    public byte[] marshalZip(Message msg) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (DeflaterOutputStream out = new DeflaterOutputStream(baos)) {
            serializeMessage(msg, out);
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to serialize message: " + msg, e);
        }

        return baos.toByteArray();
    }

    /** Unmarshals discovery message from bytes array. */
    public <T extends Message> T unmarshalZip(byte[] bytes) {
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            InflaterInputStream in = new InflaterInputStream(bais)
        ) {
            return deserializeMessage(in);
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to deserialize message.", e);
        }
    }

    /** */
    private void serializeMessage(Message m, OutputStream out) throws IOException {
        DirectMessageWriter msgWriter = new DirectMessageWriter(msgFactory);
        ByteBuffer msgBuf = ByteBuffer.allocate(MSG_BUFFER_SIZE);

        msgWriter.setBuffer(msgBuf);

        try {
            MessageMarshalling.marshal(m, kctx, null);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal discovery message", e);
        }

        boolean finished;

        do {
            msgBuf.clear();

            finished = MessageSerialization.writeTo(msgFactory, m, msgWriter);

            out.write(msgBuf.array(), 0, msgBuf.position());
        }
        while (!finished);
    }

    /** */
    private <T extends Message> T deserializeMessage(InputStream in) throws IOException {
        DirectMessageReader msgReader = new DirectMessageReader(msgFactory, null);
        ByteBuffer msgBuf = ByteBuffer.allocate(MSG_BUFFER_SIZE);

        msgReader.setBuffer(msgBuf);

        Message msg = msgFactory.create(CommonUtils.makeMessageType((byte)in.read(), (byte)in.read()));

        boolean finished;

        do {
            int read = in.read(msgBuf.array(), msgBuf.position(), msgBuf.remaining());

            if (read > 0) {
                msgBuf.limit(msgBuf.position() + read);
                msgBuf.rewind();
            }

            finished = MessageSerialization.readFrom(msgFactory, msg, msgReader);

            assert read != -1 || finished : "Stream closed before message was fully read.";

            if (!finished)
                msgBuf.compact();
        }
        while (!finished);

        try {
            MessageMarshalling.unmarshal(msg, kctx);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to unmarshal discovery message", e);
        }

        return (T)msg;
    }
}
