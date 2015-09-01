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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Job siblings request.
 */
public class GridJobSiblingsRequest implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid sesId;

    /** */
    @GridDirectTransient
    private Object topic;

    /** */
    private byte[] topicBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridJobSiblingsRequest() {
        // No-op.
    }

    /**
     * @param sesId Session ID.
     * @param topic Topic.
     * @param topicBytes Serialized topic.
     */
    public GridJobSiblingsRequest(IgniteUuid sesId, Object topic, byte[] topicBytes) {
        assert sesId != null;
        assert topic != null || topicBytes != null;

        this.sesId = sesId;
        this.topic = topic;
        this.topicBytes = topicBytes;
    }

    /**
     * @return Session ID.
     */
    public IgniteUuid sessionId() {
        return sesId;
    }

    /**
     * @return Topic.
     */
    public Object topic() {
        return topic;
    }

    /**
     * @return Serialized topic.
     */
    public byte[] topicBytes() {
        return topicBytes;
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
                if (!writer.writeIgniteUuid("sesId", sesId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByteArray("topicBytes", topicBytes))
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
                sesId = reader.readIgniteUuid("sesId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                topicBytes = reader.readByteArray("topicBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridJobSiblingsRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobSiblingsRequest.class, this);
    }
}