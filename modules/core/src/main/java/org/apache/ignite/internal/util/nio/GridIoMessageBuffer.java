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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;

/**
 * A wrapper for {@link GridIoMessage} which allows unmarshalling
 * final message in a particular worker thread. Contains some of
 * wrapped message fields.
 */
public class GridIoMessageBuffer implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Pool policy. */
    private final byte plc;

    /** Partition. */
    private final int partition;

    /** Message ordered flag. */
    private final boolean ordered;

    /** Topic ordinal number. */
    private final int topicOrdinal;

    private byte[] data;

    /**
     * @param plc Pool policy.
     * @param partition Message partition.
     * @param ordered Message ordered flag.
     * @param topicOrdinal Topic ordinal.
     */
    GridIoMessageBuffer(byte plc, int partition, boolean ordered, int topicOrdinal) {
        this.plc = plc;
        this.partition = partition;
        this.ordered = ordered;
        this.topicOrdinal = topicOrdinal;
    }

    /**
     * Adds message chunk to
     * the list with the message parts.
     *
     * @param buf Message buffer.
     * @param len Chunk length.
     */
    void add(ByteBuffer buf, int len) {
        int offset = 0;

        if (data == null) {
            data = new byte[len];
        } else {
            byte[] tmp = new byte[data.length + len];
            System.arraycopy(data, 0, tmp, 0, data.length);
            offset = data.length;
            data = tmp;
        }

        byte[] bufArr = buf.isDirect() ? null : buf.array();
        long bufOff = buf.isDirect() ? ((DirectBuffer) buf).address() : BYTE_ARR_OFF;

        GridUnsafe.copyMemory(bufArr, bufOff + buf.position(), data, BYTE_ARR_OFF + offset, len);

        buf.position(buf.position() + len);
    }

    /**
     * @return Pool policy.
     */
    public byte policy() {
        return plc;
    }

    /**
     * @return Message partition.
     */
    public int partition() {
        return partition;
    }

    /**
     * @return Message ordered flag.
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * @return Message topic ordinal number.
     */
    public int topicOrdinal() {
        return topicOrdinal;
    }

    public byte[] data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public byte directType() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public byte fieldsCount() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void onAckReceived() {
        throw new UnsupportedOperationException();
    }
}
