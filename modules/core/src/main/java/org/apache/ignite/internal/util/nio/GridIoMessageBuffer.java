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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
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

    /**
     * Head of a linked list with
     * message chunks' bytes.
     */
    private Item head;

    /**
     * The linked list's tail to
     * allow fast items adding.
     */
    private Item tail;

    /** Whole message size in bytes. */
    private int size = 0;


    /** */
    private MessageFactory factory;

    /** */
    private MessageReader reader;

    /** */
    private Marshaller marshaller;

    /** */
    private ClassLoader classLoader;


    /** */
    private GridIoMessage message;

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

        Item item;

        if (head == null) {
            item = new Item(new byte[len]);
            head = item;
            tail = head;
        }
        else {
            item = new Item(new byte[len]);
            tail.next = item;
            tail = tail.next;
        }

        byte[] bufArr = buf.isDirect() ? null : buf.array();
        long bufOff = buf.isDirect() ? ((DirectBuffer) buf).address() : BYTE_ARR_OFF;

        GridUnsafe.copyMemory(bufArr, bufOff + buf.position(), item.data, BYTE_ARR_OFF, len);

        buf.position(buf.position() + len);

        size += len;
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

    /**
     * Prepares the buffer for wrapped message unmarshalling.
     * @param factory Message factory.
     * @param reader Message reader.
     * @param marshaller Marshaller.
     * @param classLoader Used classloader.
     */
    public void prepare(
            MessageFactory factory,
            MessageReader reader,
            Marshaller marshaller,
            ClassLoader classLoader){
        this.factory = factory;
        this.reader = reader;
        this.marshaller = marshaller;
        this.classLoader = classLoader;
    }

    /**
     * @return Unmarshalled wrapped message.
     */
    public GridIoMessage message(){
        if(message == null){

            byte[] data = new byte[size];
            int offset = 0;

            Item item = head;
            do {
                System.arraycopy(item.data, 0, data, offset, item.data.length);
                offset += item.data.length;
            } while ((item = item.next) != null);

            ByteBuffer buf = ByteBuffer.wrap(data, 0, size);

            GridIoMessage msg = (GridIoMessage) factory.create(buf.get());

            assert msg != null;

            boolean finished = msg.readFrom(buf, reader);

            assert finished;

            try {
                if (msg.topic() == null) {
                    int topicOrd = msg.topicOrdinal();

                    msg.topic(topicOrd >= 0 ? GridTopic.fromOrdinal(topicOrd) :
                            U.unmarshal(marshaller, msg.topicBytes(), classLoader));
                }
            } catch (IgniteCheckedException e) {
                throw new IgniteException("Cannot unmarshall message topic", e);
            }

            message = msg;
        }

        return message;
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

    /**
     * Simple linked list item to
     * hold message chunks.
     */
    private static class Item {
        /** */
        private final byte[] data;
        /** */
        private Item next;

        /**
         * @param data Message chunk.
         */
        private Item(byte[] data) {
            this.data = data;
        }
    }
}
