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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.junit.Test;

import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

/** */
public abstract class AbstractCommunicationMessageSerializationTest {
    /** */
    private static final ByteBuffer TEST_BYTE_BUFFER = ByteBuffer.allocate(1024);

    /** */
    @Test
    public void testMessageSerializationAndDeserializationConsistency() throws Exception {
        AbstractTestMessageReader oneFieldReader = createMessageReader(1);
        AbstractTestMessageWriter oneFieldWriter = createMessageWriter(1);

        AbstractTestMessageReader unboundedReader = createMessageReader(MAX_VALUE);
        AbstractTestMessageWriter unboundedWriter = createMessageWriter(MAX_VALUE);

        IgniteMessageFactoryImpl msgFactory = new IgniteMessageFactoryImpl(new MessageFactoryProvider[]{messageFactory()});

        for (short msgType : msgFactory.registeredDirectTypes()) {
            checkSerializationAndDeserializationConsistency(msgFactory, msgType, oneFieldWriter, unboundedReader);

            checkSerializationAndDeserializationConsistency(msgFactory, msgType, unboundedWriter, oneFieldReader);

            checkSerializationAndDeserializationConsistency(msgFactory, msgType, oneFieldWriter, oneFieldReader);

            checkSerializationAndDeserializationConsistency(msgFactory, msgType, unboundedWriter, unboundedReader);
        }
    }

    /** */
    protected abstract MessageFactoryProvider messageFactory();

    /** */
    protected Message initializeMessage(Message msg) throws Exception {
        return msg;
    }

    /** */
    protected AbstractTestMessageReader createMessageReader(int capacity) {
        return new AbstractTestMessageReader(capacity) {};
    }

    /** */
    protected AbstractTestMessageWriter createMessageWriter(int capacity) {
        return new AbstractTestMessageWriter(capacity) {};
    }

    /** */
    private void checkSerializationAndDeserializationConsistency(
        MessageFactory msgFactory,
        short msgType,
        AbstractTestMessageWriter writer,
        AbstractTestMessageReader reader
    ) throws Exception {
        writer.reset();
        reader.reset();

        Message msg = msgFactory.create(msgType);

        initializeMessage(msg);

        while (!msg.writeTo(TEST_BYTE_BUFFER, writer)) {
            // No-op.
        }

        msg = msgFactory.create(msgType);

        reader.setCurrentReadClass(msg.getClass());

        while (!msg.readFrom(TEST_BYTE_BUFFER, reader)) {
            // No-op.
        }

        assertEquals("The serialization and deserialization protocol is not consistent for the message [cls="
            + msg.getClass().getName() + ']', writer.writtenFields, reader.readFields);

        if (!(msg instanceof HandshakeMessage)) {
            assertEquals("Mismatch fields count for the message [cls="
                + msg.getClass().getName() + ']', reader.readFields.size(), writer.writtenFields.size());
        }
    }

    /** */
    protected abstract static class AbstractTestMessageWriter implements MessageWriter {
        /** */
        private final Collection<T2<String, Class<?>>> writtenFields = new ArrayList<>();

        /** */
        private int state;

        /** */
        private int position;

        /** */
        private final int capacity;

        /** */
        protected AbstractTestMessageWriter(int capacity) {
            this.capacity = capacity;
        }

        /** */
        private boolean writeField(String name, Class<?> type) {
            if (position < capacity) {
                writtenFields.add(new T2<>(name, type));

                position++;

                return true;
            }

            position = 0;

            return false;
        }

        /** */
        public Collection<T2<String, Class<?>>> writtenFields() {
            return writtenFields;
        }

        /** {@inheritDoc} */
        @Override public void setBuffer(ByteBuffer buf) {}

        /** {@inheritDoc} */
        @Override public boolean writeHeader(short type) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean writeByte(String name, byte val) {
            return writeField(name, byte.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeShort(String name, short val) {
            return writeField(name, short.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeInt(String name, int val) {
            return writeField(name, int.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeLong(String name, long val) {
            return writeField(name, long.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeFloat(String name, float val) {
            return writeField(name, float.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeDouble(String name, double val) {
            return writeField(name, double.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeChar(String name, char val) {
            return writeField(name, char.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeBoolean(String name, boolean val) {
            return writeField(name, boolean.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeByteArray(String name, byte[] val) {
            return writeField(name, byte[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeByteArray(String name, byte[] val, long off, int len) {
            return writeField(name, byte[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeShortArray(String name, short[] val) {
            return writeField(name, short[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeIntArray(String name, int[] val) {
            return writeField(name, int[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeLongArray(String name, long[] val) {
            return writeField(name, long[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeLongArray(String name, long[] val, int len) {
            return writeField(name, long[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeFloatArray(String name, float[] val) {
            return writeField(name, float[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeDoubleArray(String name, double[] val) {
            return writeField(name, double[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeCharArray(String name, char[] val) {
            return writeField(name, char[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeBooleanArray(String name, boolean[] val) {
            return writeField(name, boolean[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeString(String name, String val) {
            return writeField(name, String.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeBitSet(String name, BitSet val) {
            return writeField(name, BitSet.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeUuid(String name, UUID val) {
            return writeField(name, UUID.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeIgniteUuid(String name, IgniteUuid val) {
            return writeField(name, IgniteUuid.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeAffinityTopologyVersion(String name, AffinityTopologyVersion val) {
            return writeField(name, AffinityTopologyVersion.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeMessage(String name, Message val) {
            return writeField(name, Message.class);
        }

        /** {@inheritDoc} */
        @Override public <T> boolean writeObjectArray(String name, T[] arr, MessageCollectionItemType itemType) {
            return writeField(name, Object[].class);
        }

        /** {@inheritDoc} */
        @Override public <T> boolean writeCollection(String name, Collection<T> col, MessageCollectionItemType itemType) {
            return writeField(name, Collection.class);
        }

        /** {@inheritDoc} */
        @Override public <K, V> boolean writeMap(String name, Map<K, V> map, MessageCollectionItemType keyType,
            MessageCollectionItemType valType) {
            return writeField(name, Map.class);
        }

        /** {@inheritDoc} */
        @Override public boolean isHeaderWritten() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void onHeaderWritten() {}

        /** {@inheritDoc} */
        @Override public int state() {
            return state;
        }

        /** {@inheritDoc} */
        @Override public void incrementState() {
            ++state;
        }

        /** {@inheritDoc} */
        @Override public void beforeInnerMessageWrite() {}

        /** {@inheritDoc} */
        @Override public void afterInnerMessageWrite(boolean finished) {}

        /** {@inheritDoc} */
        @Override public void reset() {
            writtenFields.clear();

            state = 0;

            position = 0;
        }
    }

    /** */
    protected abstract static class AbstractTestMessageReader implements MessageReader {
        /** */
        private final Collection<T2<String, Class<?>>> readFields = new ArrayList<>();

        /** */
        private int state;

        /** */
        private int position;

        /** */
        private final int capacity;

        /** */
        protected AbstractTestMessageReader(int capacity) {
            this.capacity = capacity;
        }

        /** */
        public Collection<T2<String, Class<?>>> readFields() {
            return readFields;
        }

        /** */
        private void readField(String name, Class<?> type) {
            if (position++ < capacity)
                readFields.add(new T2<>(name, type));
        }

        /** {@inheritDoc} */
        @Override public void setBuffer(ByteBuffer buf) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setCurrentReadClass(Class<? extends Message> msgCls) {}

        /** {@inheritDoc} */
        @Override public boolean beforeMessageRead() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean afterMessageRead(Class<? extends Message> msgCls) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public byte readByte(String name) {
            readField(name, byte.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public short readShort(String name) {
            readField(name, short.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public int readInt(String name) {
            readField(name, int.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public int readInt(String name, int dflt) {
            readField(name, int.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public long readLong(String name) {
            readField(name, long.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public float readFloat(String name) {
            readField(name, float.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public double readDouble(String name) {
            readField(name, double.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public char readChar(String name) {
            readField(name, char.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean readBoolean(String name) {
            readField(name, boolean.class);

            return false;
        }

        /** {@inheritDoc} */
        @Override public byte[] readByteArray(String name) {
            readField(name, byte[].class);

            return new byte[0];
        }

        /** {@inheritDoc} */
        @Override public short[] readShortArray(String name) {
            readField(name, short[].class);

            return new short[0];
        }

        /** {@inheritDoc} */
        @Override public int[] readIntArray(String name) {
            readField(name, int[].class);

            return new int[0];
        }

        /** {@inheritDoc} */
        @Override public long[] readLongArray(String name) {
            readField(name, long[].class);

            return new long[0];
        }

        /** {@inheritDoc} */
        @Override public float[] readFloatArray(String name) {
            readField(name, float[].class);

            return new float[0];
        }

        /** {@inheritDoc} */
        @Override public double[] readDoubleArray(String name) {
            readField(name, double[].class);

            return new double[0];
        }

        /** {@inheritDoc} */
        @Override public char[] readCharArray(String name) {
            readField(name, char[].class);

            return new char[0];
        }

        /** {@inheritDoc} */
        @Override public boolean[] readBooleanArray(String name) {
            readField(name, boolean[].class);

            return new boolean[0];
        }

        /** {@inheritDoc} */
        @Override public String readString(String name) {
            readField(name, String.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public BitSet readBitSet(String name) {
            readField(name, BitSet.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public UUID readUuid(String name) {
            readField(name, UUID.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid readIgniteUuid(String name) {
            readField(name, IgniteUuid.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public AffinityTopologyVersion readAffinityTopologyVersion(String name) {
            readField(name, AffinityTopologyVersion.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public <T extends Message> T readMessage(String name) {
            readField(name, Message.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public <T> T[] readObjectArray(String name, MessageCollectionItemType itemType, Class<T> itemCls) {
            readField(name, Object[].class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public <C extends Collection<?>> C readCollection(String name, MessageCollectionItemType itemType) {
            readField(name, Collection.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public <M extends Map<?, ?>> M readMap(String name, MessageCollectionItemType keyType,
            MessageCollectionItemType valType, boolean linked) {
            readField(name, Map.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isLastRead() {
            if (position <= capacity)
                return true;

            position = 0;

            return false;
        }

        /** {@inheritDoc} */
        @Override public int state() {
            return state;
        }

        /** {@inheritDoc} */
        @Override public void incrementState() {
            ++state;
        }

        /** {@inheritDoc} */
        @Override public void beforeInnerMessageRead() {}

        /** {@inheritDoc} */
        @Override public void afterInnerMessageRead(boolean finished) {}

        /** {@inheritDoc} */
        @Override public void reset() {
            readFields.clear();

            state = 0;

            position = 0;
        }
    }
}
