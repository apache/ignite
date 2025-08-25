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
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
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

        while (!msgFactory.serializer(msgType).writeTo(msg, TEST_BYTE_BUFFER, writer)) {
            // No-op.
        }

        msg = msgFactory.create(msgType);

        while (!msgFactory.serializer(msgType).readFrom(msg, TEST_BYTE_BUFFER, reader)) {
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
        private final Collection<Class<?>> writtenFields = new ArrayList<>();

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
        private boolean writeField(Class<?> type) {
            if (position < capacity) {
                writtenFields.add(type);

                position++;

                return true;
            }

            position = 0;

            return false;
        }

        /** {@inheritDoc} */
        @Override public void setBuffer(ByteBuffer buf) {}

        /** {@inheritDoc} */
        @Override public boolean writeHeader(short type) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean writeByte(byte val) {
            return writeField(byte.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeShort(short val) {
            return writeField(short.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeInt(int val) {
            return writeField(int.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeLong(long val) {
            return writeField(long.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeFloat(float val) {
            return writeField(float.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeDouble(double val) {
            return writeField(double.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeChar(char val) {
            return writeField(char.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeBoolean(boolean val) {
            return writeField(boolean.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeByteArray(byte[] val) {
            return writeField(byte[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeByteArray(byte[] val, long off, int len) {
            return writeField(byte[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeShortArray(short[] val) {
            return writeField(short[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeIntArray(int[] val) {
            return writeField(int[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeLongArray(long[] val) {
            return writeField(long[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeLongArray(long[] val, int len) {
            return writeField(long[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeFloatArray(float[] val) {
            return writeField(float[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeDoubleArray(double[] val) {
            return writeField(double[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeCharArray(char[] val) {
            return writeField(char[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeBooleanArray(boolean[] val) {
            return writeField(boolean[].class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeString(String val) {
            return writeField(String.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeBitSet(BitSet val) {
            return writeField(BitSet.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeUuid(UUID val) {
            return writeField(UUID.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeIgniteUuid(IgniteUuid val) {
            return writeField(IgniteUuid.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeAffinityTopologyVersion(AffinityTopologyVersion val) {
            return writeField(AffinityTopologyVersion.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeCacheObject(CacheObject obj) {
            return writeField(CacheObject.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeKeyCacheObject(KeyCacheObject obj) {
            return writeField(KeyCacheObject.class);
        }

        /** {@inheritDoc} */
        @Override public boolean writeMessage(Message val) {
            return writeField(Message.class);
        }

        /** {@inheritDoc} */
        @Override public <T> boolean writeObjectArray(T[] arr, MessageCollectionItemType itemType) {
            return writeField(Object[].class);
        }

        /** {@inheritDoc} */
        @Override public <T> boolean writeCollection(Collection<T> col, MessageCollectionItemType itemType) {
            return writeField(Collection.class);
        }

        /** {@inheritDoc} */
        @Override public <K, V> boolean writeMap(Map<K, V> map, MessageCollectionItemType keyType,
            MessageCollectionItemType valType) {
            return writeField(Map.class);
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
        private final Collection<Class<?>> readFields = new ArrayList<>();

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
        private void readField(Class<?> type) {
            if (position++ < capacity)
                readFields.add(type);
        }

        /** {@inheritDoc} */
        @Override public void setBuffer(ByteBuffer buf) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public byte readByte() {
            readField(byte.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public short readShort() {
            readField(short.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public int readInt() {
            readField(int.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public long readLong() {
            readField(long.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public float readFloat() {
            readField(float.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public double readDouble() {
            readField(double.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public char readChar() {
            readField(char.class);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean readBoolean() {
            readField(boolean.class);

            return false;
        }

        /** {@inheritDoc} */
        @Override public byte[] readByteArray() {
            readField(byte[].class);

            return new byte[0];
        }

        /** {@inheritDoc} */
        @Override public short[] readShortArray() {
            readField(short[].class);

            return new short[0];
        }

        /** {@inheritDoc} */
        @Override public int[] readIntArray() {
            readField(int[].class);

            return new int[0];
        }

        /** {@inheritDoc} */
        @Override public long[] readLongArray() {
            readField(long[].class);

            return new long[0];
        }

        /** {@inheritDoc} */
        @Override public float[] readFloatArray() {
            readField(float[].class);

            return new float[0];
        }

        /** {@inheritDoc} */
        @Override public double[] readDoubleArray() {
            readField(double[].class);

            return new double[0];
        }

        /** {@inheritDoc} */
        @Override public char[] readCharArray() {
            readField(char[].class);

            return new char[0];
        }

        /** {@inheritDoc} */
        @Override public boolean[] readBooleanArray() {
            readField(boolean[].class);

            return new boolean[0];
        }

        /** {@inheritDoc} */
        @Override public String readString() {
            readField(String.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public BitSet readBitSet() {
            readField(BitSet.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public UUID readUuid() {
            readField(UUID.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid readIgniteUuid() {
            readField(IgniteUuid.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public AffinityTopologyVersion readAffinityTopologyVersion() {
            readField(AffinityTopologyVersion.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public <T extends Message> T readMessage() {
            readField(Message.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public CacheObject readCacheObject() {
            readField(CacheObject.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject readKeyCacheObject() {
            readField(KeyCacheObject.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public <T> T[] readObjectArray(MessageCollectionItemType itemType, Class<T> itemCls) {
            readField(Object[].class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public <C extends Collection<?>> C readCollection(MessageCollectionItemType itemType) {
            readField(Collection.class);

            return null;
        }

        /** {@inheritDoc} */
        @Override public <M extends Map<?, ?>> M readMap(MessageCollectionItemType keyType,
            MessageCollectionItemType valType, boolean linked) {
            readField(Map.class);

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
