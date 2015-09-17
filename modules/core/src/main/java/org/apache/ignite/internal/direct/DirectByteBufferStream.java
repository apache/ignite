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

package org.apache.ignite.internal.direct;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * Portable stream based on {@link ByteBuffer}.
 */
public class DirectByteBufferStream {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    private static final long SHORT_ARR_OFF = UNSAFE.arrayBaseOffset(short[].class);

    /** */
    private static final long INT_ARR_OFF = UNSAFE.arrayBaseOffset(int[].class);

    /** */
    private static final long LONG_ARR_OFF = UNSAFE.arrayBaseOffset(long[].class);

    /** */
    private static final long FLOAT_ARR_OFF = UNSAFE.arrayBaseOffset(float[].class);

    /** */
    private static final long DOUBLE_ARR_OFF = UNSAFE.arrayBaseOffset(double[].class);

    /** */
    private static final long CHAR_ARR_OFF = UNSAFE.arrayBaseOffset(char[].class);

    /** */
    private static final long BOOLEAN_ARR_OFF = UNSAFE.arrayBaseOffset(boolean[].class);

    /** */
    private static final byte[] BYTE_ARR_EMPTY = new byte[0];

    /** */
    private static final short[] SHORT_ARR_EMPTY = new short[0];

    /** */
    private static final int[] INT_ARR_EMPTY = U.EMPTY_INTS;

    /** */
    private static final long[] LONG_ARR_EMPTY = U.EMPTY_LONGS;

    /** */
    private static final float[] FLOAT_ARR_EMPTY = new float[0];

    /** */
    private static final double[] DOUBLE_ARR_EMPTY = new double[0];

    /** */
    private static final char[] CHAR_ARR_EMPTY = new char[0];

    /** */
    private static final boolean[] BOOLEAN_ARR_EMPTY = new boolean[0];

    /** */
    private static final ArrayCreator<byte[]> BYTE_ARR_CREATOR = new ArrayCreator<byte[]>() {
        @Override public byte[] create(int len) {
            assert len >= 0;

            switch (len) {
                case 0:
                    return BYTE_ARR_EMPTY;

                default:
                    return new byte[len];
            }
        }
    };

    /** */
    private static final ArrayCreator<short[]> SHORT_ARR_CREATOR = new ArrayCreator<short[]>() {
        @Override public short[] create(int len) {
            assert len >= 0;

            switch (len) {
                case 0:
                    return SHORT_ARR_EMPTY;

                default:
                    return new short[len];
            }
        }
    };

    /** */
    private static final ArrayCreator<int[]> INT_ARR_CREATOR = new ArrayCreator<int[]>() {
        @Override public int[] create(int len) {
            assert len >= 0;

            switch (len) {
                case 0:
                    return INT_ARR_EMPTY;

                default:
                    return new int[len];
            }
        }
    };

    /** */
    private static final ArrayCreator<long[]> LONG_ARR_CREATOR = new ArrayCreator<long[]>() {
        @Override public long[] create(int len) {
            assert len >= 0;

            switch (len) {
                case 0:
                    return LONG_ARR_EMPTY;

                default:
                    return new long[len];
            }
        }
    };

    /** */
    private static final ArrayCreator<float[]> FLOAT_ARR_CREATOR = new ArrayCreator<float[]>() {
        @Override public float[] create(int len) {
            assert len >= 0;

            switch (len) {
                case 0:
                    return FLOAT_ARR_EMPTY;

                default:
                    return new float[len];
            }
        }
    };

    /** */
    private static final ArrayCreator<double[]> DOUBLE_ARR_CREATOR = new ArrayCreator<double[]>() {
        @Override public double[] create(int len) {
            assert len >= 0;

            switch (len) {
                case 0:
                    return DOUBLE_ARR_EMPTY;

                default:
                    return new double[len];
            }
        }
    };

    /** */
    private static final ArrayCreator<char[]> CHAR_ARR_CREATOR = new ArrayCreator<char[]>() {
        @Override public char[] create(int len) {
            assert len >= 0;

            switch (len) {
                case 0:
                    return CHAR_ARR_EMPTY;

                default:
                    return new char[len];
            }
        }
    };

    /** */
    private static final ArrayCreator<boolean[]> BOOLEAN_ARR_CREATOR = new ArrayCreator<boolean[]>() {
        @Override public boolean[] create(int len) {
            assert len >= 0;

            switch (len) {
                case 0:
                    return BOOLEAN_ARR_EMPTY;

                default:
                    return new boolean[len];
            }
        }
    };

    /** */
    private static final Object NULL = new Object();

    /** */
    private final MessageFactory msgFactory;

    /** */
    private final MessageFormatter msgFormatter;

    /** */
    private ByteBuffer buf;

    /** */
    private byte[] heapArr;

    /** */
    private long baseOff;

    /** */
    private int arrOff = -1;

    /** */
    private Object tmpArr;

    /** */
    private int tmpArrOff;

    /** */
    private int tmpArrBytes;

    /** */
    private boolean msgTypeDone;

    /** */
    private Message msg;

    /** */
    private Iterator<?> mapIt;

    /** */
    private Iterator<?> it;

    /** */
    private Iterator<?> arrIt;

    /** */
    private Object arrCur = NULL;

    /** */
    private Object mapCur = NULL;

    /** */
    private Object cur = NULL;

    /** */
    private boolean keyDone;

    /** */
    private int readSize = -1;

    /** */
    private int readItems;

    /** */
    private Object[] objArr;

    /** */
    private Collection<Object> col;

    /** */
    private Map<Object, Object> map;

    /** */
    private boolean lastFinished;

    /** */
    private MessageReader reader;

    /**
     * @param msgFactory Message factory.
     * @param msgFormatter Message formatter.
     */
    public DirectByteBufferStream(MessageFactory msgFactory, MessageFormatter msgFormatter) {
        this.msgFactory = msgFactory;
        this.msgFormatter = msgFormatter;
    }

    /**
     * @param buf Buffer.
     */
    public void setBuffer(ByteBuffer buf) {
        assert buf != null;

        if (this.buf != buf) {
            this.buf = buf;

            heapArr = buf.isDirect() ? null : buf.array();
            baseOff = buf.isDirect() ? ((DirectBuffer)buf).address() : BYTE_ARR_OFF;
        }
    }

    /**
     * @return Number of remaining bytes.
     */
    public int remaining() {
        return buf.remaining();
    }

    /**
     * @return Whether last object was fully written or read.
     */
    public boolean lastFinished() {
        return lastFinished;
    }

    /**
     * @param val Value.
     */
    public void writeByte(byte val) {
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putByte(heapArr, baseOff + pos, val);

            buf.position(pos + 1);
        }
    }

    /**
     * @param val Value.
     */
    public void writeShort(short val) {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putShort(heapArr, baseOff + pos, val);

            buf.position(pos + 2);
        }
    }

    /**
     * @param val Value.
     */
    public void writeInt(int val) {
        lastFinished = buf.remaining() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putInt(heapArr, baseOff + pos, val);

            buf.position(pos + 4);
        }
    }

    /**
     * @param val Value.
     */
    public void writeLong(long val) {
        lastFinished = buf.remaining() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putLong(heapArr, baseOff + pos, val);

            buf.position(pos + 8);
        }
    }

    /**
     * @param val Value.
     */
    public void writeFloat(float val) {
        lastFinished = buf.remaining() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putFloat(heapArr, baseOff + pos, val);

            buf.position(pos + 4);
        }
    }

    /**
     * @param val Value.
     */
    public void writeDouble(double val) {
        lastFinished = buf.remaining() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putDouble(heapArr, baseOff + pos, val);

            buf.position(pos + 8);
        }
    }

    /**
     * @param val Value.
     */
    public void writeChar(char val) {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putChar(heapArr, baseOff + pos, val);

            buf.position(pos + 2);
        }
    }

    /**
     * @param val Value.
     */
    public void writeBoolean(boolean val) {
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putBoolean(heapArr, baseOff + pos, val);

            buf.position(pos + 1);
        }
    }

    /**
     * @param val Value.
     */
    public void writeByteArray(byte[] val) {
        if (val != null)
            lastFinished = writeArray(val, BYTE_ARR_OFF, val.length, val.length);
        else
            writeInt(-1);
    }

    /**
     * @param val Value.
     * @param off Offset.
     * @param len Length.
     */
    public void writeByteArray(byte[] val, long off, int len) {
        if (val != null)
            lastFinished = writeArray(val, BYTE_ARR_OFF + off, len, len);
        else
            writeInt(-1);
    }

    /**
     * @param val Value
     */
    public void writeShortArray(short[] val) {
        if (val != null)
            lastFinished = writeArray(val, SHORT_ARR_OFF, val.length, val.length << 1);
        else
            writeInt(-1);
    }

    /**
     * @param val Value
     */
    public void writeIntArray(int[] val) {
        if (val != null)
            lastFinished = writeArray(val, INT_ARR_OFF, val.length, val.length << 2);
        else
            writeInt(-1);
    }

    /**
     * @param val Value
     */
    public void writeLongArray(long[] val) {
        if (val != null)
            lastFinished = writeArray(val, LONG_ARR_OFF, val.length, val.length << 3);
        else
            writeInt(-1);
    }

    /**
     * @param val Value
     */
    public void writeFloatArray(float[] val) {
        if (val != null)
            lastFinished = writeArray(val, FLOAT_ARR_OFF, val.length, val.length << 2);
        else
            writeInt(-1);
    }

    /**
     * @param val Value
     */
    public void writeDoubleArray(double[] val) {
        if (val != null)
            lastFinished = writeArray(val, DOUBLE_ARR_OFF, val.length, val.length << 3);
        else
            writeInt(-1);
    }

    /**
     * @param val Value
     */
    public void writeCharArray(char[] val) {
        if (val != null)
            lastFinished = writeArray(val, CHAR_ARR_OFF, val.length, val.length << 1);
        else
            writeInt(-1);
    }

    /**
     * @param val Value
     */
    public void writeBooleanArray(boolean[] val) {
        if (val != null)
            lastFinished = writeArray(val, BOOLEAN_ARR_OFF, val.length, val.length);
        else
            writeInt(-1);
    }

    /**
     * @param val Value
     */
    public void writeString(String val) {
        writeByteArray(val != null ? val.getBytes() : null);
    }

    /**
     * @param val Value
     */
    public void writeBitSet(BitSet val) {
        writeLongArray(val != null ? val.toLongArray() : null);
    }

    /**
     * @param val Value
     */
    public void writeUuid(UUID val) {
        writeByteArray(val != null ? U.uuidToBytes(val) : null);
    }

    /**
     * @param val Value
     */
    public void writeIgniteUuid(IgniteUuid val) {
        writeByteArray(val != null ? U.igniteUuidToBytes(val) : null);
    }

    /**
     * @param msg Message.
     */
    public void writeMessage(Message msg, MessageWriter writer) {
        if (msg != null) {
            if (buf.hasRemaining()) {
                try {
                    writer.beforeInnerMessageWrite();

                    lastFinished = msg.writeTo(buf, writer);
                }
                finally {
                    writer.afterInnerMessageWrite(lastFinished);
                }
            }
            else
                lastFinished = false;
        }
        else
            writeByte(Byte.MIN_VALUE);
    }

    /**
     * @param arr Array.
     * @param itemType Component type.
     * @param writer Writer.
     */
    public <T> void writeObjectArray(T[] arr, MessageCollectionItemType itemType, MessageWriter writer) {
        if (arr != null) {
            if (arrIt == null) {
                writeInt(arr.length);

                if (!lastFinished)
                    return;

                arrIt = arrayIterator(arr);
            }

            while (arrIt.hasNext() || arrCur != NULL) {
                if (arrCur == NULL)
                    arrCur = arrIt.next();

                write(itemType, arrCur, writer);

                if (!lastFinished)
                    return;

                arrCur = NULL;
            }

            arrIt = null;
        }
        else
            writeInt(-1);
    }

    /**
     * @param col Collection.
     * @param itemType Item type.
     * @param writer Writer.
     */
    public <T> void writeCollection(Collection<T> col, MessageCollectionItemType itemType, MessageWriter writer) {
        if (col != null) {
            if (it == null) {
                writeInt(col.size());

                if (!lastFinished)
                    return;

                it = col.iterator();
            }

            while (it.hasNext() || cur != NULL) {
                if (cur == NULL)
                    cur = it.next();

                write(itemType, cur, writer);

                if (!lastFinished)
                    return;

                cur = NULL;
            }

            it = null;
        }
        else
            writeInt(-1);
    }

    /**
     * @param map Map.
     * @param keyType Key type.
     * @param valType Value type.
     * @param writer Writer.
     */
    @SuppressWarnings("unchecked")
    public <K, V> void writeMap(Map<K, V> map, MessageCollectionItemType keyType, MessageCollectionItemType valType,
        MessageWriter writer) {
        if (map != null) {
            if (mapIt == null) {
                writeInt(map.size());

                if (!lastFinished)
                    return;

                mapIt = map.entrySet().iterator();
            }

            while (mapIt.hasNext() || mapCur != NULL) {
                Map.Entry<K, V> e;

                if (mapCur == NULL)
                    mapCur = mapIt.next();

                e = (Map.Entry<K, V>)mapCur;

                if (!keyDone) {
                    write(keyType, e.getKey(), writer);

                    if (!lastFinished)
                        return;

                    keyDone = true;
                }

                write(valType, e.getValue(), writer);

                if (!lastFinished)
                    return;

                mapCur = NULL;
                keyDone = false;
            }

            mapIt = null;
        }
        else
            writeInt(-1);
    }

    /**
     * @return Value.
     */
    public byte readByte() {
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 1);

            return UNSAFE.getByte(heapArr, baseOff + pos);
        }
        else
            return 0;
    }

    /**
     * @return Value.
     */
    public short readShort() {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 2);

            return UNSAFE.getShort(heapArr, baseOff + pos);
        }
        else
            return 0;
    }

    /**
     * @return Value.
     */
    public int readInt() {
        lastFinished = buf.remaining() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 4);

            return UNSAFE.getInt(heapArr, baseOff + pos);
        }
        else
            return 0;
    }

    /**
     * @return Value.
     */
    public long readLong() {
        lastFinished = buf.remaining() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 8);

            return UNSAFE.getLong(heapArr, baseOff + pos);
        }
        else
            return 0;
    }

    /**
     * @return Value.
     */
    public float readFloat() {
        lastFinished = buf.remaining() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 4);

            return UNSAFE.getFloat(heapArr, baseOff + pos);
        }
        else
            return 0;
    }

    /**
     * @return Value.
     */
    public double readDouble() {
        lastFinished = buf.remaining() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 8);

            return UNSAFE.getDouble(heapArr, baseOff + pos);
        }
        else
            return 0;
    }

    /**
     * @return Value.
     */
    public char readChar() {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 2);

            return UNSAFE.getChar(heapArr, baseOff + pos);
        }
        else
            return 0;
    }

    /**
     * @return Value.
     */
    public boolean readBoolean() {
        lastFinished = buf.hasRemaining();

        if (lastFinished) {
            int pos = buf.position();

            buf.position(pos + 1);

            return UNSAFE.getBoolean(heapArr, baseOff + pos);
        }
        else
            return false;
    }

    /**
     * @return Value.
     */
    public byte[] readByteArray() {
        return readArray(BYTE_ARR_CREATOR, 0, BYTE_ARR_OFF);
    }

    /**
     /**
      * @return Value.
      */
    public short[] readShortArray() {
        return readArray(SHORT_ARR_CREATOR, 1, SHORT_ARR_OFF);
    }

    /**
     * @return Value.
     */
    public int[] readIntArray() {
        return readArray(INT_ARR_CREATOR, 2, INT_ARR_OFF);
    }

    /**
     * @return Value.
     */
    public long[] readLongArray() {
        return readArray(LONG_ARR_CREATOR, 3, LONG_ARR_OFF);
    }

    /**
     * @return Value.
     */
    public float[] readFloatArray() {
        return readArray(FLOAT_ARR_CREATOR, 2, FLOAT_ARR_OFF);
    }

    /**
     * @return Value.
     */
    public double[] readDoubleArray() {
        return readArray(DOUBLE_ARR_CREATOR, 3, DOUBLE_ARR_OFF);
    }

    /**
     * @return Value.
     */
    public char[] readCharArray() {
        return readArray(CHAR_ARR_CREATOR, 1, CHAR_ARR_OFF);
    }

    /**
     * @return Value.
     */
    public boolean[] readBooleanArray() {
        return readArray(BOOLEAN_ARR_CREATOR, 0, BOOLEAN_ARR_OFF);
    }

    /**
     * @return Value.
     */
    public String readString() {
        byte[] arr = readByteArray();

        return arr != null ? new String(arr) : null;
    }

    /**
     * @return Value.
     */
    public BitSet readBitSet() {
        long[] arr = readLongArray();

        return arr != null ? BitSet.valueOf(arr) : null;
    }

    /**
     * @return Value.
     */
    public UUID readUuid() {
        byte[] arr = readByteArray();

        return arr != null ? U.bytesToUuid(arr, 0) : null;
    }

    /**
     * @return Value.
     */
    public IgniteUuid readIgniteUuid() {
        byte[] arr = readByteArray();

        return arr != null ? U.bytesToIgniteUuid(arr, 0) : null;
    }

    /**
     * @return Message.
     */
    @SuppressWarnings("unchecked")
    public <T extends Message> T readMessage() {
        if (!msgTypeDone) {
            if (!buf.hasRemaining()) {
                lastFinished = false;

                return null;
            }

            byte type = readByte();

            msg = type == Byte.MIN_VALUE ? null : msgFactory.create(type);

            if (msg != null)
                reader = msgFormatter.reader(msgFactory, msg.getClass());

            msgTypeDone = true;
        }

        lastFinished = msg == null || msg.readFrom(buf, reader);

        if (lastFinished) {
            Message msg0 = msg;

            msgTypeDone = false;
            msg = null;

            return (T)msg0;
        }
        else
            return null;
    }

    /**
     * @param itemType Component type.
     * @param itemCls Component class.
     * @return Array.
     */
    @SuppressWarnings("unchecked")
    public <T> T[] readObjectArray(MessageCollectionItemType itemType, Class<T> itemCls) {
        if (readSize == -1) {
            int size = readInt();

            if (!lastFinished)
                return null;

            readSize = size;
        }

        if (readSize >= 0) {
            if (objArr == null)
                objArr = itemCls != null ? (Object[])Array.newInstance(itemCls, readSize) : new Object[readSize];

            for (int i = readItems; i < readSize; i++) {
                Object item = read(itemType);

                if (!lastFinished)
                    return null;

                objArr[i] = item;

                readItems++;
            }
        }

        readSize = -1;
        readItems = 0;
        cur = null;

        T[] objArr0 = (T[])objArr;

        objArr = null;

        return objArr0;
    }

    /**
     * @param itemType Item type.
     * @return Collection.
     */
    @SuppressWarnings("unchecked")
    public <C extends Collection<?>> C readCollection(MessageCollectionItemType itemType) {
        if (readSize == -1) {
            int size = readInt();

            if (!lastFinished)
                return null;

            readSize = size;
        }

        if (readSize >= 0) {
            if (col == null)
                col = new ArrayList<>(readSize);

            for (int i = readItems; i < readSize; i++) {
                Object item = read(itemType);

                if (!lastFinished)
                    return null;

                col.add(item);

                readItems++;
            }
        }

        readSize = -1;
        readItems = 0;
        cur = null;

        C col0 = (C)col;

        col = null;

        return col0;
    }

    /**
     * @param keyType Key type.
     * @param valType Value type.
     * @param linked Whether linked map should be created.
     * @return Map.
     */
    @SuppressWarnings("unchecked")
    public <M extends Map<?, ?>> M readMap(MessageCollectionItemType keyType, MessageCollectionItemType valType,
        boolean linked) {
        if (readSize == -1) {
            int size = readInt();

            if (!lastFinished)
                return null;

            readSize = size;
        }

        if (readSize >= 0) {
            if (map == null)
                map = linked ? U.newLinkedHashMap(readSize) : U.newHashMap(readSize);

            for (int i = readItems; i < readSize; i++) {
                if (!keyDone) {
                    Object key = read(keyType);

                    if (!lastFinished)
                        return null;

                    mapCur = key;
                    keyDone = true;
                }

                Object val = read(valType);

                if (!lastFinished)
                    return null;

                map.put(mapCur, val);

                keyDone = false;

                readItems++;
            }
        }

        readSize = -1;
        readItems = 0;
        mapCur = null;

        M map0 = (M)map;

        map = null;

        return map0;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     * @param bytes Length in bytes.
     * @return Whether array was fully written
     */
    private boolean writeArray(Object arr, long off, int len, int bytes) {
        return writeArray(arr, off, len, bytes, false);
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     * @param bytes Length in bytes.
     * @param skipLen {@code true} if length should not be written.
     * @return Whether array was fully written
     */
    private boolean writeArray(Object arr, long off, int len, int bytes, boolean skipLen) {
        assert arr != null;
        assert arr.getClass().isArray() && arr.getClass().getComponentType().isPrimitive();
        assert off > 0;
        assert len >= 0;
        assert bytes >= 0;
        assert bytes >= arrOff;

        if (arrOff == -1) {
            if (!skipLen) {
                if (buf.remaining() < 4)
                    return false;

                writeInt(len);
            }

            arrOff = 0;
        }

        int toWrite = bytes - arrOff;
        int pos = buf.position();
        int remaining = buf.remaining();

        if (toWrite <= remaining) {
            UNSAFE.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, toWrite);

            pos += toWrite;

            buf.position(pos);

            arrOff = -1;

            return true;
        }
        else {
            UNSAFE.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, remaining);

            pos += remaining;

            buf.position(pos);

            arrOff += remaining;

            return false;
        }
    }

    /**
     * @param creator Array creator.
     * @param lenShift Array length shift size.
     * @param off Base offset.
     * @return Array or special value if it was not fully read.
     */
    private <T> T readArray(ArrayCreator<T> creator, int lenShift, long off) {
        return readArray(creator, lenShift, off, -1);
    }

    /**
     * @param creator Array creator.
     * @param lenShift Array length shift size.
     * @param off Base offset.
     * @param len Length.
     * @return Array or special value if it was not fully read.
     */
    @SuppressWarnings("unchecked")
    private <T> T readArray(ArrayCreator<T> creator, int lenShift, long off, int len) {
        assert creator != null;

        if (tmpArr == null) {
            if (len == -1) {
                if (buf.remaining() < 4) {
                    lastFinished = false;

                    return null;
                }

                len = readInt();
            }

            switch (len) {
                case -1:
                    lastFinished = true;

                    return null;

                case 0:
                    lastFinished = true;

                    return creator.create(0);

                default:
                    tmpArr = creator.create(len);
                    tmpArrBytes = len << lenShift;
            }
        }

        int toRead = tmpArrBytes - tmpArrOff;
        int remaining = buf.remaining();
        int pos = buf.position();

        lastFinished = toRead <= remaining;

        if (lastFinished) {
            UNSAFE.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, toRead);

            buf.position(pos + toRead);

            T arr = (T)tmpArr;

            tmpArr = null;
            tmpArrBytes = 0;
            tmpArrOff = 0;

            return arr;
        }
        else {
            UNSAFE.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, remaining);

            buf.position(pos + remaining);

            tmpArrOff += remaining;

            return null;
        }
    }

    /**
     * @param type Type.
     * @param val Value.
     * @param writer Writer.
     */
    private void write(MessageCollectionItemType type, Object val, MessageWriter writer) {
        switch (type) {
            case BYTE:
                writeByte((Byte)val);

                break;

            case SHORT:
                writeShort((Short)val);

                break;

            case INT:
                writeInt((Integer)val);

                break;

            case LONG:
                writeLong((Long)val);

                break;

            case FLOAT:
                writeFloat((Float)val);

                break;

            case DOUBLE:
                writeDouble((Double)val);

                break;

            case CHAR:
                writeChar((Character)val);

                break;

            case BOOLEAN:
                writeBoolean((Boolean)val);

                break;

            case BYTE_ARR:
                writeByteArray((byte[])val);

                break;

            case SHORT_ARR:
                writeShortArray((short[])val);

                break;

            case INT_ARR:
                writeIntArray((int[])val);

                break;

            case LONG_ARR:
                writeLongArray((long[])val);

                break;

            case FLOAT_ARR:
                writeFloatArray((float[])val);

                break;

            case DOUBLE_ARR:
                writeDoubleArray((double[])val);

                break;

            case CHAR_ARR:
                writeCharArray((char[])val);

                break;

            case BOOLEAN_ARR:
                writeBooleanArray((boolean[])val);

                break;

            case STRING:
                writeString((String)val);

                break;

            case BIT_SET:
                writeBitSet((BitSet)val);

                break;

            case UUID:
                writeUuid((UUID)val);

                break;

            case IGNITE_UUID:
                writeIgniteUuid((IgniteUuid)val);

                break;

            case MSG:
                try {
                    if (val != null)
                        writer.beforeInnerMessageWrite();

                    writeMessage((Message)val, writer);
                }
                finally {
                    if (val != null)
                        writer.afterInnerMessageWrite(lastFinished);
                }

                break;

            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * @param type Type.
     * @return Value.
     */
    private Object read(MessageCollectionItemType type) {
        switch (type) {
            case BYTE:
                return readByte();

            case SHORT:
                return readShort();

            case INT:
                return readInt();

            case LONG:
                return readLong();

            case FLOAT:
                return readFloat();

            case DOUBLE:
                return readDouble();

            case CHAR:
                return readChar();

            case BOOLEAN:
                return readBoolean();

            case BYTE_ARR:
                return readByteArray();

            case SHORT_ARR:
                return readShortArray();

            case INT_ARR:
                return readIntArray();

            case LONG_ARR:
                return readLongArray();

            case FLOAT_ARR:
                return readFloatArray();

            case DOUBLE_ARR:
                return readDoubleArray();

            case CHAR_ARR:
                return readCharArray();

            case BOOLEAN_ARR:
                return readBooleanArray();

            case STRING:
                return readString();

            case BIT_SET:
                return readBitSet();

            case UUID:
                return readUuid();

            case IGNITE_UUID:
                return readIgniteUuid();

            case MSG:
                return readMessage();

            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * @param arr Array.
     * @return Array iterator.
     */
    private Iterator<?> arrayIterator(final Object[] arr) {
        return new Iterator<Object>() {
            private int idx;

            @Override public boolean hasNext() {
                return idx < arr.length;
            }

            @Override public Object next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return arr[idx++];
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Array creator.
     */
    private static interface ArrayCreator<T> {
        /**
         * @param len Array length or {@code -1} if array was not fully read.
         * @return New array.
         */
        public T create(int len);
    }
}