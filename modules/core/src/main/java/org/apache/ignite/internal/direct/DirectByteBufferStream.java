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

import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;
import sun.misc.*;
import sun.nio.ch.*;

import java.nio.*;
import java.util.*;

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
    private static final int[] INT_ARR_EMPTY = new int[0];

    /** */
    private static final long[] LONG_ARR_EMPTY = new long[0];

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
    private static final Map<Class<?>, IgniteInClosure<Object>> WRITERS = U.newHashMap(8);

    /** */
    private static final Map<Class<?>, IgniteOutClosure<Object>> READERS = U.newHashMap(8);

    static {
        WRITERS.put(byte.class, new CI1<Object>() {
            @Override public void apply(Object o) {
                
            }
        });
    }

    /** */
    private final GridTcpMessageFactory msgFactory;

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
    private MessageAdapter msg;

    /** */
    private Iterator<?> it;

    /** */
    private Object cur = NULL;

    /** */
    private boolean keyDone;

    /** */
    private int readSize = -1;

    /** */
    private int readItems;

    /** */
    private Collection<Object> col;

    /** */
    private Map<Object, Object> map;

    /** */
    private boolean lastFinished;

    /**
     * @param msgFactory Message factory.
     */
    public DirectByteBufferStream(@Nullable GridTcpMessageFactory msgFactory) {
        this.msgFactory = msgFactory;
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

    public int remaining() {
        return buf.remaining();
    }

    /**
     * @return Whether last object was fully written or read.
     */
    public boolean lastFinished() {
        return lastFinished;
    }

    /** {@inheritDoc} */
    public void writeByte(byte val) {
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putByte(heapArr, baseOff + pos, val);

            buf.position(pos + 1);
        }
    }

    /** {@inheritDoc} */
    public void writeShort(short val) {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putShort(heapArr, baseOff + pos, val);

            buf.position(pos + 2);
        }
    }

    /** {@inheritDoc} */
    public void writeInt(int val) {
        lastFinished = buf.remaining() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putInt(heapArr, baseOff + pos, val);

            buf.position(pos + 4);
        }
    }

    /** {@inheritDoc} */
    public void writeLong(long val) {
        lastFinished = buf.remaining() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putLong(heapArr, baseOff + pos, val);

            buf.position(pos + 8);
        }
    }

    /** {@inheritDoc} */
    public void writeFloat(float val) {
        lastFinished = buf.remaining() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putFloat(heapArr, baseOff + pos, val);

            buf.position(pos + 4);
        }
    }

    /** {@inheritDoc} */
    public void writeDouble(double val) {
        lastFinished = buf.remaining() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putDouble(heapArr, baseOff + pos, val);

            buf.position(pos + 8);
        }
    }

    /** {@inheritDoc} */
    public void writeChar(char val) {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putChar(heapArr, baseOff + pos, val);

            buf.position(pos + 2);
        }
    }

    /** {@inheritDoc} */
    public void writeBoolean(boolean val) {
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putBoolean(heapArr, baseOff + pos, val);

            buf.position(pos + 1);
        }
    }

    /** {@inheritDoc} */
    public void writeByteArray(byte[] val) {
        if (val != null)
            lastFinished = writeArray(val, BYTE_ARR_OFF, val.length, val.length);
        else
            writeInt(-1);
    }

    /**
     * @param val Array.
     */
    public void writeByteArray(byte[] val, int off, int len) {
        if (val != null)
            lastFinished = writeArray(val, BYTE_ARR_OFF + off, len, len, true);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    public void writeShortArray(short[] val) {
        if (val != null)
            lastFinished = writeArray(val, SHORT_ARR_OFF, val.length, val.length << 1);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    public void writeIntArray(int[] val) {
        if (val != null)
            lastFinished = writeArray(val, INT_ARR_OFF, val.length, val.length << 2);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    public void writeLongArray(long[] val) {
        if (val != null)
            lastFinished = writeArray(val, LONG_ARR_OFF, val.length, val.length << 3);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    public void writeFloatArray(float[] val) {
        if (val != null)
            lastFinished = writeArray(val, FLOAT_ARR_OFF, val.length, val.length << 2);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    public void writeDoubleArray(double[] val) {
        if (val != null)
            lastFinished = writeArray(val, DOUBLE_ARR_OFF, val.length, val.length << 3);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    public void writeCharArray(char[] val) {
        if (val != null)
            lastFinished = writeArray(val, CHAR_ARR_OFF, val.length, val.length << 1);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    public void writeBooleanArray(boolean[] val) {
        if (val != null)
            lastFinished = writeArray(val, BOOLEAN_ARR_OFF, val.length, val.length);
        else
            writeInt(-1);
    }

    /**
     * @param msg Message.
     */
    public void writeMessage(MessageAdapter msg) {
        if (msg != null)
            lastFinished = buf.hasRemaining() && msg.writeTo(buf);
        else
            writeByte(Byte.MIN_VALUE);
    }

    public <T> void writeCollection(Collection<T> col, Class<T> itemCls) {
        if (col != null) {
            if (it == null) {
                writeInt(col.size());

                if (!lastFinished)
                    return;

                it = col.iterator();
            }

            IgniteInClosure<Object> itemWriter = WRITERS.get(itemCls);

            while (it.hasNext() || cur != NULL) {
                if (cur == NULL)
                    cur = it.next();

                itemWriter.apply(cur);

                if (!lastFinished)
                    return;

                cur = NULL;
            }

            it = null;
        }
        else
            writeInt(-1);
    }

    public <K, V> void writeMap(Map<K, V> map, Class<K> keyCls, Class<V> valCls) {
        if (map != null) {
            if (it == null) {
                writeInt(map.size());

                if (!lastFinished)
                    return;

                it = map.entrySet().iterator();
            }

            IgniteInClosure<Object> keyWriter = WRITERS.get(keyCls);
            IgniteInClosure<Object> valWriter = WRITERS.get(valCls);

            while (it.hasNext() || cur != NULL) {
                if (cur == NULL)
                    cur = it.next();

                Map.Entry<K, V> e = (Map.Entry<K, V>)cur;

                if (!keyDone) {
                    keyWriter.apply(e.getKey());

                    if (!lastFinished)
                        return;

                    keyDone = true;
                }

                valWriter.apply(e.getValue());

                if (!lastFinished)
                    return;

                cur = NULL;
                keyDone = false;
            }

            it = null;
        }
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    public byte[] readByteArray() {
        return readArray(BYTE_ARR_CREATOR, 0, BYTE_ARR_OFF);
    }

    /**
     * @param len Length.
     * @return Array.
     */
    public byte[] readByteArray(int len) {
        return readArray(BYTE_ARR_CREATOR, 0, BYTE_ARR_OFF, len);
    }

    /** {@inheritDoc} */
    public short[] readShortArray() {
        return readArray(SHORT_ARR_CREATOR, 1, SHORT_ARR_OFF);
    }

    /** {@inheritDoc} */
    public int[] readIntArray() {
        return readArray(INT_ARR_CREATOR, 2, INT_ARR_OFF);
    }

    /** {@inheritDoc} */
    public long[] readLongArray() {
        return readArray(LONG_ARR_CREATOR, 3, LONG_ARR_OFF);
    }

    /** {@inheritDoc} */
    public float[] readFloatArray() {
        return readArray(FLOAT_ARR_CREATOR, 2, FLOAT_ARR_OFF);
    }

    /** {@inheritDoc} */
    public double[] readDoubleArray() {
        return readArray(DOUBLE_ARR_CREATOR, 3, DOUBLE_ARR_OFF);
    }

    /** {@inheritDoc} */
    public char[] readCharArray() {
        return readArray(CHAR_ARR_CREATOR, 1, CHAR_ARR_OFF);
    }

    /** {@inheritDoc} */
    public boolean[] readBooleanArray() {
        return readArray(BOOLEAN_ARR_CREATOR, 0, BOOLEAN_ARR_OFF);
    }

    /**
     * @return Message.
     */
    public MessageAdapter readMessage() {
        if (!msgTypeDone) {
            if (!buf.hasRemaining()) {
                lastFinished = false;

                return null;
            }

            byte type = readByte();

            msg = type == Byte.MIN_VALUE ? null : msgFactory.create(type);

            msgTypeDone = true;
        }

        lastFinished = msg == null || msg.readFrom(buf);

        if (lastFinished) {
            MessageAdapter msg0 = msg;

            msgTypeDone = false;
            msg = null;

            return msg0;
        }
        else
            return null;
    }

    public <T> Collection<T> readCollection(Class<T> itemCls) {
        if (readSize == -1) {
            int size = readInt();

            if (!lastFinished)
                return null;

            readSize = size;
        }

        if (readSize >= 0) {
            if (col == null)
                col = new ArrayList<>(readSize);

            IgniteOutClosure<Object> itemReader = READERS.get(itemCls);

            for (int i = readItems; i < readSize; i++) {
                Object item = itemReader.apply();

                if (!lastFinished)
                    return null;

                col.add(item);

                readItems++;
            }
        }

        readSize = -1;
        readItems = 0;

        return (Collection<T>)col;
    }

    public <K, V> Map<K, V> readMap(Class<K> keyCls, Class<V> valCls) {
        if (readSize == -1) {
            int size = readInt();

            if (!lastFinished)
                return null;

            readSize = size;
        }

        if (readSize >= 0) {
            if (map == null)
                map = U.newHashMap(readSize);

            IgniteOutClosure<Object> keyReader = READERS.get(keyCls);
            IgniteOutClosure<Object> valReader = READERS.get(valCls);

            for (int i = readItems; i < readSize; i++) {
                if (!keyDone) {
                    Object key = keyReader.apply();

                    if (!lastFinished)
                        return null;

                    cur = key;
                    keyDone = true;
                }

                Object val = valReader.apply();

                if (!lastFinished)
                    return null;

                map.put(cur, val);

                keyDone = false;

                readItems++;
            }
        }

        readSize = -1;
        readItems = 0;
        cur = null;

        return (Map<K, V>)map;
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
