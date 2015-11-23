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

package org.apache.ignite.internal.direct.stream.v2;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.UUID;
import org.apache.ignite.internal.direct.stream.DirectByteBufferStream;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * Direct marshalling I/O stream (version 2).
 */
public class DirectByteBufferStreamImplV2 implements DirectByteBufferStream {
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
    private int arrPos = -1;

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
    private long prim;

    /** */
    private int primShift;

    /** */
    private int uuidState;

    /** */
    private long uuidMost;

    /** */
    private long uuidLeast;

    /** */
    private long uuidLocId;

    /** */
    private boolean lastFinished;

    /**
     * @param msgFactory Message factory.
     */
    public DirectByteBufferStreamImplV2(MessageFactory msgFactory) {
        this.msgFactory = msgFactory;
    }

    /** {@inheritDoc} */
    @Override public void setBuffer(ByteBuffer buf) {
        assert buf != null;

        if (this.buf != buf) {
            this.buf = buf;

            heapArr = buf.isDirect() ? null : buf.array();
            baseOff = buf.isDirect() ? ((DirectBuffer)buf).address() : BYTE_ARR_OFF;
        }
    }

    /** {@inheritDoc} */
    @Override public int remaining() {
        return buf.remaining();
    }

    /** {@inheritDoc} */
    @Override public boolean lastFinished() {
        return lastFinished;
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) {
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putByte(heapArr, baseOff + pos, val);

            buf.position(pos + 1);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putShort(heapArr, baseOff + pos, val);

            buf.position(pos + 2);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        lastFinished = buf.remaining() >= 5;

        if (lastFinished) {
            if (val == Integer.MAX_VALUE)
                val = Integer.MIN_VALUE;
            else
                val++;

            int pos = buf.position();

            while ((val & 0xFFFF_FF80) != 0) {
                byte b = (byte)(val | 0x80);

                UNSAFE.putByte(heapArr, baseOff + pos++, b);

                val >>>= 7;
            }

            UNSAFE.putByte(heapArr, baseOff + pos++, (byte)val);

            buf.position(pos);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        lastFinished = buf.remaining() >= 10;

        if (lastFinished) {
            if (val == Long.MAX_VALUE)
                val = Long.MIN_VALUE;
            else
                val++;

            int pos = buf.position();

            while ((val & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
                byte b = (byte)(val | 0x80);

                UNSAFE.putByte(heapArr, baseOff + pos++, b);

                val >>>= 7;
            }

            UNSAFE.putByte(heapArr, baseOff + pos++, (byte)val);

            buf.position(pos);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) {
        lastFinished = buf.remaining() >= 4;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putFloat(heapArr, baseOff + pos, val);

            buf.position(pos + 4);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) {
        lastFinished = buf.remaining() >= 8;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putDouble(heapArr, baseOff + pos, val);

            buf.position(pos + 8);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        lastFinished = buf.remaining() >= 2;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putChar(heapArr, baseOff + pos, val);

            buf.position(pos + 2);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) {
        lastFinished = buf.remaining() >= 1;

        if (lastFinished) {
            int pos = buf.position();

            UNSAFE.putBoolean(heapArr, baseOff + pos, val);

            buf.position(pos + 1);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val) {
        if (val != null)
            lastFinished = writeArray(val, BYTE_ARR_OFF, val.length, val.length);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val, long off, int len) {
        if (val != null)
            lastFinished = writeArray(val, BYTE_ARR_OFF + off, len, len);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        if (val != null)
            lastFinished = writeArray(val, SHORT_ARR_OFF, val.length, val.length << 1);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        if (val != null)
            lastFinished = writeArray(val, INT_ARR_OFF, val.length, val.length << 2);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        if (val != null)
            lastFinished = writeArray(val, LONG_ARR_OFF, val.length, val.length << 3);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        if (val != null)
            lastFinished = writeArray(val, FLOAT_ARR_OFF, val.length, val.length << 2);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        if (val != null)
            lastFinished = writeArray(val, DOUBLE_ARR_OFF, val.length, val.length << 3);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        if (val != null)
            lastFinished = writeArray(val, CHAR_ARR_OFF, val.length, val.length << 1);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(boolean[] val) {
        if (val != null)
            lastFinished = writeArray(val, BOOLEAN_ARR_OFF, val.length, val.length);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String val) {
        writeByteArray(val != null ? val.getBytes() : null);
    }

    /** {@inheritDoc} */
    @Override public void writeBitSet(BitSet val) {
        writeLongArray(val != null ? val.toLongArray() : null);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(UUID val) {
        switch (uuidState) {
            case 0:
                writeBoolean(val == null);

                if (!lastFinished || val == null)
                    return;

                uuidState++;

            case 1:
                writeLong(val.getMostSignificantBits());

                if (!lastFinished)
                    return;

                uuidState++;

            case 2:
                writeLong(val.getLeastSignificantBits());

                if (!lastFinished)
                    return;

                uuidState = 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void writeIgniteUuid(IgniteUuid val) {
        switch (uuidState) {
            case 0:
                writeBoolean(val == null);

                if (!lastFinished || val == null)
                    return;

                uuidState++;

            case 1:
                writeLong(val.globalId().getMostSignificantBits());

                if (!lastFinished)
                    return;

                uuidState++;

            case 2:
                writeLong(val.globalId().getLeastSignificantBits());

                if (!lastFinished)
                    return;

                uuidState++;

            case 3:
                writeLong(val.localId());

                if (!lastFinished)
                    return;

                uuidState = 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void writeMessage(Message msg, MessageWriter writer) {
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

    /** {@inheritDoc} */
    @Override public <T> void writeObjectArray(T[] arr, MessageCollectionItemType itemType,
        MessageWriter writer) {
        if (arr != null) {
            int len = arr.length;

            if (arrPos == -1) {
                writeInt(len);

                if (!lastFinished)
                    return;

                arrPos = 0;
            }

            while (arrPos < len || arrCur != NULL) {
                if (arrCur == NULL)
                    arrCur = arr[arrPos++];

                write(itemType, arrCur, writer);

                if (!lastFinished)
                    return;

                arrCur = NULL;
            }

            arrPos = -1;
        }
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(Collection<T> col, MessageCollectionItemType itemType,
        MessageWriter writer) {
        if (col != null) {
            if (col instanceof List && col instanceof RandomAccess)
                writeRandomAccessList((List<T>)col, itemType, writer);
            else {
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
        }
        else
            writeInt(-1);
    }

    /**
     * @param list List.
     * @param itemType Component type.
     * @param writer Writer.
     */
    private <T> void writeRandomAccessList(List<T> list, MessageCollectionItemType itemType, MessageWriter writer) {
        assert list instanceof RandomAccess;

        int size = list.size();

        if (arrPos == -1) {
            writeInt(size);

            if (!lastFinished)
                return;

            arrPos = 0;
        }

        while (arrPos < size || arrCur != NULL) {
            if (arrCur == NULL)
                arrCur = list.get(arrPos++);

            write(itemType, arrCur, writer);

            if (!lastFinished)
                return;

            arrCur = NULL;
        }

        arrPos = -1;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> void writeMap(Map<K, V> map, MessageCollectionItemType keyType,
        MessageCollectionItemType valType, MessageWriter writer) {
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

    /** {@inheritDoc} */
    @Override public byte readByte() {
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
    @Override public short readShort() {
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
    @Override public int readInt() {
        lastFinished = false;

        int val = 0;

        while (buf.hasRemaining()) {
            int pos = buf.position();

            byte b = UNSAFE.getByte(heapArr, baseOff + pos);

            buf.position(pos + 1);

            prim |= ((long)b & 0x7F) << (7 * primShift);

            if ((b & 0x80) == 0) {
                lastFinished = true;

                val = (int)prim;

                if (val == Integer.MIN_VALUE)
                    val = Integer.MAX_VALUE;
                else
                    val--;

                prim = 0;
                primShift = 0;

                break;
            }
            else
                primShift++;
        }

        return val;
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        lastFinished = false;

        long val = 0;

        while (buf.hasRemaining()) {
            int pos = buf.position();

            byte b = UNSAFE.getByte(heapArr, baseOff + pos);

            buf.position(pos + 1);

            prim |= ((long)b & 0x7F) << (7 * primShift);

            if ((b & 0x80) == 0) {
                lastFinished = true;

                val = prim;

                if (val == Long.MIN_VALUE)
                    val = Long.MAX_VALUE;
                else
                    val--;

                prim = 0;
                primShift = 0;

                break;
            }
            else
                primShift++;
        }

        return val;
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
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
    @Override public double readDouble() {
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
    @Override public char readChar() {
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
    @Override public boolean readBoolean() {
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
    @Override public byte[] readByteArray() {
        return readArray(BYTE_ARR_CREATOR, 0, BYTE_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray() {
        return readArray(SHORT_ARR_CREATOR, 1, SHORT_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray() {
        return readArray(INT_ARR_CREATOR, 2, INT_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray() {
        return readArray(LONG_ARR_CREATOR, 3, LONG_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray() {
        return readArray(FLOAT_ARR_CREATOR, 2, FLOAT_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray() {
        return readArray(DOUBLE_ARR_CREATOR, 3, DOUBLE_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray() {
        return readArray(CHAR_ARR_CREATOR, 1, CHAR_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public boolean[] readBooleanArray() {
        return readArray(BOOLEAN_ARR_CREATOR, 0, BOOLEAN_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public String readString() {
        byte[] arr = readByteArray();

        return arr != null ? new String(arr) : null;
    }

    /** {@inheritDoc} */
    @Override public BitSet readBitSet() {
        long[] arr = readLongArray();

        return arr != null ? BitSet.valueOf(arr) : null;
    }

    /** {@inheritDoc} */
    @Override public UUID readUuid() {
        switch (uuidState) {
            case 0:
                boolean isNull = readBoolean();

                if (!lastFinished || isNull)
                    return null;

                uuidState++;

            case 1:
                uuidMost = readLong();

                if (!lastFinished)
                    return null;

                uuidState++;

            case 2:
                uuidLeast = readLong();

                if (!lastFinished)
                    return null;

                uuidState = 0;
        }

        UUID val = new UUID(uuidMost, uuidLeast);

        uuidMost = 0;
        uuidLeast = 0;

        return val;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid readIgniteUuid() {
        switch (uuidState) {
            case 0:
                boolean isNull = readBoolean();

                if (!lastFinished || isNull)
                    return null;

                uuidState++;

            case 1:
                uuidMost = readLong();

                if (!lastFinished)
                    return null;

                uuidState++;

            case 2:
                uuidLeast = readLong();

                if (!lastFinished)
                    return null;

                uuidState++;

            case 3:
                uuidLocId = readLong();

                if (!lastFinished)
                    return null;

                uuidState = 0;
        }

        IgniteUuid val = new IgniteUuid(new UUID(uuidMost, uuidLeast), uuidLocId);

        uuidMost = 0;
        uuidLeast = 0;
        uuidLocId = 0;

        return val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T extends Message> T readMessage(MessageReader reader) {
        if (!msgTypeDone) {
            if (!buf.hasRemaining()) {
                lastFinished = false;

                return null;
            }

            byte type = readByte();

            msg = type == Byte.MIN_VALUE ? null : msgFactory.create(type);

            msgTypeDone = true;
        }

        if (msg != null) {
            try {
                reader.beforeInnerMessageRead();

                reader.setCurrentReadClass(msg.getClass());

                lastFinished = msg.readFrom(buf, reader);
            }
            finally {
                reader.afterInnerMessageRead(lastFinished);
            }
        }
        else
            lastFinished = true;

        if (lastFinished) {
            Message msg0 = msg;

            msgTypeDone = false;
            msg = null;

            return (T)msg0;
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T[] readObjectArray(MessageCollectionItemType itemType, Class<T> itemCls,
        MessageReader reader) {
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
                Object item = read(itemType, reader);

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

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <C extends Collection<?>> C readCollection(MessageCollectionItemType itemType,
        MessageReader reader) {
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
                Object item = read(itemType, reader);

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

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <M extends Map<?, ?>> M readMap(MessageCollectionItemType keyType,
        MessageCollectionItemType valType, boolean linked, MessageReader reader) {
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
                    Object key = read(keyType, reader);

                    if (!lastFinished)
                        return null;

                    mapCur = key;
                    keyDone = true;
                }

                Object val = read(valType, reader);

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
     * @return Whether array was fully written.
     */
    private boolean writeArray(Object arr, long off, int len, int bytes) {
        assert arr != null;
        assert arr.getClass().isArray() && arr.getClass().getComponentType().isPrimitive();
        assert off > 0;
        assert len >= 0;
        assert bytes >= 0;
        assert bytes >= arrOff;

        if (arrOff == -1) {
            writeInt(len);

            if (!lastFinished)
                return false;

            arrOff = 0;
        }

        int toWrite = bytes - arrOff;
        int pos = buf.position();
        int remaining = buf.remaining();

        if (toWrite <= remaining) {
            if (toWrite > 0) {
                UNSAFE.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, toWrite);

                buf.position(pos + toWrite);
            }

            arrOff = -1;

            return true;
        }
        else {
            if (remaining > 0) {
                UNSAFE.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, remaining);

                buf.position(pos + remaining);

                arrOff += remaining;
            }

            return false;
        }
    }

    /**
     * @param creator Array creator.
     * @param lenShift Array length shift size.
     * @param off Base offset.
     * @return Array or special value if it was not fully read.
     */
    @SuppressWarnings("unchecked")
    private <T> T readArray(ArrayCreator<T> creator, int lenShift, long off) {
        assert creator != null;

        if (tmpArr == null) {
            int len = readInt();

            if (!lastFinished)
                return null;

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
     * @param reader Reader.
     * @return Value.
     */
    private Object read(MessageCollectionItemType type, MessageReader reader) {
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
                return readMessage(reader);

            default:
                throw new IllegalArgumentException("Unknown type: " + type);
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
