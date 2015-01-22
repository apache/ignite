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

package org.apache.ignite.internal.util.direct;

import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.clock.*;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import sun.misc.*;
import sun.nio.ch.*;

import java.nio.*;
import java.util.*;

import static org.apache.ignite.internal.util.direct.GridTcpCommunicationMessageAdapter.*;

/**
 * Communication message state.
 */
@SuppressWarnings("PublicField")
public class GridTcpCommunicationMessageState {
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
    private static final byte[] EMPTY_UUID_BYTES = new byte[16];

    /** */
    private static final ArrayCreator<byte[]> BYTE_ARR_CREATOR = new ArrayCreator<byte[]>() {
        @Override public byte[] create(int len) {
            switch (len) {
                case -1:
                    return BYTE_ARR_NOT_READ;

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
            switch (len) {
                case -1:
                    return SHORT_ARR_NOT_READ;

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
            switch (len) {
                case -1:
                    return INT_ARR_NOT_READ;

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
            switch (len) {
                case -1:
                    return LONG_ARR_NOT_READ;

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
            switch (len) {
                case -1:
                    return FLOAT_ARR_NOT_READ;

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
            switch (len) {
                case -1:
                    return DOUBLE_ARR_NOT_READ;

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
            switch (len) {
                case -1:
                    return CHAR_ARR_NOT_READ;

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
            switch (len) {
                case -1:
                    return BOOLEAN_ARR_NOT_READ;

                case 0:
                    return BOOLEAN_ARR_EMPTY;

                default:
                    return new boolean[len];
            }
        }
    };

    /** */
    private GridNioMessageWriter msgWriter;

    /** */
    private GridNioMessageReader msgReader;

    /** */
    private UUID nodeId;

    /** */
    private ByteBuffer buf;

    /** */
    private byte[] heapArr;

    /** */
    private long baseOff;

    /** */
    private boolean arrHdrDone;

    /** */
    private int arrOff;

    /** */
    private Object tmpArr;

    /** */
    private int tmpArrOff;

    /** */
    private int tmpArrBytes;

    /** */
    private boolean msgNotNull;

    /** */
    private boolean msgNotNullDone;

    /** */
    private boolean msgTypeDone;

    /** */
    private GridTcpCommunicationMessageAdapter msg;

    /** */
    public int idx;

    /** */
    public boolean typeWritten;

    /** */
    public Iterator<?> it;

    /** */
    public Object cur = NULL;

    /** */
    public boolean keyDone;

    /** */
    public int readSize = -1;

    /** */
    public int readItems;

    /**
     * @param msgWriter Message writer.
     * @param nodeId Node ID (provided only if versions are different).
     */
    public void messageWriter(GridNioMessageWriter msgWriter, @Nullable UUID nodeId) {
        assert msgWriter != null;

        this.msgWriter = msgWriter;
        this.nodeId = nodeId;
    }

    /**
     * @param msgReader Message reader.
     * @param nodeId Node ID (provided only if versions are different).
     */
    public void messageReader(GridNioMessageReader msgReader, @Nullable UUID nodeId) {
        assert msgReader != null;

        this.msgReader = msgReader;
        this.nodeId = nodeId;
    }

    /**
     * @param buf Buffer.
     */
    public final void setBuffer(ByteBuffer buf) {
        assert buf != null;

        if (this.buf != buf) {
            this.buf = buf;

            heapArr = buf.isDirect() ? null : buf.array();
            baseOff = buf.isDirect() ? ((DirectBuffer)buf).address() : BYTE_ARR_OFF;
        }
    }

    /**
     * @param b Byte value.
     * @return Whether value was written.
     */
    public final boolean putByte(byte b) {
        assert buf != null;

        if (!buf.hasRemaining())
            return false;

        int pos = buf.position();

        UNSAFE.putByte(heapArr, baseOff + pos, b);

        buf.position(pos + 1);

        return true;
    }

    /**
     * @return Byte value.
     */
    public final byte getByte() {
        assert buf != null;
        assert buf.hasRemaining();

        int pos = buf.position();

        buf.position(pos + 1);

        return UNSAFE.getByte(heapArr, baseOff + pos);
    }

    /**
     * @param s Short value.
     * @return Whether value was written.
     */
    public final boolean putShort(short s) {
        assert buf != null;

        if (buf.remaining() < 2)
            return false;

        int pos = buf.position();

        UNSAFE.putShort(heapArr, baseOff + pos, s);

        buf.position(pos + 2);

        return true;
    }

    /**
     * @return Short value.
     */
    public final short getShort() {
        assert buf != null;
        assert buf.remaining() >= 2;

        int pos = buf.position();

        buf.position(pos + 2);

        return UNSAFE.getShort(heapArr, baseOff + pos);
    }

    /**
     * @param i Integer value.
     * @return Whether value was written.
     */
    public final boolean putInt(int i) {
        assert buf != null;

        if (buf.remaining() < 4)
            return false;

        int pos = buf.position();

        UNSAFE.putInt(heapArr, baseOff + pos, i);

        buf.position(pos + 4);

        return true;
    }

    /**
     * @return Integer value.
     */
    public final int getInt() {
        assert buf != null;
        assert buf.remaining() >= 4;

        int pos = buf.position();

        buf.position(pos + 4);

        return UNSAFE.getInt(heapArr, baseOff + pos);
    }

    /**
     * @param l Long value.
     * @return Whether value was written.
     */
    public final boolean putLong(long l) {
        assert buf != null;

        if (buf.remaining() < 8)
            return false;

        int pos = buf.position();

        UNSAFE.putLong(heapArr, baseOff + pos, l);

        buf.position(pos + 8);

        return true;
    }

    /**
     * @return Long value.
     */
    public final long getLong() {
        assert buf != null;
        assert buf.remaining() >= 8;

        int pos = buf.position();

        buf.position(pos + 8);

        return UNSAFE.getLong(heapArr, baseOff + pos);
    }

    /**
     * @param f Float value.
     * @return Whether value was written.
     */
    public final boolean putFloat(float f) {
        assert buf != null;

        if (buf.remaining() < 4)
            return false;

        int pos = buf.position();

        UNSAFE.putFloat(heapArr, baseOff + pos, f);

        buf.position(pos + 4);

        return true;
    }

    /**
     * @return Float value.
     */
    public final float getFloat() {
        assert buf != null;
        assert buf.remaining() >= 4;

        int pos = buf.position();

        buf.position(pos + 4);

        return UNSAFE.getFloat(heapArr, baseOff + pos);
    }

    /**
     * @param d Double value.
     * @return Whether value was written.
     */
    public final boolean putDouble(double d) {
        assert buf != null;

        if (buf.remaining() < 8)
            return false;

        int pos = buf.position();

        UNSAFE.putDouble(heapArr, baseOff + pos, d);

        buf.position(pos + 8);

        return true;
    }

    /**
     * @return Double value.
     */
    public final double getDouble() {
        assert buf != null;
        assert buf.remaining() >= 8;

        int pos = buf.position();

        buf.position(pos + 8);

        return UNSAFE.getDouble(heapArr, baseOff + pos);
    }

    /**
     * @param c Char value.
     * @return Whether value was written.
     */
    public final boolean putChar(char c) {
        assert buf != null;

        if (buf.remaining() < 2)
            return false;

        int pos = buf.position();

        UNSAFE.putChar(heapArr, baseOff + pos, c);

        buf.position(pos + 2);

        return true;
    }

    /**
     * @return Char value.
     */
    public final char getChar() {
        assert buf != null;
        assert buf.remaining() >= 2;

        int pos = buf.position();

        buf.position(pos + 2);

        return UNSAFE.getChar(heapArr, baseOff + pos);
    }

    /**
     * @param b Boolean value.
     * @return Whether value was written.
     */
    public final boolean putBoolean(boolean b) {
        assert buf != null;

        if (buf.remaining() < 1)
            return false;

        int pos = buf.position();

        UNSAFE.putBoolean(heapArr, baseOff + pos, b);

        buf.position(pos + 1);

        return true;
    }

    /**
     * @return Boolean value.
     */
    public final boolean getBoolean() {
        assert buf != null;
        assert buf.hasRemaining();

        int pos = buf.position();

        buf.position(pos + 1);

        return UNSAFE.getBoolean(heapArr, baseOff + pos);
    }

    /**
     * @param arr Byte array.
     * @return Whether array was fully written.
     */
    public final boolean putByteArray(@Nullable byte[] arr) {
        assert buf != null;

        int len = arr != null ? arr.length : 0;

        return putArray(arr, BYTE_ARR_OFF, len, len);
    }

    /**
     * @return Byte array or special
     *      {@link GridTcpCommunicationMessageAdapter#BYTE_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final byte[] getByteArray() {
        assert buf != null;

        return getArray(BYTE_ARR_CREATOR, 0, BYTE_ARR_OFF);
    }

    /**
     * @param arr Short array.
     * @return Whether array was fully written.
     */
    public final boolean putShortArray(short[] arr) {
        assert buf != null;

        int len = arr != null ? arr.length : 0;

        return putArray(arr, SHORT_ARR_OFF, len, len << 1);
    }

    /**
     * @return Short array or special
     *      {@link GridTcpCommunicationMessageAdapter#SHORT_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final short[] getShortArray() {
        assert buf != null;

        return getArray(SHORT_ARR_CREATOR, 1, SHORT_ARR_OFF);
    }

    /**
     * @param arr Integer array.
     * @return Whether array was fully written.
     */
    public final boolean putIntArray(int[] arr) {
        assert buf != null;

        int len = arr != null ? arr.length : 0;

        return putArray(arr, INT_ARR_OFF, len, len << 2);
    }

    /**
     * @return Integer array or special
     *      {@link GridTcpCommunicationMessageAdapter#INT_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final int[] getIntArray() {
        assert buf != null;

        return getArray(INT_ARR_CREATOR, 2, INT_ARR_OFF);
    }

    /**
     * @param arr Long array.
     * @return Whether array was fully written.
     */
    public final boolean putLongArray(long[] arr) {
        assert buf != null;

        int len = arr != null ? arr.length : 0;

        return putArray(arr, LONG_ARR_OFF, len, len << 3);
    }

    /**
     * @return Long array or special
     *      {@link GridTcpCommunicationMessageAdapter#LONG_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final long[] getLongArray() {
        assert buf != null;

        return getArray(LONG_ARR_CREATOR, 3, LONG_ARR_OFF);
    }

    /**
     * @param arr Float array.
     * @return Whether array was fully written.
     */
    public final boolean putFloatArray(float[] arr) {
        assert buf != null;

        int len = arr != null ? arr.length : 0;

        return putArray(arr, FLOAT_ARR_OFF, len, len << 2);
    }

    /**
     * @return Float array or special
     *      {@link GridTcpCommunicationMessageAdapter#FLOAT_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final float[] getFloatArray() {
        assert buf != null;

        return getArray(FLOAT_ARR_CREATOR, 2, FLOAT_ARR_OFF);
    }

    /**
     * @param arr Double array.
     * @return Whether array was fully written.
     */
    public final boolean putDoubleArray(double[] arr) {
        assert buf != null;

        int len = arr != null ? arr.length : 0;

        return putArray(arr, DOUBLE_ARR_OFF, len, len << 3);
    }

    /**
     * @return Double array or special
     *      {@link GridTcpCommunicationMessageAdapter#DOUBLE_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final double[] getDoubleArray() {
        assert buf != null;

        return getArray(DOUBLE_ARR_CREATOR, 3, DOUBLE_ARR_OFF);
    }

    /**
     * @param arr Char array.
     * @return Whether array was fully written.
     */
    public final boolean putCharArray(char[] arr) {
        assert buf != null;

        int len = arr != null ? arr.length : 0;

        return putArray(arr, CHAR_ARR_OFF, len, len << 1);
    }

    /**
     * @return Char array or special
     *      {@link GridTcpCommunicationMessageAdapter#CHAR_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final char[] getCharArray() {
        assert buf != null;

        return getArray(CHAR_ARR_CREATOR, 1, CHAR_ARR_OFF);
    }

    /**
     * @param arr Boolean array.
     * @return Whether array was fully written.
     */
    public final boolean putBooleanArray(boolean[] arr) {
        assert buf != null;

        int len = arr != null ? arr.length : 0;

        return putArray(arr, BOOLEAN_ARR_OFF, len, len);
    }

    /**
     * @return Boolean array or special
     *      {@link GridTcpCommunicationMessageAdapter#BOOLEAN_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final boolean[] getBooleanArray() {
        assert buf != null;

        return getArray(BOOLEAN_ARR_CREATOR, 0, BOOLEAN_ARR_OFF);
    }

    /**
     * @param uuid {@link UUID}.
     * @return Whether value was fully written.
     */
    public final boolean putUuid(@Nullable UUID uuid) {
        byte[] arr = null;

        if (uuid != null) {
            arr = new byte[16];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, uuid.getMostSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, uuid.getLeastSignificantBits());
        }

        return putByteArray(arr);
    }

    /**
     * @return {@link UUID} or special
     *      {@link GridTcpCommunicationMessageAdapter#UUID_NOT_READ}
     *      value if it was not fully read.
     */
    public final UUID getUuid() {
        byte[] arr = getByteArray();

        if (arr == BYTE_ARR_NOT_READ)
            return UUID_NOT_READ;
        else if (arr == null)
            return null;
        else {
            long most = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long least = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);

            return new UUID(most, least);
        }
    }

    /**
     * @param uuid {@link org.apache.ignite.lang.IgniteUuid}.
     * @return Whether value was fully written.
     */
    public final boolean putGridUuid(@Nullable IgniteUuid uuid) {
        byte[] arr = null;

        if (uuid != null) {
            arr = new byte[24];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, uuid.globalId().getMostSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, uuid.globalId().getLeastSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 16, uuid.localId());
        }

        return putByteArray(arr);
    }

    /**
     * @return {@link org.apache.ignite.lang.IgniteUuid} or special
     *      {@link GridTcpCommunicationMessageAdapter#GRID_UUID_NOT_READ}
     *      value if it was not fully read.
     */
    public final IgniteUuid getGridUuid() {
        byte[] arr = getByteArray();

        if (arr == BYTE_ARR_NOT_READ)
            return GRID_UUID_NOT_READ;
        else if (arr == null)
            return null;
        else {
            long most = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long least = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);
            long loc = UNSAFE.getLong(arr, BYTE_ARR_OFF + 16);

            return new IgniteUuid(new UUID(most, least), loc);
        }
    }

    /**
     * @param ver {@link GridClockDeltaVersion}.
     * @return Whether value was fully written.
     */
    public final boolean putClockDeltaVersion(@Nullable GridClockDeltaVersion ver) {
        byte[] arr = null;

        if (ver != null) {
            arr = new byte[16];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, ver.version());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, ver.topologyVersion());
        }

        return putByteArray(arr);
    }

    /**
     * @return {@link GridClockDeltaVersion} or special
     *      {@link GridTcpCommunicationMessageAdapter#CLOCK_DELTA_VER_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridClockDeltaVersion getClockDeltaVersion() {
        byte[] arr = getByteArray();

        if (arr == BYTE_ARR_NOT_READ)
            return CLOCK_DELTA_VER_NOT_READ;
        else if (arr == null)
            return null;
        else {
            long ver = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long topVer = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);

            return new GridClockDeltaVersion(ver, topVer);
        }
    }

    /**
     * @param list {@link GridByteArrayList}.
     * @return Whether value was fully written.
     */
    public final boolean putByteArrayList(@Nullable GridByteArrayList list) {
        byte[] arr = list != null ? list.internalArray() : null;
        int size = list != null ? list.size() : 0;

        return putArray(arr, BYTE_ARR_OFF, size, size);
    }

    /**
     * @return {@link GridByteArrayList} or special
     *      {@link GridTcpCommunicationMessageAdapter#BYTE_ARR_LIST_NOT_READ}
     *      value if it was not fully read.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final GridByteArrayList getByteArrayList() {
        byte[] arr = getByteArray();

        if (arr == BYTE_ARR_NOT_READ)
            return BYTE_ARR_LIST_NOT_READ;
        else if (arr == null)
            return null;
        else
            return new GridByteArrayList(arr);
    }

    /**
     * @param list {@link GridLongList}.
     * @return Whether value was fully written.
     */
    public final boolean putLongList(@Nullable GridLongList list) {
        long[] arr = list != null ? list.internalArray() : null;
        int size = list != null ? list.size() : 0;

        return putArray(arr, LONG_ARR_OFF, size, size << 3);
    }

    /**
     * @return {@link GridLongList} or special
     *      {@link GridTcpCommunicationMessageAdapter#LONG_LIST_NOT_READ}
     *      value if it was not fully read.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final GridLongList getLongList() {
        long[] arr = getLongArray();

        if (arr == LONG_ARR_NOT_READ)
            return LONG_LIST_NOT_READ;
        else if (arr == null)
            return null;
        else
            return new GridLongList(arr);
    }

    /**
     * @param ver {@link GridCacheVersion}.
     * @return Whether value was fully written.
     */
    public final boolean putCacheVersion(@Nullable GridCacheVersion ver) {
        byte[] arr = null;

        if (ver != null) {
            arr = new byte[24];

            UNSAFE.putInt(arr, BYTE_ARR_OFF, ver.topologyVersion());
            UNSAFE.putInt(arr, BYTE_ARR_OFF + 4, ver.nodeOrderAndDrIdRaw());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, ver.globalTime());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 16, ver.order());
        }

        return putByteArray(arr);
    }

    /**
     * @return {@link GridCacheVersion} or special
     *      {@link GridTcpCommunicationMessageAdapter#CACHE_VER_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridCacheVersion getCacheVersion() {
        byte[] arr = getByteArray();

        if (arr == BYTE_ARR_NOT_READ)
            return CACHE_VER_NOT_READ;
        else if (arr == null)
            return null;
        else {
            int topVerDrId = UNSAFE.getInt(arr, BYTE_ARR_OFF);
            int nodeOrder = UNSAFE.getInt(arr, BYTE_ARR_OFF + 4);
            long globalTime = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);
            long order = UNSAFE.getLong(arr, BYTE_ARR_OFF + 16);

            return new GridCacheVersion(topVerDrId, nodeOrder, globalTime, order);
        }
    }

    /**
     * @param id {@link GridDhtPartitionExchangeId}.
     * @return Whether value was fully written.
     */
    public final boolean putDhtPartitionExchangeId(@Nullable GridDhtPartitionExchangeId id) {
        byte[] arr = null;

        if (id != null) {
            arr = new byte[28];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, id.nodeId().getMostSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, id.nodeId().getLeastSignificantBits());
            UNSAFE.putInt(arr, BYTE_ARR_OFF + 16, id.event());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 20, id.topologyVersion());
        }

        return putByteArray(arr);
    }

    /**
     * @return {@link GridDhtPartitionExchangeId} or special
     *      {@link GridTcpCommunicationMessageAdapter#DHT_PART_EXCHANGE_ID_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridDhtPartitionExchangeId getDhtPartitionExchangeId() {
        byte[] arr = getByteArray();

        if (arr == BYTE_ARR_NOT_READ)
            return DHT_PART_EXCHANGE_ID_NOT_READ;
        else if (arr == null)
            return null;
        else {
            long most = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long least = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);
            int evt = UNSAFE.getInt(arr, BYTE_ARR_OFF + 16);
            long topVer = UNSAFE.getLong(arr, BYTE_ARR_OFF + 20);

            return new GridDhtPartitionExchangeId(new UUID(most, least), evt, topVer);
        }
    }

    /**
     * @param bytes {@link GridCacheValueBytes}.
     * @return Whether value was fully written.
     */
    public final boolean putValueBytes(@Nullable GridCacheValueBytes bytes) {
        byte[] arr = null;

        if (bytes != null) {
            if (bytes.get() != null) {
                int len = bytes.get().length;

                arr = new byte[len + 2];

                UNSAFE.putBoolean(arr, BYTE_ARR_OFF, true);
                UNSAFE.copyMemory(bytes.get(), BYTE_ARR_OFF, arr, BYTE_ARR_OFF + 1, len);
                UNSAFE.putBoolean(arr, BYTE_ARR_OFF + 1 + len, bytes.isPlain());
            }
            else {
                arr = new byte[1];

                UNSAFE.putBoolean(arr, BYTE_ARR_OFF, false);
            }
        }

        return putByteArray(arr);
    }

    /**
     * @return {@link GridCacheValueBytes} or special
     *      {@link GridTcpCommunicationMessageAdapter#VAL_BYTES_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridCacheValueBytes getValueBytes() {
        byte[] arr = getByteArray();

        if (arr == BYTE_ARR_NOT_READ)
            return VAL_BYTES_NOT_READ;
        else if (arr == null)
            return null;
        else {
            boolean notNull = UNSAFE.getBoolean(arr, BYTE_ARR_OFF);

            if (notNull) {
                int len = arr.length - 2;

                assert len >= 0 : len;

                byte[] bytesArr = new byte[len];

                UNSAFE.copyMemory(arr, BYTE_ARR_OFF + 1, bytesArr, BYTE_ARR_OFF, len);

                boolean isPlain = UNSAFE.getBoolean(arr, BYTE_ARR_OFF + 1 + len);

                return new GridCacheValueBytes(bytesArr, isPlain);
            }
            else
                return new GridCacheValueBytes();
        }
    }

    /**
     * @param str {@link String}.
     * @return Whether value was fully written.
     */
    public final boolean putString(@Nullable String str) {
        return putByteArray(str != null ? str.getBytes() : null);
    }

    /**
     * @return {@link String} or special {@link GridTcpCommunicationMessageAdapter#STR_NOT_READ}
     *      value if it was not fully read.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final String getString() {
        byte[] arr = getByteArray();

        if (arr == BYTE_ARR_NOT_READ)
            return STR_NOT_READ;
        else if (arr == null)
            return null;
        else
            return new String(arr);
    }

    /**
     * @param bits {@link BitSet}.
     * @return Whether value was fully written.
     */
    public final boolean putBitSet(@Nullable BitSet bits) {
        return putLongArray(bits != null ? bits.toLongArray() : null);
    }

    /**
     * @return {@link BitSet} or special {@link GridTcpCommunicationMessageAdapter#BIT_SET_NOT_READ}
     *      value if it was not fully read.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final BitSet getBitSet() {
        long[] arr = getLongArray();

        if (arr == LONG_ARR_NOT_READ)
            return BIT_SET_NOT_READ;
        else if (arr == null)
            return null;
        else
            return BitSet.valueOf(arr);
    }

    /**
     * @param e Enum.
     * @return Whether value was fully written.
     */
    public final boolean putEnum(@Nullable Enum<?> e) {
        return putByte(e != null ? (byte)e.ordinal() : -1);
    }

    /**
     * @param msg {@link GridTcpCommunicationMessageAdapter}.
     * @return Whether value was fully written.
     */
    public final boolean putMessage(@Nullable GridTcpCommunicationMessageAdapter msg) {
        assert buf != null;

        if (!msgNotNullDone) {
            if (!putBoolean(msg != null))
                return false;

            msgNotNullDone = true;
        }

        if (msg != null) {
            if (!msgWriter.write(nodeId, msg, buf))
                return false;

            msgNotNullDone = false;
        }

        return true;
    }

    /**
     * @return {@link GridTcpCommunicationMessageAdapter} or special
     * {@link GridTcpCommunicationMessageAdapter#MSG_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridTcpCommunicationMessageAdapter getMessage() {
        assert buf != null;

        if (!msgNotNullDone) {
            if (!buf.hasRemaining())
                return MSG_NOT_READ;

            msgNotNull = getBoolean();

            msgNotNullDone = true;
        }

        if (msgNotNull) {
            if (!msgTypeDone) {
                if (!buf.hasRemaining())
                    return MSG_NOT_READ;

                GridTcpMessageFactory factory = msgReader.messageFactory();

                byte type = getByte();

                msg = factory != null ? factory.create(type) : GridTcpCommunicationMessageFactory.create(type);

                msgTypeDone = true;
            }

            if (msgReader.read(nodeId, msg, buf)) {
                GridTcpCommunicationMessageAdapter msg0 = msg;

                msgNotNullDone = false;
                msgTypeDone = false;
                msg = null;

                return msg0;
            }
            else
                return MSG_NOT_READ;
        }
        else
            return null;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     * @param bytes Length in bytes.
     * @return Whether array was fully written
     */
    private boolean putArray(@Nullable Object arr, long off, int len, int bytes) {
        assert off > 0;
        assert len >= 0;
        assert bytes >= 0;
        assert bytes >= arrOff;

        if (!buf.hasRemaining())
            return false;

        int pos = buf.position();

        if (arr != null) {
            assert arr.getClass().isArray() && arr.getClass().getComponentType().isPrimitive();

            if (!arrHdrDone) {
                if (buf.remaining() < 5)
                    return false;

                UNSAFE.putBoolean(heapArr, baseOff + pos++, true);
                UNSAFE.putInt(heapArr, baseOff + pos, len);

                pos += 4;

                buf.position(pos);

                arrHdrDone = true;
            }

            if (!buf.hasRemaining())
                return false;

            int left = bytes - arrOff;
            int remaining = buf.remaining();

            if (left <= remaining) {
                UNSAFE.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, left);

                pos += left;

                buf.position(pos);

                arrHdrDone = false;
                arrOff = 0;
            }
            else {
                UNSAFE.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, remaining);

                pos += remaining;

                buf.position(pos);

                arrOff += remaining;

                return false;
            }
        }
        else {
            UNSAFE.putBoolean(heapArr, baseOff + pos++, false);

            buf.position(pos);
        }

        return true;
    }

    /**
     * @param creator Array creator.
     * @param lenShift Array length shift size.
     * @param off Base offset.
     * @return Array or special value if it was not fully read.
     */
    private <T> T getArray(ArrayCreator<T> creator, int lenShift, long off) {
        assert creator != null;
        assert lenShift >= 0;

        if (!arrHdrDone) {
            if (!buf.hasRemaining())
                return creator.create(-1);

            if (!getBoolean())
                return null;

            arrHdrDone = true;
        }

        if (tmpArr == null) {
            if (buf.remaining() < 4)
                return creator.create(-1);

            int len = getInt();

            if (len == 0) {
                arrHdrDone = false;

                return creator.create(0);
            }

            tmpArr = creator.create(len);
            tmpArrBytes = len << lenShift;
        }

        int toRead = tmpArrBytes - tmpArrOff;
        int remaining = buf.remaining();
        int pos = buf.position();

        if (remaining < toRead) {
            UNSAFE.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, remaining);

            buf.position(pos + remaining);

            tmpArrOff += remaining;

            return creator.create(-1);
        }
        else {
            UNSAFE.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, toRead);

            buf.position(pos + toRead);

            T arr = (T)tmpArr;

            arrHdrDone = false;
            tmpArr = null;
            tmpArrBytes = 0;
            tmpArrOff = 0;

            return arr;
        }
    }

    /**
     * @param i Integer value.
     * @return Whether value was written.
     */
    public final boolean putIntClient(int i) {
        assert buf != null;

        if (buf.remaining() < 4)
            return false;

        putByte((byte)(0xFF & (i >>> 24)));
        putByte((byte)(0xFF & (i >>> 16)));
        putByte((byte)(0xFF & (i >>> 8)));
        putByte((byte)(0xFF & i));

        return true;
    }

    /**
     * @return Integer value.
     */
    public final int getIntClient() {
        assert buf != null;
        assert buf.remaining() >= 4;

        int val = 0;

        val |= (0xFF & getByte()) << 24;
        val |= (0xFF & getByte()) << 16;
        val |= (0xFF & getByte()) << 8;
        val |= (0xFF & getByte());

        return val;
    }

    /**
     * @param val Long value.
     * @return Whether value was written.
     */
    public final boolean putLongClient(long val) {
        assert buf != null;

        if (buf.remaining() < 8)
            return false;

        putByte((byte)(val >>> 56));
        putByte((byte)(0xFFL & (val >>> 48)));
        putByte((byte)(0xFFL & (val >>> 40)));
        putByte((byte)(0xFFL & (val >>> 32)));
        putByte((byte)(0xFFL & (val >>> 24)));
        putByte((byte)(0xFFL & (val >>> 16)));
        putByte((byte)(0xFFL & (val >>> 8)));
        putByte((byte) (0xFFL & val));

        return true;
    }

    /**
     * @return Long value.
     */
    public final long getLongClient() {
        assert buf != null;
        assert buf.remaining() >= 8;

        long x = 0;

        x |= (0xFFL & getByte()) << 56;
        x |= (0xFFL & getByte()) << 48;
        x |= (0xFFL & getByte()) << 40;
        x |= (0xFFL & getByte()) << 32;
        x |= (0xFFL & getByte()) << 24;
        x |= (0xFFL & getByte()) << 16;
        x |= (0xFFL & getByte()) << 8;
        x |= (0xFFL & getByte());

        return x;
    }

    /**
     * @param uuid {@link UUID}.
     * @return Whether value was fully written.
     */
    public final boolean putUuidClient(@Nullable UUID uuid) {
        byte[] arr = uuid != null ? U.uuidToBytes(uuid) : EMPTY_UUID_BYTES;

        return putByteArrayClient(arr);
    }

    /**
     * @param arr Byte array.
     * @return Whether array was fully written.
     */
    public final boolean putByteArrayClient(byte[] arr) {
        assert buf != null;
        assert arr != null;

        return putArrayClient(arr, BYTE_ARR_OFF, arr.length, arr.length);
    }

    /**
     * @param src Buffer.
     * @return Whether array was fully written
     */
    public boolean putByteBufferClient(ByteBuffer src) {
        assert src != null;
        assert src.hasArray();

        return putArrayClient(src.array(), BYTE_ARR_OFF + src.position(), src.remaining(), src.remaining());
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     * @param bytes Length in bytes.
     * @return Whether array was fully written
     */
    private boolean putArrayClient(Object arr, long off, int len, int bytes) {
        assert off > 0;
        assert len >= 0;
        assert bytes >= 0;
        assert bytes >= arrOff;
        assert arr != null;

        if (!buf.hasRemaining())
            return false;

        int pos = buf.position();

        assert arr.getClass().isArray() && arr.getClass().getComponentType().isPrimitive();

        if (!arrHdrDone)
            arrHdrDone = true;

        if (!buf.hasRemaining())
            return false;

        int left = bytes - arrOff;
        int remaining = buf.remaining();

        if (left <= remaining) {
            UNSAFE.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, left);

            pos += left;

            buf.position(pos);

            arrHdrDone = false;
            arrOff = 0;
        }
        else {
            UNSAFE.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, remaining);

            pos += remaining;

            buf.position(pos);

            arrOff += remaining;

            return false;
        }

        return true;
    }

    /**
     * @param len Array length.
     * @return Byte array or special {@link GridTcpCommunicationMessageAdapter#BYTE_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final byte[] getByteArrayClient(int len) {
        assert buf != null;

        return getArrayClient(BYTE_ARR_CREATOR, BYTE_ARR_OFF, len);
    }

    /**
     * @return {@link UUID} or special
     *      {@link GridTcpCommunicationMessageAdapter#UUID_NOT_READ}
     *      value if it was not fully read.
     */
    public final UUID getUuidClient() {
        byte[] arr = getByteArrayClient(16);

        assert arr != null;

        return arr == BYTE_ARR_NOT_READ ? UUID_NOT_READ : U.bytesToUuid(arr, 0);
    }

    /**
     * @param creator Array creator.
     * @param off Base offset.
     * @param len Length.
     * @return Array or special value if it was not fully read.
     */
    private <T> T getArrayClient(ArrayCreator<T> creator, long off, int len) {
        assert creator != null;

        if (tmpArr == null) {
            tmpArr = creator.create(len);
            tmpArrBytes = len;
        }

        int toRead = tmpArrBytes - tmpArrOff;
        int remaining = buf.remaining();
        int pos = buf.position();

        if (remaining < toRead) {
            UNSAFE.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, remaining);

            buf.position(pos + remaining);

            tmpArrOff += remaining;

            return creator.create(-1);
        }
        else {
            UNSAFE.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, toRead);

            buf.position(pos + toRead);

            T arr = (T)tmpArr;

            arrHdrDone = false;
            tmpArr = null;
            tmpArrBytes = 0;
            tmpArrOff = 0;

            return arr;
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

    /**
     * Dummy enum.
     */
    private enum DummyEnum {
        /** */
        DUMMY
    }
}
