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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.clock.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

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
    private MessageWriter writer;

    /** */
    private MessageReader reader;

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
     * @param writer Writer.
     */
    public final void setWriter(MessageWriter writer) {
        if (this.writer == null)
            this.writer = writer;
    }

    /**
     * @param reader Reader.
     */
    public final void setReader(MessageReader reader) {
        if (this.reader == null)
            this.reader = reader;
    }

    /**
     * @param buf Buffer.
     */
    public final void setBuffer(ByteBuffer buf) {
        if (writer != null)
            writer.setBuffer(buf);

        if (reader != null)
            reader.setBuffer(buf);
    }

    /**
     * @param name Field name.
     * @param b Byte value.
     * @return Whether value was written.
     */
    public final boolean putByte(String name, byte b) {
        return writer.writeByte(name, b);
    }

    /**
     * @param name Field name.
     * @return Byte value.
     */
    public final byte getByte(String name) {
        return reader.readByte(name);
    }

    /**
     * @param name Field name.
     * @param s Short value.
     * @return Whether value was written.
     */
    public final boolean putShort(String name, short s) {
        return writer.writeShort(name, s);
    }

    /**
     * @param name Field name.
     * @return Short value.
     */
    public final short getShort(String name) {
        return reader.readShort(name);
    }

    /**
     * @param name Field name.
     * @param i Integer value.
     * @return Whether value was written.
     */
    public final boolean putInt(String name, int i) {
        return writer.writeInt(name, i);
    }

    /**
     * @param name Field name.
     * @return Integer value.
     */
    public final int getInt(String name) {
        return reader.readInt(name);
    }

    /**
     * @param name Field name.
     * @param l Long value.
     * @return Whether value was written.
     */
    public final boolean putLong(String name, long l) {
        return writer.writeLong(name, l);
    }

    /**
     * @param name Field name.
     * @return Long value.
     */
    public final long getLong(String name) {
        return reader.readLong(name);
    }

    /**
     * @param name Field name.
     * @param f Float value.
     * @return Whether value was written.
     */
    public final boolean putFloat(String name, float f) {
        return writer.writeFloat(name, f);
    }

    /**
     * @param name Field name.
     * @return Float value.
     */
    public final float getFloat(String name) {
        return reader.readFloat(name);
    }

    /**
     * @param name Field name.
     * @param d Double value.
     * @return Whether value was written.
     */
    public final boolean putDouble(String name, double d) {
        return writer.writeDouble(name, d);
    }

    /**
     * @param name Field name.
     * @return Double value.
     */
    public final double getDouble(String name) {
        return reader.readDouble(name);
    }

    /**
     * @param name Field name.
     * @param c Char value.
     * @return Whether value was written.
     */
    public final boolean putChar(String name, char c) {
        return writer.writeChar(name, c);
    }

    /**
     * @param name Field name.
     * @return Char value.
     */
    public final char getChar(String name) {
        return reader.readChar(name);
    }

    /**
     * @param name Field name.
     * @param b Boolean value.
     * @return Whether value was written.
     */
    public final boolean putBoolean(String name, boolean b) {
        return writer.writeBoolean(name, b);
    }

    /**
     * @param name Field name.
     * @return Boolean value.
     */
    public final boolean getBoolean(String name) {
        return reader.readBoolean(name);
    }

    /**
     * @param name Field name.
     * @param arr Byte array.
     * @return Whether array was fully written.
     */
    public final boolean putByteArray(String name, @Nullable byte[] arr) {
        return writer.writeByteArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return Byte array.
     */
    public final byte[] getByteArray(String name) {
        return reader.readByteArray(name);
    }

    /**
     * @param name Field name.
     * @param arr Short array.
     * @return Whether array was fully written.
     */
    public final boolean putShortArray(String name, short[] arr) {
        return writer.writeShortArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return Short array.
     */
    public final short[] getShortArray(String name) {
        return reader.readShortArray(name);
    }

    /**
     * @param name Field name.
     * @param arr Integer array.
     * @return Whether array was fully written.
     */
    public final boolean putIntArray(String name, int[] arr) {
        return writer.writeIntArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return Integer array.
     */
    public final int[] getIntArray(String name) {
        return reader.readIntArray(name);
    }

    /**
     * @param name Field name.
     * @param arr Long array.
     * @return Whether array was fully written.
     */
    public final boolean putLongArray(String name, long[] arr) {
        return writer.writeLongArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return Long array.
     */
    public final long[] getLongArray(String name) {
        return reader.readLongArray(name);
    }

    /**
     * @param name Field name.
     * @param arr Float array.
     * @return Whether array was fully written.
     */
    public final boolean putFloatArray(String name, float[] arr) {
        return writer.writeFloatArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return Float array.
     */
    public final float[] getFloatArray(String name) {
        return reader.readFloatArray(name);
    }

    /**
     * @param name Field name.
     * @param arr Double array.
     * @return Whether array was fully written.
     */
    public final boolean putDoubleArray(String name, double[] arr) {
        return writer.writeDoubleArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return Double array.
     */
    public final double[] getDoubleArray(String name) {
        return reader.readDoubleArray(name);
    }

    /**
     * @param name Field name.
     * @param arr Char array.
     * @return Whether array was fully written.
     */
    public final boolean putCharArray(String name, char[] arr) {
        return writer.writeCharArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return Char array.
     */
    public final char[] getCharArray(String name) {
        return reader.readCharArray(name);
    }

    /**
     * @param name Field name.
     * @param arr Boolean array.
     * @return Whether array was fully written.
     */
    public final boolean putBooleanArray(String name, boolean[] arr) {
        return writer.writeBooleanArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return Boolean array.
     */
    public final boolean[] getBooleanArray(String name) {
        return reader.readBooleanArray(name);
    }

    /**
     * @param name Field name.
     * @param buf Buffer.
     * @return Whether value was fully written.
     */
    public final boolean putByteBuffer(String name, @Nullable ByteBuffer buf) {
        byte[] arr = null;

        if (buf != null) {
            ByteBuffer buf0 = buf.duplicate();

            buf0.flip();

            arr = new byte[buf0.remaining()];

            buf0.get(arr);
        }

        return putByteArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return {@link ByteBuffer}.
     */
    public final ByteBuffer getByteBuffer(String name) {
        byte[] arr = getByteArray(name);

        if (arr == null)
            return null;
        else
            return ByteBuffer.wrap(arr);
    }

    /**
     * @param name Field name.
     * @param uuid {@link UUID}.
     * @return Whether value was fully written.
     */
    public final boolean putUuid(String name, @Nullable UUID uuid) {
        byte[] arr = null;

        if (uuid != null) {
            arr = new byte[16];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, uuid.getMostSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, uuid.getLeastSignificantBits());
        }

        return putByteArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return {@link UUID}.
     */
    public final UUID getUuid(String name) {
        byte[] arr = getByteArray(name);

        if (arr == null)
            return null;
        else {
            long most = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long least = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);

            return new UUID(most, least);
        }
    }

    /**
     * @param name Field name.
     * @param uuid {@link IgniteUuid}.
     * @return Whether value was fully written.
     */
    public final boolean putGridUuid(String name, @Nullable IgniteUuid uuid) {
        byte[] arr = null;

        if (uuid != null) {
            arr = new byte[24];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, uuid.globalId().getMostSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, uuid.globalId().getLeastSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 16, uuid.localId());
        }

        return putByteArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return {@link IgniteUuid}.
     */
    public final IgniteUuid getGridUuid(String name) {
        byte[] arr = getByteArray(name);

        if (arr == null)
            return null;
        else {
            long most = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long least = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);
            long loc = UNSAFE.getLong(arr, BYTE_ARR_OFF + 16);

            return new IgniteUuid(new UUID(most, least), loc);
        }
    }

    /**
     * @param name Field name.
     * @param ver {@link GridClockDeltaVersion}.
     * @return Whether value was fully written.
     */
    public final boolean putClockDeltaVersion(String name, @Nullable GridClockDeltaVersion ver) {
        byte[] arr = null;

        if (ver != null) {
            arr = new byte[16];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, ver.version());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, ver.topologyVersion());
        }

        return putByteArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return {@link GridClockDeltaVersion}.
     */
    public final GridClockDeltaVersion getClockDeltaVersion(String name) {
        byte[] arr = getByteArray(name);

        if (arr == null)
            return null;
        else {
            long ver = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long topVer = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);

            return new GridClockDeltaVersion(ver, topVer);
        }
    }

    /**
     * @param name Field name.
     * @param list {@link GridByteArrayList}.
     * @return Whether value was fully written.
     */
    public final boolean putByteArrayList(String name, @Nullable GridByteArrayList list) {
        return putByteArray(name, list != null ? list.array() : null);
    }

    /**
     * @param name Field name.
     * @return {@link GridByteArrayList}.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final GridByteArrayList getByteArrayList(String name) {
        byte[] arr = getByteArray(name);

        if (arr == null)
            return null;
        else
            return new GridByteArrayList(arr);
    }

    /**
     * @param name Field name.
     * @param list {@link GridLongList}.
     * @return Whether value was fully written.
     */
    public final boolean putLongList(String name, @Nullable GridLongList list) {
        return putLongArray(name, list != null ? list.array() : null);
    }

    /**
     * @param name Field name.
     * @return {@link GridLongList}.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final GridLongList getLongList(String name) {
        long[] arr = getLongArray(name);

        if (arr == null)
            return null;
        else
            return new GridLongList(arr);
    }

    /**
     * @param name Field name.
     * @param ver {@link org.apache.ignite.internal.processors.cache.version.GridCacheVersion}.
     * @return Whether value was fully written.
     */
    public final boolean putCacheVersion(String name, @Nullable GridCacheVersion ver) {
        byte[] arr = null;

        if (ver != null) {
            arr = new byte[24];

            UNSAFE.putInt(arr, BYTE_ARR_OFF, ver.topologyVersion());
            UNSAFE.putInt(arr, BYTE_ARR_OFF + 4, ver.nodeOrderAndDrIdRaw());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, ver.globalTime());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 16, ver.order());
        }

        return putByteArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return {@link GridCacheVersion}.
     */
    public final GridCacheVersion getCacheVersion(String name) {
        byte[] arr = getByteArray(name);

        if (arr == null)
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
     * @param name Field name.
     * @param id {@link GridDhtPartitionExchangeId}.
     * @return Whether value was fully written.
     */
    public final boolean putDhtPartitionExchangeId(String name, @Nullable GridDhtPartitionExchangeId id) {
        byte[] arr = null;

        if (id != null) {
            arr = new byte[28];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, id.nodeId().getMostSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, id.nodeId().getLeastSignificantBits());
            UNSAFE.putInt(arr, BYTE_ARR_OFF + 16, id.event());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 20, id.topologyVersion());
        }

        return putByteArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return {@link GridDhtPartitionExchangeId}.
     */
    public final GridDhtPartitionExchangeId getDhtPartitionExchangeId(String name) {
        byte[] arr = getByteArray(name);

        if (arr == null)
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
     * @param name Field name.
     * @param bytes {@link GridCacheValueBytes}.
     * @return Whether value was fully written.
     */
    public final boolean putValueBytes(String name, @Nullable GridCacheValueBytes bytes) {
        byte[] arr = null;

        if (bytes != null) {
            byte[] bytes0 = bytes.get();

            if (bytes0 != null) {
                int len = bytes0.length;

                arr = new byte[len + 2];

                UNSAFE.putBoolean(arr, BYTE_ARR_OFF, true);
                UNSAFE.copyMemory(bytes0, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + 1, len);
                UNSAFE.putBoolean(arr, BYTE_ARR_OFF + 1 + len, bytes.isPlain());
            }
            else {
                arr = new byte[1];

                UNSAFE.putBoolean(arr, BYTE_ARR_OFF, false);
            }
        }

        return putByteArray(name, arr);
    }

    /**
     * @param name Field name.
     * @return {@link GridCacheValueBytes}.
     */
    public final GridCacheValueBytes getValueBytes(String name) {
        byte[] arr = getByteArray(name);

        if (arr == null)
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
     * @param name Field name.
     * @param str {@link String}.
     * @return Whether value was fully written.
     */
    public final boolean putString(String name, @Nullable String str) {
        return putByteArray(name, str != null ? str.getBytes() : null);
    }

    /**
     * @param name Field name.
     * @return {@link String}.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final String getString(String name) {
        byte[] arr = getByteArray(name);

        if (arr == null)
            return null;
        else
            return new String(arr);
    }

    /**
     * @param name Field name.
     * @param bits {@link BitSet}.
     * @return Whether value was fully written.
     */
    public final boolean putBitSet(String name, @Nullable BitSet bits) {
        return putLongArray(name, bits != null ? bits.toLongArray() : null);
    }

    /**
     * @param name Field name.
     * @return {@link BitSet}.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final BitSet getBitSet(String name) {
        long[] arr = getLongArray(name);

        if (arr == null)
            return null;
        else
            return BitSet.valueOf(arr);
    }

    /**
     * @param name Field name.
     * @param e Enum.
     * @return Whether value was fully written.
     */
    public final boolean putEnum(String name, @Nullable Enum<?> e) {
        return putByte(name, e != null ? (byte)e.ordinal() : -1);
    }

    /**
     * @param name Field name.
     * @param msg {@link GridTcpCommunicationMessageAdapter}.
     * @return Whether value was fully written.
     */
    public final boolean putMessage(String name, @Nullable GridTcpCommunicationMessageAdapter msg) {
        if (msg != null)
            msg.setWriter(writer);

        return writer.writeMessage(name, msg);
    }

    /**
     * @param name Field name.
     * @return {@link GridTcpCommunicationMessageAdapter}.
     */
    public final GridTcpCommunicationMessageAdapter getMessage(String name) {
        return reader.readMessage(name);
    }

    public final boolean lastRead() {
        return reader.isLastRead();
    }
}
