/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.direct;

import org.apache.ignite.lang.*;
import org.apache.ignite.portables.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.kernal.processors.clock.*;
import org.gridgain.grid.util.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.nio.*;
import java.util.*;

import static org.gridgain.grid.util.direct.GridTcpCommunicationMessageAdapter.*;

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
    private static final int FIELD_HDR_LEN = 0;

    /** */
    private final GridTcpCommunicationPortableStream stream = new GridTcpCommunicationPortableStream();

    /** */
    private final PortableWriter writer = new GridTcpCommunicationPortableWriter(stream);

    /** */
    private final PortableReader reader = new GridTcpCommunicationPortableReader(stream);

    /** */
    private boolean hdrDone;

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
     * @param buf Buffer.
     */
    public final void setBuffer(ByteBuffer buf) {
        assert buf != null;
    }

    /**
     * @param name Field name.
     * @param b Byte value.
     * @return Whether value was written.
     */
    public final boolean putByte(String name, byte b) {
        if (stream.remaining() < FIELD_HDR_LEN + 1)
            return false;

        writer.writeByte(name, b);

        return true;
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
        if (stream.remaining() < FIELD_HDR_LEN + 2)
            return false;

        writer.writeShort(name, s);

        return true;
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
        if (stream.remaining() < FIELD_HDR_LEN + 4)
            return false;

        writer.writeInt(name, i);

        return true;
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
        if (stream.remaining() < FIELD_HDR_LEN + 8)
            return false;

        writer.writeLong(name, l);

        return true;
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
        if (stream.remaining() < FIELD_HDR_LEN + 4)
            return false;

        writer.writeFloat(name, f);

        return true;
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
        if (stream.remaining() < FIELD_HDR_LEN + 8)
            return false;

        writer.writeDouble(name, d);

        return true;
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
        if (stream.remaining() < FIELD_HDR_LEN + 2)
            return false;

        writer.writeChar(name, c);

        return true;
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
        if (stream.remaining() < FIELD_HDR_LEN + 1)
            return false;

        writer.writeBoolean(name, b);

        return true;
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
        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return false;

            writer.writeByteArray(name, arr);

            hdrDone = true;
        }
        else
            stream.writeByteArray(arr);

        return lastWritten();
    }

    /**
     * @param name Field name.
     * @return Byte array or special
     *      {@link GridTcpCommunicationMessageAdapter#BYTE_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final byte[] getByteArray(String name) {
        byte[] arr;

        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return BYTE_ARR_NOT_READ;

            arr = reader.readByteArray(name);

            hdrDone = true;
        }
        else
            arr = stream.readByteArray(-1);

        if (arr != BYTE_ARR_NOT_READ)
            hdrDone = false;

        return arr;
    }

    /**
     * @param name Field name.
     * @param arr Short array.
     * @return Whether array was fully written.
     */
    public final boolean putShortArray(String name, short[] arr) {
        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return false;

            writer.writeShortArray(name, arr);

            hdrDone = true;
        }
        else
            stream.writeShortArray(arr);

        return lastWritten();
    }

    /**
     * @param name Field name.
     * @return Short array or special
     *      {@link GridTcpCommunicationMessageAdapter#SHORT_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final short[] getShortArray(String name) {
        short[] arr;

        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return SHORT_ARR_NOT_READ;

            arr = reader.readShortArray(name);

            hdrDone = true;
        }
        else
            arr = stream.readShortArray(-1);

        if (arr != SHORT_ARR_NOT_READ)
            hdrDone = false;

        return arr;
    }

    /**
     * @param name Field name.
     * @param arr Integer array.
     * @return Whether array was fully written.
     */
    public final boolean putIntArray(String name, int[] arr) {
        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return false;

            writer.writeIntArray(name, arr);

            hdrDone = true;
        }
        else
            stream.writeIntArray(arr);

        return lastWritten();
    }

    /**
     * @param name Field name.
     * @return Integer array or special
     *      {@link GridTcpCommunicationMessageAdapter#INT_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final int[] getIntArray(String name) {
        int[] arr;

        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return INT_ARR_NOT_READ;

            arr = reader.readIntArray(name);

            hdrDone = true;
        }
        else
            arr = stream.readIntArray(-1);

        if (arr != INT_ARR_NOT_READ)
            hdrDone = false;

        return arr;
    }

    /**
     * @param name Field name.
     * @param arr Long array.
     * @return Whether array was fully written.
     */
    public final boolean putLongArray(String name, long[] arr) {
        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return false;

            writer.writeLongArray(name, arr);

            hdrDone = true;
        }
        else
            stream.writeLongArray(arr);

        return lastWritten();
    }

    /**
     * @param name Field name.
     * @return Long array or special
     *      {@link GridTcpCommunicationMessageAdapter#LONG_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final long[] getLongArray(String name) {
        long[] arr;

        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return LONG_ARR_NOT_READ;

            arr = reader.readLongArray(name);

            hdrDone = true;
        }
        else
            arr = stream.readLongArray(-1);

        if (arr != LONG_ARR_NOT_READ)
            hdrDone = false;

        return arr;
    }

    /**
     * @param name Field name.
     * @param arr Float array.
     * @return Whether array was fully written.
     */
    public final boolean putFloatArray(String name, float[] arr) {
        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return false;

            writer.writeFloatArray(name, arr);

            hdrDone = true;
        }
        else
            stream.writeFloatArray(arr);

        return lastWritten();
    }

    /**
     * @param name Field name.
     * @return Float array or special
     *      {@link GridTcpCommunicationMessageAdapter#FLOAT_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final float[] getFloatArray(String name) {
        float[] arr;

        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return FLOAT_ARR_NOT_READ;

            arr = reader.readFloatArray(name);

            hdrDone = true;
        }
        else
            arr = stream.readFloatArray(-1);

        if (arr != FLOAT_ARR_NOT_READ)
            hdrDone = false;

        return arr;
    }

    /**
     * @param name Field name.
     * @param arr Double array.
     * @return Whether array was fully written.
     */
    public final boolean putDoubleArray(String name, double[] arr) {
        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return false;

            writer.writeDoubleArray(name, arr);

            hdrDone = true;
        }
        else
            stream.writeDoubleArray(arr);

        return lastWritten();
    }

    /**
     * @param name Field name.
     * @return Double array or special
     *      {@link GridTcpCommunicationMessageAdapter#DOUBLE_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final double[] getDoubleArray(String name) {
        double[] arr;

        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return DOUBLE_ARR_NOT_READ;

            arr = reader.readDoubleArray(name);

            hdrDone = true;
        }
        else
            arr = stream.readDoubleArray(-1);

        if (arr != DOUBLE_ARR_NOT_READ)
            hdrDone = false;

        return arr;
    }

    /**
     * @param name Field name.
     * @param arr Char array.
     * @return Whether array was fully written.
     */
    public final boolean putCharArray(String name, char[] arr) {
        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return false;

            writer.writeCharArray(name, arr);

            hdrDone = true;
        }
        else
            stream.writeCharArray(arr);

        return lastWritten();
    }

    /**
     * @param name Field name.
     * @return Char array or special
     *      {@link GridTcpCommunicationMessageAdapter#CHAR_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final char[] getCharArray(String name) {
        char[] arr;

        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return CHAR_ARR_NOT_READ;

            arr = reader.readCharArray(name);

            hdrDone = true;
        }
        else
            arr = stream.readCharArray(-1);

        if (arr != CHAR_ARR_NOT_READ)
            hdrDone = false;

        return arr;
    }

    /**
     * @param name Field name.
     * @param arr Boolean array.
     * @return Whether array was fully written.
     */
    public final boolean putBooleanArray(String name, boolean[] arr) {
        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return false;

            writer.writeBooleanArray(name, arr);

            hdrDone = true;
        }
        else
            stream.writeBooleanArray(arr);

        return lastWritten();
    }

    /**
     * @param name Field name.
     * @return Boolean array or special
     *      {@link GridTcpCommunicationMessageAdapter#BOOLEAN_ARR_NOT_READ}
     *      value if it was not fully read.
     */
    public final boolean[] getBooleanArray(String name) {
        boolean[] arr;

        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 4)
                return BOOLEAN_ARR_NOT_READ;

            arr = reader.readBooleanArray(name);

            hdrDone = true;
        }
        else
            arr = stream.readBooleanArray(-1);

        if (arr != BOOLEAN_ARR_NOT_READ)
            hdrDone = false;

        return arr;
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
     * @return {@link ByteBuffer} or special
     *      {@link GridTcpCommunicationMessageAdapter#BYTE_BUF_NOT_READ}
     *      value if it was not fully read.
     */
    public final ByteBuffer getByteBuffer(String name) {
        byte[] arr = getByteArray(name);

        if (arr == BYTE_ARR_NOT_READ)
            return BYTE_BUF_NOT_READ;
        else if (arr == null)
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
     * @return {@link UUID} or special
     *      {@link GridTcpCommunicationMessageAdapter#UUID_NOT_READ}
     *      value if it was not fully read.
     */
    public final UUID getUuid(String name) {
        byte[] arr = getByteArray(name);

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
     * @return {@link org.apache.ignite.lang.IgniteUuid} or special
     *      {@link GridTcpCommunicationMessageAdapter#GRID_UUID_NOT_READ}
     *      value if it was not fully read.
     */
    public final IgniteUuid getGridUuid(String name) {
        byte[] arr = getByteArray(name);

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
     * @return {@link GridClockDeltaVersion} or special
     *      {@link GridTcpCommunicationMessageAdapter#CLOCK_DELTA_VER_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridClockDeltaVersion getClockDeltaVersion(String name) {
        byte[] arr = getByteArray(name);

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
     * @param name Field name.
     * @param list {@link GridByteArrayList}.
     * @return Whether value was fully written.
     */
    public final boolean putByteArrayList(String name, @Nullable GridByteArrayList list) {
        return putByteArray(name, list != null ? list.array() : null);
    }

    /**
     * @param name Field name.
     * @return {@link GridByteArrayList} or special
     *      {@link GridTcpCommunicationMessageAdapter#BYTE_ARR_LIST_NOT_READ}
     *      value if it was not fully read.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final GridByteArrayList getByteArrayList(String name) {
        byte[] arr = getByteArray(name);

        if (arr == BYTE_ARR_NOT_READ)
            return BYTE_ARR_LIST_NOT_READ;
        else if (arr == null)
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
     * @return {@link GridLongList} or special
     *      {@link GridTcpCommunicationMessageAdapter#LONG_LIST_NOT_READ}
     *      value if it was not fully read.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final GridLongList getLongList(String name) {
        long[] arr = getLongArray(name);

        if (arr == LONG_ARR_NOT_READ)
            return LONG_LIST_NOT_READ;
        else if (arr == null)
            return null;
        else
            return new GridLongList(arr);
    }

    /**
     * @param name Field name.
     * @param ver {@link GridCacheVersion}.
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
     * @return {@link GridCacheVersion} or special
     *      {@link GridTcpCommunicationMessageAdapter#CACHE_VER_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridCacheVersion getCacheVersion(String name) {
        byte[] arr = getByteArray(name);

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
     * @return {@link GridDhtPartitionExchangeId} or special
     *      {@link GridTcpCommunicationMessageAdapter#DHT_PART_EXCHANGE_ID_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridDhtPartitionExchangeId getDhtPartitionExchangeId(String name) {
        byte[] arr = getByteArray(name);

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
     * @return {@link GridCacheValueBytes} or special
     *      {@link GridTcpCommunicationMessageAdapter#VAL_BYTES_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridCacheValueBytes getValueBytes(String name) {
        byte[] arr = getByteArray(name);

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
     * @param name Field name.
     * @param str {@link String}.
     * @return Whether value was fully written.
     */
    public final boolean putString(String name, @Nullable String str) {
        return putByteArray(name, str != null ? str.getBytes() : null);
    }

    /**
     * @param name Field name.
     * @return {@link String} or special {@link GridTcpCommunicationMessageAdapter#STR_NOT_READ}
     *      value if it was not fully read.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final String getString(String name) {
        byte[] arr = getByteArray(name);

        if (arr == BYTE_ARR_NOT_READ)
            return STR_NOT_READ;
        else if (arr == null)
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
     * @return {@link BitSet} or special {@link GridTcpCommunicationMessageAdapter#BIT_SET_NOT_READ}
     *      value if it was not fully read.
     */
    @SuppressWarnings("IfMayBeConditional")
    public final BitSet getBitSet(String name) {
        long[] arr = getLongArray(name);

        if (arr == LONG_ARR_NOT_READ)
            return BIT_SET_NOT_READ;
        else if (arr == null)
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
        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN + 1)
                return false;

            writer.writeObject(name, msg);

            hdrDone = true;
        }
        else
            stream.writeMessage(msg);

        return lastWritten();
    }

    /**
     * @param name Field name.
     * @return {@link GridTcpCommunicationMessageAdapter} or special
     *      {@link GridTcpCommunicationMessageAdapter#MSG_NOT_READ}
     *      value if it was not fully read.
     */
    public final GridTcpCommunicationMessageAdapter getMessage(String name) {
        GridTcpCommunicationMessageAdapter msg;

        if (!hdrDone) {
            if (stream.remaining() < FIELD_HDR_LEN)
                return MSG_NOT_READ;

            msg = reader.readObject(name);

            hdrDone = true;
        }
        else
            msg = stream.readMessage();

        if (msg != MSG_NOT_READ)
            hdrDone = false;

        return msg;
    }

    /**
     * @return Whether last array was fully written.
     */
    private boolean lastWritten() {
        boolean written = stream.lastWritten();

        if (written)
            hdrDone = false;

        return written;
    }
}
