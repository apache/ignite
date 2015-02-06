/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.direct;

import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.direct.*;
import org.jetbrains.annotations.*;
import sun.misc.*;
import sun.nio.ch.*;

import java.nio.*;

/**
 * Portable stream based on {@link ByteBuffer}.
 */
public class GridTcpCommunicationByteBufferStream {
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
    private GridTcpCommunicationMessageAdapter msg;

    /** */
    private boolean lastFinished;

    /**
     * @param msgFactory Message factory.
     */
    public GridTcpCommunicationByteBufferStream(@Nullable GridTcpMessageFactory msgFactory) {
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
        int pos = buf.position();

        UNSAFE.putByte(heapArr, baseOff + pos, val);

        buf.position(pos + 1);
    }

    /** {@inheritDoc} */
    public void writeByteArray(byte[] val) {
        assert val != null;

        lastFinished = writeArray(val, BYTE_ARR_OFF, val.length, val.length);
    }

    /** {@inheritDoc} */
    public void writeBoolean(boolean val) {
        int pos = buf.position();

        UNSAFE.putBoolean(heapArr, baseOff + pos, val);

        buf.position(pos + 1);
    }

    /** {@inheritDoc} */
    public void writeBooleanArray(boolean[] val) {
        assert val != null;

        lastFinished = writeArray(val, BOOLEAN_ARR_OFF, val.length, val.length);
    }

    /** {@inheritDoc} */
    public void writeShort(short val) {
        int pos = buf.position();

        UNSAFE.putShort(heapArr, baseOff + pos, val);

        buf.position(pos + 2);
    }

    /** {@inheritDoc} */
    public void writeShortArray(short[] val) {
        assert val != null;

        lastFinished = writeArray(val, SHORT_ARR_OFF, val.length, val.length << 1);
    }

    /** {@inheritDoc} */
    public void writeChar(char val) {
        int pos = buf.position();

        UNSAFE.putChar(heapArr, baseOff + pos, val);

        buf.position(pos + 2);
    }

    /** {@inheritDoc} */
    public void writeCharArray(char[] val) {
        assert val != null;

        lastFinished = writeArray(val, CHAR_ARR_OFF, val.length, val.length << 1);
    }

    /** {@inheritDoc} */
    public void writeInt(int val) {
        int pos = buf.position();

        UNSAFE.putInt(heapArr, baseOff + pos, val);

        buf.position(pos + 4);
    }

    /** {@inheritDoc} */
    public void writeIntArray(int[] val) {
        assert val != null;

        lastFinished = writeArray(val, INT_ARR_OFF, val.length, val.length << 2);
    }

    /** {@inheritDoc} */
    public void writeFloat(float val) {
        int pos = buf.position();

        UNSAFE.putFloat(heapArr, baseOff + pos, val);

        buf.position(pos + 4);
    }

    /** {@inheritDoc} */
    public void writeFloatArray(float[] val) {
        assert val != null;

        lastFinished = writeArray(val, FLOAT_ARR_OFF, val.length, val.length << 2);
    }

    /** {@inheritDoc} */
    public void writeLong(long val) {
        int pos = buf.position();

        UNSAFE.putLong(heapArr, baseOff + pos, val);

        buf.position(pos + 8);
    }

    /** {@inheritDoc} */
    public void writeLongArray(long[] val) {
        assert val != null;

        lastFinished = writeArray(val, LONG_ARR_OFF, val.length, val.length << 3);
    }

    /** {@inheritDoc} */
    public void writeDouble(double val) {
        int pos = buf.position();

        UNSAFE.putDouble(heapArr, baseOff + pos, val);

        buf.position(pos + 8);
    }

    /** {@inheritDoc} */
    public void writeDoubleArray(double[] val) {
        assert val != null;

        lastFinished = writeArray(val, DOUBLE_ARR_OFF, val.length, val.length << 3);
    }

    /**
     * @param msg Message.
     */
    public void writeMessage(GridTcpCommunicationMessageAdapter msg) {
        assert msg != null;

        lastFinished = buf.hasRemaining() && msg.writeTo(buf);
    }

    /** {@inheritDoc} */
    public byte readByte() {
        assert buf.hasRemaining();

        int pos = buf.position();

        buf.position(pos + 1);

        return UNSAFE.getByte(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    public byte[] readByteArray() {
        return readArray(BYTE_ARR_CREATOR, 0, BYTE_ARR_OFF);
    }

    /** {@inheritDoc} */
    public boolean readBoolean() {
        assert buf.hasRemaining();

        int pos = buf.position();

        buf.position(pos + 1);

        return UNSAFE.getBoolean(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    public boolean[] readBooleanArray() {
        return readArray(BOOLEAN_ARR_CREATOR, 0, BOOLEAN_ARR_OFF);
    }

    /** {@inheritDoc} */
    public short readShort() {
        assert buf.remaining() >= 2;

        int pos = buf.position();

        buf.position(pos + 2);

        return UNSAFE.getShort(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    public short[] readShortArray() {
        return readArray(SHORT_ARR_CREATOR, 1, SHORT_ARR_OFF);
    }

    /** {@inheritDoc} */
    public char readChar() {
        assert buf.remaining() >= 2;

        int pos = buf.position();

        buf.position(pos + 2);

        return UNSAFE.getChar(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    public char[] readCharArray() {
        return readArray(CHAR_ARR_CREATOR, 1, CHAR_ARR_OFF);
    }

    /** {@inheritDoc} */
    public int readInt() {
        assert buf.remaining() >= 4;

        int pos = buf.position();

        buf.position(pos + 4);

        return UNSAFE.getInt(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    public int[] readIntArray() {
        return readArray(INT_ARR_CREATOR, 2, INT_ARR_OFF);
    }

    /** {@inheritDoc} */
    public float readFloat() {
        assert buf.remaining() >= 4;

        int pos = buf.position();

        buf.position(pos + 4);

        return UNSAFE.getFloat(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    public float[] readFloatArray() {
        return readArray(FLOAT_ARR_CREATOR, 2, FLOAT_ARR_OFF);
    }

    /** {@inheritDoc} */
    public long readLong() {
        assert buf.remaining() >= 8;

        int pos = buf.position();

        buf.position(pos + 8);

        return UNSAFE.getLong(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    public long[] readLongArray() {
        return readArray(LONG_ARR_CREATOR, 3, LONG_ARR_OFF);
    }

    /** {@inheritDoc} */
    public double readDouble() {
        assert buf.remaining() >= 8;

        int pos = buf.position();

        buf.position(pos + 8);

        return UNSAFE.getDouble(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    public double[] readDoubleArray() {
        return readArray(DOUBLE_ARR_CREATOR, 3, DOUBLE_ARR_OFF);
    }

    /**
     * @return Message.
     */
    public GridTcpCommunicationMessageAdapter readMessage() {
        if (!msgTypeDone) {
            if (!buf.hasRemaining()) {
                lastFinished = false;

                return null;
            }

            byte type = readByte();

            msg = type == Byte.MIN_VALUE ? null : msgFactory.create(type);

            msgTypeDone = true;
        }

        if (msg == null || msg.readFrom(buf)) {
            GridTcpCommunicationMessageAdapter msg0 = msg;

            msgTypeDone = false;
            msg = null;

            lastFinished = true;

            return msg0;
        }
        else {
            lastFinished = false;

            return null;
        }
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     * @param bytes Length in bytes.
     * @return Whether array was fully written
     */
    private boolean writeArray(Object arr, long off, int len, int bytes) {
        assert arr != null;
        assert arr.getClass().isArray() && arr.getClass().getComponentType().isPrimitive();
        assert off > 0;
        assert len >= 0;
        assert bytes >= 0;
        assert bytes >= arrOff;

        if (arrOff == -1) {
            if (remaining() < 4)
                return false;

            writeInt(len);

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
        assert creator != null;

        if (tmpArr == null) {
            if (remaining() < 4) {
                lastFinished = false;

                return null;
            }

            int len = readInt();

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

        if (remaining < toRead) {
            UNSAFE.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, remaining);

            buf.position(pos + remaining);

            tmpArrOff += remaining;

            lastFinished = false;

            return null;
        }
        else {
            UNSAFE.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, toRead);

            buf.position(pos + toRead);

            T arr = (T)tmpArr;

            tmpArr = null;
            tmpArrBytes = 0;
            tmpArrOff = 0;

            lastFinished = true;

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
}
