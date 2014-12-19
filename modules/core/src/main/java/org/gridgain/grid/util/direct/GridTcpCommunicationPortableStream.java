/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.direct;

import org.gridgain.grid.kernal.processors.portable.*;
import org.gridgain.grid.util.*;
import sun.misc.*;
import sun.nio.ch.*;

import java.nio.*;

import static org.gridgain.grid.util.direct.GridTcpCommunicationMessageAdapter.*;

/**
 * Portable stream based on {@link ByteBuffer}.
 */
public class GridTcpCommunicationPortableStream implements GridPortableOutputStream, GridPortableInputStream {
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
    private ByteBuffer buf;

    /** */
    private byte[] heapArr;

    /** */
    private long baseOff;

    /** */
    private int arrOff;

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
    private boolean lastWritten;

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
     * @return Whether last object was fully written.
     */
    public boolean lastWritten() {
        return lastWritten;
    }

    /**
     * @param msg Message.
     */
    public void writeMessage(GridTcpCommunicationMessageAdapter msg) {
        assert msg != null;

        lastWritten = msg.writeTo(buf);
    }

    /**
     * @return Message.
     */
    public GridTcpCommunicationMessageAdapter readMessage() {
        if (!msgTypeDone) {
            assert buf.hasRemaining();

            byte type = readByte();

            msg = type == Byte.MIN_VALUE ? null : GridTcpCommunicationMessageFactory.create(type);

            msgTypeDone = true;
        }

        if (msg == null || msg.readFrom(buf)) {
            GridTcpCommunicationMessageAdapter msg0 = msg;

            msgTypeDone = false;
            msg = null;

            return msg0;
        }
        else
            return MSG_NOT_READ;
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) {
        int pos = buf.position();

        UNSAFE.putByte(heapArr, baseOff + pos, val);

        buf.position(pos + 1);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val) {
        assert val != null;

        lastWritten = writeArray(val, BYTE_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) {
        int pos = buf.position();

        UNSAFE.putBoolean(heapArr, baseOff + pos, val);

        buf.position(pos + 1);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(boolean[] val) {
        assert val != null;

        lastWritten = writeArray(val, BOOLEAN_ARR_OFF, val.length);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        int pos = buf.position();

        UNSAFE.putShort(heapArr, baseOff + pos, val);

        buf.position(pos + 2);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        assert val != null;

        lastWritten = writeArray(val, SHORT_ARR_OFF, val.length << 1);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        int pos = buf.position();

        UNSAFE.putChar(heapArr, baseOff + pos, val);

        buf.position(pos + 2);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        assert val != null;

        lastWritten = writeArray(val, CHAR_ARR_OFF, val.length << 1);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        int pos = buf.position();

        UNSAFE.putInt(heapArr, baseOff + pos, val);

        buf.position(pos + 4);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        assert val != null;

        lastWritten = writeArray(val, INT_ARR_OFF, val.length << 2);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) {
        int pos = buf.position();

        UNSAFE.putFloat(heapArr, baseOff + pos, val);

        buf.position(pos + 4);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        assert val != null;

        lastWritten = writeArray(val, FLOAT_ARR_OFF, val.length << 2);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        int pos = buf.position();

        UNSAFE.putLong(heapArr, baseOff + pos, val);

        buf.position(pos + 8);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        assert val != null;

        lastWritten = writeArray(val, LONG_ARR_OFF, val.length << 3);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) {
        int pos = buf.position();

        UNSAFE.putDouble(heapArr, baseOff + pos, val);

        buf.position(pos + 8);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        assert val != null;

        lastWritten = writeArray(val, DOUBLE_ARR_OFF, val.length << 3);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] arr, int off, int len) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void write(long addr, int cnt) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public byte readByte() {
        assert buf.hasRemaining();

        int pos = buf.position();

        buf.position(pos + 1);

        return UNSAFE.getByte(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    @Override public byte[] readByteArray(int cnt) {
        return readArray(BYTE_ARR_CREATOR, cnt, 0, BYTE_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() {
        assert buf.hasRemaining();

        int pos = buf.position();

        buf.position(pos + 1);

        return UNSAFE.getBoolean(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    @Override public boolean[] readBooleanArray(int cnt) {
        return readArray(BOOLEAN_ARR_CREATOR, cnt, 0, BOOLEAN_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public short readShort() {
        assert buf.remaining() >= 2;

        int pos = buf.position();

        buf.position(pos + 2);

        return UNSAFE.getShort(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray(int cnt) {
        return readArray(SHORT_ARR_CREATOR, cnt, 1, SHORT_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public char readChar() {
        assert buf.remaining() >= 2;

        int pos = buf.position();

        buf.position(pos + 2);

        return UNSAFE.getChar(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray(int cnt) {
        return readArray(CHAR_ARR_CREATOR, cnt, 1, CHAR_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public int readInt() {
        assert buf.remaining() >= 4;

        int pos = buf.position();

        buf.position(pos + 4);

        return UNSAFE.getInt(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    @Override public int readInt(int pos) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray(int cnt) {
        return readArray(INT_ARR_CREATOR, cnt, 2, INT_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public float readFloat() {
        assert buf.remaining() >= 4;

        int pos = buf.position();

        buf.position(pos + 4);

        return UNSAFE.getFloat(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray(int cnt) {
        return readArray(FLOAT_ARR_CREATOR, cnt, 2, FLOAT_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public long readLong() {
        assert buf.remaining() >= 8;

        int pos = buf.position();

        buf.position(pos + 8);

        return UNSAFE.getLong(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray(int cnt) {
        return readArray(LONG_ARR_CREATOR, cnt, 3, LONG_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public double readDouble() {
        assert buf.remaining() >= 8;

        int pos = buf.position();

        buf.position(pos + 8);

        return UNSAFE.getDouble(heapArr, baseOff + pos);
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray(int cnt) {
        return readArray(DOUBLE_ARR_CREATOR, cnt, 3, DOUBLE_ARR_OFF);
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] arr, int off, int len) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int read(long addr, int len) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int position() {
        return buf.position();
    }

    /** {@inheritDoc} */
    @Override public void position(int pos) {
        buf.position(pos);
    }

    /** {@inheritDoc} */
    public int remaining() {
        return buf.remaining();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public byte[] arrayCopy() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param bytes Length in bytes.
     * @return Whether array was fully written
     */
    private boolean writeArray(Object arr, long off, int bytes) {
        assert arr != null;
        assert arr.getClass().isArray() && arr.getClass().getComponentType().isPrimitive();
        assert off > 0;
        assert bytes >= 0;
        assert bytes >= arrOff;

        if (!buf.hasRemaining())
            return false;

        int toWrite = bytes - arrOff;
        int pos = buf.position();
        int remaining = buf.remaining();

        if (toWrite <= remaining) {
            UNSAFE.copyMemory(arr, off + arrOff, heapArr, baseOff + pos, toWrite);

            pos += toWrite;

            buf.position(pos);

            arrOff = 0;

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
     * @param len Array Array length.
     * @param lenShift Array length shift size.
     * @param off Base offset.
     * @return Array or special value if it was not fully read.
     */
    private <T> T readArray(ArrayCreator<T> creator, int len, int lenShift, long off) {
        assert creator != null;

        if (tmpArr == null) {
            switch (len) {
                case -1:
                    return null;

                case 0:
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

            return creator.create(-1);
        }
        else {
            UNSAFE.copyMemory(heapArr, baseOff + pos, tmpArr, off + tmpArrOff, toRead);

            buf.position(pos + toRead);

            T arr = (T)tmpArr;

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
}
