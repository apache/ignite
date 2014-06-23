/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

import org.gridgain.grid.util.*;
import sun.misc.*;

import static java.nio.ByteOrder.*;

/**
 * Primitives writer.
 */
abstract class GridPortablePrimitives {
    /** */
    private static final GridPortablePrimitives INSTANCE =
        nativeOrder() == LITTLE_ENDIAN ? new UnsafePrimitives() : new BytePrimitives();

    /**
     * @return Primitives writer.
     */
    static GridPortablePrimitives get() {
        return INSTANCE;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeByte(byte[] arr, int off, byte val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract byte readByte(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeShort(byte[] arr, int off, short val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract short readShort(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeInt(byte[] arr, int off, int val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract int readInt(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeLong(byte[] arr, int off, long val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract long readLong(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeFloat(byte[] arr, int off, float val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract float readFloat(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeDouble(byte[] arr, int off, double val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract double readDouble(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeChar(byte[] arr, int off, char val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract char readChar(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeBoolean(byte[] arr, int off, boolean val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract boolean readBoolean(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeByteArray(byte[] arr, int off, byte[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract byte[] readByteArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeShortArray(byte[] arr, int off, short[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract short[] readShortArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeIntArray(byte[] arr, int off, int[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract int[] readIntArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeLongArray(byte[] arr, int off, long[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract long[] readLongArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeFloatArray(byte[] arr, int off, float[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract float[] readFloatArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeDoubleArray(byte[] arr, int off, double[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract double[] readDoubleArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeCharArray(byte[] arr, int off, char[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract char[] readCharArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    abstract void writeBooleanArray(byte[] arr, int off, boolean[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    abstract boolean[] readBooleanArray(byte[] arr, int off, int len);

    /** */
    private static class UnsafePrimitives extends GridPortablePrimitives {
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

        /** {@inheritDoc} */
        @Override void writeByte(byte[] arr, int off, byte val) {
            UNSAFE.putByte(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override byte readByte(byte[] arr, int off) {
            return UNSAFE.getByte(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override void writeShort(byte[] arr, int off, short val) {
            UNSAFE.putShort(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override short readShort(byte[] arr, int off) {
            return UNSAFE.getShort(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override void writeInt(byte[] arr, int off, int val) {
            UNSAFE.putInt(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override int readInt(byte[] arr, int off) {
            return UNSAFE.getInt(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override void writeLong(byte[] arr, int off, long val) {
            UNSAFE.putLong(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override long readLong(byte[] arr, int off) {
            return UNSAFE.getLong(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override void writeFloat(byte[] arr, int off, float val) {
            UNSAFE.putFloat(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override float readFloat(byte[] arr, int off) {
            return UNSAFE.getFloat(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override void writeDouble(byte[] arr, int off, double val) {
            UNSAFE.putDouble(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override double readDouble(byte[] arr, int off) {
            return UNSAFE.getDouble(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override void writeChar(byte[] arr, int off, char val) {
            UNSAFE.putChar(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override char readChar(byte[] arr, int off) {
            return UNSAFE.getChar(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override void writeBoolean(byte[] arr, int off, boolean val) {
            UNSAFE.putBoolean(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override boolean readBoolean(byte[] arr, int off) {
            return UNSAFE.getBoolean(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override void writeByteArray(byte[] arr, int off, byte[] val) {
            UNSAFE.copyMemory(val, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length);
        }

        /** {@inheritDoc} */
        @Override byte[] readByteArray(byte[] arr, int off, int len) {
            byte[] arr0 = new byte[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, BYTE_ARR_OFF, len);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override void writeShortArray(byte[] arr, int off, short[] val) {
            UNSAFE.copyMemory(val, SHORT_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 1);
        }

        /** {@inheritDoc} */
        @Override short[] readShortArray(byte[] arr, int off, int len) {
            short[] arr0 = new short[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, SHORT_ARR_OFF, len << 1);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override void writeIntArray(byte[] arr, int off, int[] val) {
            UNSAFE.copyMemory(val, INT_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 2);
        }

        /** {@inheritDoc} */
        @Override int[] readIntArray(byte[] arr, int off, int len) {
            int[] arr0 = new int[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, INT_ARR_OFF, len << 2);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override void writeLongArray(byte[] arr, int off, long[] val) {
            UNSAFE.copyMemory(val, LONG_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 3);
        }

        /** {@inheritDoc} */
        @Override long[] readLongArray(byte[] arr, int off, int len) {
            long[] arr0 = new long[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, LONG_ARR_OFF, len << 3);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override void writeFloatArray(byte[] arr, int off, float[] val) {
            UNSAFE.copyMemory(val, FLOAT_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 2);
        }

        /** {@inheritDoc} */
        @Override float[] readFloatArray(byte[] arr, int off, int len) {
            float[] arr0 = new float[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, FLOAT_ARR_OFF, len << 2);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override void writeDoubleArray(byte[] arr, int off, double[] val) {
            UNSAFE.copyMemory(val, DOUBLE_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 3);
        }

        /** {@inheritDoc} */
        @Override double[] readDoubleArray(byte[] arr, int off, int len) {
            double[] arr0 = new double[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, DOUBLE_ARR_OFF, len << 3);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override void writeCharArray(byte[] arr, int off, char[] val) {
            UNSAFE.copyMemory(val, CHAR_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 1);
        }

        /** {@inheritDoc} */
        @Override char[] readCharArray(byte[] arr, int off, int len) {
            char[] arr0 = new char[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, CHAR_ARR_OFF, len << 1);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override void writeBooleanArray(byte[] arr, int off, boolean[] val) {
            UNSAFE.copyMemory(val, BOOLEAN_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length);
        }

        /** {@inheritDoc} */
        @Override boolean[] readBooleanArray(byte[] arr, int off, int len) {
            boolean[] arr0 = new boolean[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, BOOLEAN_ARR_OFF, len);

            return arr0;
        }
    }

    /** */
    private static class BytePrimitives extends GridPortablePrimitives {
        /** {@inheritDoc} */
        @Override void writeByte(byte[] arr, int off, byte val) {
            arr[off] = val;
        }

        /** {@inheritDoc} */
        @Override byte readByte(byte[] arr, int off) {
            return arr[off];
        }

        /** {@inheritDoc} */
        @Override void writeShort(byte[] arr, int off, short val) {
            arr[off++] = (byte)(val & 0xff);
            arr[off] = (byte)((val >>> 8) & 0xff);
        }

        /** {@inheritDoc} */
        @Override short readShort(byte[] arr, int off) {
            short val = 0;

            val |= (arr[off++] & 0xff);
            val |= (arr[off] & 0xff) << 8;

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeInt(byte[] arr, int off, int val) {
            arr[off++] = (byte)(val & 0xff);
            arr[off++] = (byte)((val >>> 8) & 0xff);
            arr[off++] = (byte)((val >>> 16) & 0xff);
            arr[off] = (byte)((val >>> 24) & 0xff);
        }

        /** {@inheritDoc} */
        @Override int readInt(byte[] arr, int off) {
            int val = 0;

            val |= (arr[off++] & 0xff);
            val |= (arr[off++] & 0xff) << 8;
            val |= (arr[off++] & 0xff) << 16;
            val |= (arr[off] & 0xff) << 24;

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeLong(byte[] arr, int off, long val) {
            arr[off++] = (byte)(val & 0xffL);
            arr[off++] = (byte)((val >>> 8) & 0xffL);
            arr[off++] = (byte)((val >>> 16) & 0xffL);
            arr[off++] = (byte)((val >>> 24) & 0xffL);
            arr[off++] = (byte)((val >>> 32) & 0xffL);
            arr[off++] = (byte)((val >>> 40) & 0xffL);
            arr[off++] = (byte)((val >>> 48) & 0xffL);
            arr[off] = (byte)((val >>> 56) & 0xffL);
        }

        /** {@inheritDoc} */
        @Override long readLong(byte[] arr, int off) {
            long val = 0;

            val |= (arr[off++] & 0xffL);
            val |= (arr[off++] & 0xffL) << 8;
            val |= (arr[off++] & 0xffL) << 16;
            val |= (arr[off++] & 0xffL) << 24;
            val |= (arr[off++] & 0xffL) << 32;
            val |= (arr[off++] & 0xffL) << 40;
            val |= (arr[off++] & 0xffL) << 48;
            val |= (arr[off] & 0xffL) << 56;

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeFloat(byte[] arr, int off, float val) {
            writeInt(arr, off, Float.floatToIntBits(val));
        }

        /** {@inheritDoc} */
        @Override float readFloat(byte[] arr, int off) {
            return Float.intBitsToFloat(readInt(arr, off));
        }

        /** {@inheritDoc} */
        @Override void writeDouble(byte[] arr, int off, double val) {
            writeLong(arr, off, Double.doubleToLongBits(val));
        }

        /** {@inheritDoc} */
        @Override double readDouble(byte[] arr, int off) {
            return Double.longBitsToDouble(readLong(arr, off));
        }

        /** {@inheritDoc} */
        @Override void writeChar(byte[] arr, int off, char val) {
            arr[off++] = (byte)(val & 0xff);
            arr[off] = (byte)((val >>> 8) & 0xff);
        }

        /** {@inheritDoc} */
        @Override char readChar(byte[] arr, int off) {
            char val = 0;

            val |= (arr[off++] & 0xff);
            val |= (arr[off] & 0xff) << 8;

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeBoolean(byte[] arr, int off, boolean val) {
            arr[off] = (byte)(val ? 1 : 0);
        }

        /** {@inheritDoc} */
        @Override boolean readBoolean(byte[] arr, int off) {
            return arr[off] != 0;
        }

        /** {@inheritDoc} */
        @Override void writeByteArray(byte[] arr, int off, byte[] val) {
            for (byte b : val)
                arr[off++] = b;
        }

        /** {@inheritDoc} */
        @Override byte[] readByteArray(byte[] arr, int off, int len) {
            byte[] val = new byte[len];

            for (int i = 0; i < len; i++)
                val[i] = arr[off++];

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeShortArray(byte[] arr, int off, short[] val) {
            for (short s : val) {
                writeShort(arr, off, s);

                off += 2;
            }
        }

        /** {@inheritDoc} */
        @Override short[] readShortArray(byte[] arr, int off, int len) {
            short[] val = new short[len];

            for (int i = 0; i < len; i++) {
                val[i] = readShort(arr, off);

                off += 2;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeIntArray(byte[] arr, int off, int[] val) {
            for (int i : val) {
                writeInt(arr, off, i);

                off += 4;
            }
        }

        /** {@inheritDoc} */
        @Override int[] readIntArray(byte[] arr, int off, int len) {
            int[] val = new int[len];

            for (int i = 0; i < len; i++) {
                val[i] = readInt(arr, off);

                off += 4;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeLongArray(byte[] arr, int off, long[] val) {
            for (long l : val) {
                writeLong(arr, off, l);

                off += 8;
            }
        }

        /** {@inheritDoc} */
        @Override long[] readLongArray(byte[] arr, int off, int len) {
            long[] val = new long[len];

            for (int i = 0; i < len; i++) {
                val[i] = readLong(arr, off);

                off += 8;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeFloatArray(byte[] arr, int off, float[] val) {
            for (float f : val) {
                writeFloat(arr, off, f);

                off += 4;
            }
        }

        /** {@inheritDoc} */
        @Override float[] readFloatArray(byte[] arr, int off, int len) {
            float[] val = new float[len];

            for (int i = 0; i < len; i++) {
                val[i] = readFloat(arr, off);

                off += 4;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeDoubleArray(byte[] arr, int off, double[] val) {
            for (double d : val) {
                writeDouble(arr, off, d);

                off += 8;
            }
        }

        /** {@inheritDoc} */
        @Override double[] readDoubleArray(byte[] arr, int off, int len) {
            double[] val = new double[len];

            for (int i = 0; i < len; i++) {
                val[i] = readDouble(arr, off);

                off += 8;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeCharArray(byte[] arr, int off, char[] val) {
            for (char c : val) {
                writeChar(arr, off, c);

                off += 2;
            }
        }

        /** {@inheritDoc} */
        @Override char[] readCharArray(byte[] arr, int off, int len) {
            char[] val = new char[len];

            for (int i = 0; i < len; i++) {
                val[i] = readChar(arr, off);

                off += 2;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override void writeBooleanArray(byte[] arr, int off, boolean[] val) {
            for (boolean b : val)
                writeBoolean(arr, off++, b);
        }

        /** {@inheritDoc} */
        @Override boolean[] readBooleanArray(byte[] arr, int off, int len) {
            boolean[] val = new boolean[len];

            for (int i = 0; i < len; i++)
                val[i] = readBoolean(arr, off++);

            return val;
        }
    }
}
