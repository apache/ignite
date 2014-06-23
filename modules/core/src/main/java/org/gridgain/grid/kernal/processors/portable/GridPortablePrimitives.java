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
    // TODO: Implement.
    private static class BytePrimitives extends GridPortablePrimitives {
        /** {@inheritDoc} */
        @Override void writeByte(byte[] arr, int off, byte val) {

        }

        /** {@inheritDoc} */
        @Override byte readByte(byte[] arr, int off) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override void writeShort(byte[] arr, int off, short val) {

        }

        /** {@inheritDoc} */
        @Override short readShort(byte[] arr, int off) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override void writeInt(byte[] arr, int off, int val) {

        }

        /** {@inheritDoc} */
        @Override int readInt(byte[] arr, int off) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override void writeLong(byte[] arr, int off, long val) {

        }

        /** {@inheritDoc} */
        @Override long readLong(byte[] arr, int off) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override void writeFloat(byte[] arr, int off, float val) {

        }

        /** {@inheritDoc} */
        @Override float readFloat(byte[] arr, int off) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override void writeDouble(byte[] arr, int off, double val) {

        }

        /** {@inheritDoc} */
        @Override double readDouble(byte[] arr, int off) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override void writeChar(byte[] arr, int off, char val) {

        }

        /** {@inheritDoc} */
        @Override char readChar(byte[] arr, int off) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override void writeBoolean(byte[] arr, int off, boolean val) {

        }

        /** {@inheritDoc} */
        @Override boolean readBoolean(byte[] arr, int off) {
            return false;
        }

        /** {@inheritDoc} */
        @Override void writeByteArray(byte[] arr, int off, byte[] val) {

        }

        /** {@inheritDoc} */
        @Override byte[] readByteArray(byte[] arr, int off, int len) {
            return null;
        }

        /** {@inheritDoc} */
        @Override void writeShortArray(byte[] arr, int off, short[] val) {

        }

        /** {@inheritDoc} */
        @Override short[] readShortArray(byte[] arr, int off, int len) {
            return null;
        }

        /** {@inheritDoc} */
        @Override void writeIntArray(byte[] arr, int off, int[] val) {

        }

        /** {@inheritDoc} */
        @Override int[] readIntArray(byte[] arr, int off, int len) {
            return null;
        }

        /** {@inheritDoc} */
        @Override void writeLongArray(byte[] arr, int off, long[] val) {

        }

        /** {@inheritDoc} */
        @Override long[] readLongArray(byte[] arr, int off, int len) {
            return null;
        }

        /** {@inheritDoc} */
        @Override void writeFloatArray(byte[] arr, int off, float[] val) {

        }

        /** {@inheritDoc} */
        @Override float[] readFloatArray(byte[] arr, int off, int len) {
            return null;
        }

        /** {@inheritDoc} */
        @Override void writeDoubleArray(byte[] arr, int off, double[] val) {

        }

        /** {@inheritDoc} */
        @Override double[] readDoubleArray(byte[] arr, int off, int len) {
            return null;
        }

        /** {@inheritDoc} */
        @Override void writeCharArray(byte[] arr, int off, char[] val) {

        }

        /** {@inheritDoc} */
        @Override char[] readCharArray(byte[] arr, int off, int len) {
            return null;
        }

        /** {@inheritDoc} */
        @Override void writeBooleanArray(byte[] arr, int off, boolean[] val) {

        }

        /** {@inheritDoc} */
        @Override boolean[] readBooleanArray(byte[] arr, int off, int len) {
            return null;
        }
    }
}
