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

package org.apache.ignite.internal.portable;

import org.apache.ignite.internal.util.GridUnsafe;
import sun.misc.Unsafe;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;

/**
 * Primitives writer.
 */
public abstract class PortablePrimitives {
    /** */
    private static final PortablePrimitives INSTANCE =
        nativeOrder() == LITTLE_ENDIAN ? new UnsafePrimitives() : new BytePrimitives();

    /**
     * @return Primitives writer.
     */
    public static PortablePrimitives get() {
        return INSTANCE;
    }

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeByte(byte[] arr, int off, byte val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract byte readByte(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeShort(byte[] arr, int off, short val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract short readShort(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeInt(byte[] arr, int off, int val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract int readInt(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeLong(byte[] arr, int off, long val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract long readLong(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeFloat(byte[] arr, int off, float val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract float readFloat(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeDouble(byte[] arr, int off, double val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract double readDouble(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeChar(byte[] arr, int off, char val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract char readChar(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeBoolean(byte[] arr, int off, boolean val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract boolean readBoolean(byte[] arr, int off);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeByteArray(byte[] arr, int off, byte[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract byte[] readByteArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeShortArray(byte[] arr, int off, short[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract short[] readShortArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeIntArray(byte[] arr, int off, int[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract int[] readIntArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeLongArray(byte[] arr, int off, long[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract long[] readLongArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeFloatArray(byte[] arr, int off, float[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract float[] readFloatArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeDoubleArray(byte[] arr, int off, double[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract double[] readDoubleArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeCharArray(byte[] arr, int off, char[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract char[] readCharArray(byte[] arr, int off, int len);

    /**
     * @param arr Array.
     * @param off Offset.
     * @param val Value.
     */
    public abstract void writeBooleanArray(byte[] arr, int off, boolean[] val);

    /**
     * @param arr Array.
     * @param off Offset.
     * @return Value.
     */
    public abstract boolean[] readBooleanArray(byte[] arr, int off, int len);

    /** */
    private static class UnsafePrimitives extends PortablePrimitives {
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
        @Override public void writeByte(byte[] arr, int off, byte val) {
            UNSAFE.putByte(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override public byte readByte(byte[] arr, int off) {
            return UNSAFE.getByte(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override public void writeShort(byte[] arr, int off, short val) {
            UNSAFE.putShort(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override public short readShort(byte[] arr, int off) {
            return UNSAFE.getShort(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override public void writeInt(byte[] arr, int off, int val) {
            UNSAFE.putInt(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override public int readInt(byte[] arr, int off) {
            return UNSAFE.getInt(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override public void writeLong(byte[] arr, int off, long val) {
            UNSAFE.putLong(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override public long readLong(byte[] arr, int off) {
            return UNSAFE.getLong(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override public void writeFloat(byte[] arr, int off, float val) {
            UNSAFE.putFloat(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override public float readFloat(byte[] arr, int off) {
            return UNSAFE.getFloat(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override public void writeDouble(byte[] arr, int off, double val) {
            UNSAFE.putDouble(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override public double readDouble(byte[] arr, int off) {
            return UNSAFE.getDouble(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override public void writeChar(byte[] arr, int off, char val) {
            UNSAFE.putChar(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override public char readChar(byte[] arr, int off) {
            return UNSAFE.getChar(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override public void writeBoolean(byte[] arr, int off, boolean val) {
            UNSAFE.putBoolean(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override public boolean readBoolean(byte[] arr, int off) {
            return UNSAFE.getBoolean(arr, BYTE_ARR_OFF + off);
        }

        /** {@inheritDoc} */
        @Override public void writeByteArray(byte[] arr, int off, byte[] val) {
            UNSAFE.copyMemory(val, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length);
        }

        /** {@inheritDoc} */
        @Override public byte[] readByteArray(byte[] arr, int off, int len) {
            byte[] arr0 = new byte[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, BYTE_ARR_OFF, len);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override public void writeShortArray(byte[] arr, int off, short[] val) {
            UNSAFE.copyMemory(val, SHORT_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 1);
        }

        /** {@inheritDoc} */
        @Override public short[] readShortArray(byte[] arr, int off, int len) {
            short[] arr0 = new short[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, SHORT_ARR_OFF, len << 1);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override public void writeIntArray(byte[] arr, int off, int[] val) {
            UNSAFE.copyMemory(val, INT_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 2);
        }

        /** {@inheritDoc} */
        @Override public int[] readIntArray(byte[] arr, int off, int len) {
            int[] arr0 = new int[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, INT_ARR_OFF, len << 2);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override public void writeLongArray(byte[] arr, int off, long[] val) {
            UNSAFE.copyMemory(val, LONG_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 3);
        }

        /** {@inheritDoc} */
        @Override public long[] readLongArray(byte[] arr, int off, int len) {
            long[] arr0 = new long[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, LONG_ARR_OFF, len << 3);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override public void writeFloatArray(byte[] arr, int off, float[] val) {
            UNSAFE.copyMemory(val, FLOAT_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 2);
        }

        /** {@inheritDoc} */
        @Override public float[] readFloatArray(byte[] arr, int off, int len) {
            float[] arr0 = new float[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, FLOAT_ARR_OFF, len << 2);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override public void writeDoubleArray(byte[] arr, int off, double[] val) {
            UNSAFE.copyMemory(val, DOUBLE_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 3);
        }

        /** {@inheritDoc} */
        @Override public double[] readDoubleArray(byte[] arr, int off, int len) {
            double[] arr0 = new double[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, DOUBLE_ARR_OFF, len << 3);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override public void writeCharArray(byte[] arr, int off, char[] val) {
            UNSAFE.copyMemory(val, CHAR_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length << 1);
        }

        /** {@inheritDoc} */
        @Override public char[] readCharArray(byte[] arr, int off, int len) {
            char[] arr0 = new char[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, CHAR_ARR_OFF, len << 1);

            return arr0;
        }

        /** {@inheritDoc} */
        @Override public void writeBooleanArray(byte[] arr, int off, boolean[] val) {
            UNSAFE.copyMemory(val, BOOLEAN_ARR_OFF, arr, BYTE_ARR_OFF + off, val.length);
        }

        /** {@inheritDoc} */
        @Override public boolean[] readBooleanArray(byte[] arr, int off, int len) {
            boolean[] arr0 = new boolean[len];

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF + off, arr0, BOOLEAN_ARR_OFF, len);

            return arr0;
        }
    }

    /** */
    private static class BytePrimitives extends PortablePrimitives {
        /** {@inheritDoc} */
        @Override public void writeByte(byte[] arr, int off, byte val) {
            arr[off] = val;
        }

        /** {@inheritDoc} */
        @Override public byte readByte(byte[] arr, int off) {
            return arr[off];
        }

        /** {@inheritDoc} */
        @Override public void writeShort(byte[] arr, int off, short val) {
            arr[off++] = (byte)(val & 0xff);
            arr[off] = (byte)((val >>> 8) & 0xff);
        }

        /** {@inheritDoc} */
        @Override public short readShort(byte[] arr, int off) {
            short val = 0;

            val |= (arr[off++] & 0xff);
            val |= (arr[off] & 0xff) << 8;

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeInt(byte[] arr, int off, int val) {
            arr[off++] = (byte)(val & 0xff);
            arr[off++] = (byte)((val >>> 8) & 0xff);
            arr[off++] = (byte)((val >>> 16) & 0xff);
            arr[off] = (byte)((val >>> 24) & 0xff);
        }

        /** {@inheritDoc} */
        @Override public int readInt(byte[] arr, int off) {
            int val = 0;

            val |= (arr[off++] & 0xff);
            val |= (arr[off++] & 0xff) << 8;
            val |= (arr[off++] & 0xff) << 16;
            val |= (arr[off] & 0xff) << 24;

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeLong(byte[] arr, int off, long val) {
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
        @Override public long readLong(byte[] arr, int off) {
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
        @Override public void writeFloat(byte[] arr, int off, float val) {
            writeInt(arr, off, Float.floatToIntBits(val));
        }

        /** {@inheritDoc} */
        @Override public float readFloat(byte[] arr, int off) {
            return Float.intBitsToFloat(readInt(arr, off));
        }

        /** {@inheritDoc} */
        @Override public void writeDouble(byte[] arr, int off, double val) {
            writeLong(arr, off, Double.doubleToLongBits(val));
        }

        /** {@inheritDoc} */
        @Override public double readDouble(byte[] arr, int off) {
            return Double.longBitsToDouble(readLong(arr, off));
        }

        /** {@inheritDoc} */
        @Override public void writeChar(byte[] arr, int off, char val) {
            arr[off++] = (byte)(val & 0xff);
            arr[off] = (byte)((val >>> 8) & 0xff);
        }

        /** {@inheritDoc} */
        @Override public char readChar(byte[] arr, int off) {
            char val = 0;

            val |= (arr[off++] & 0xff);
            val |= (arr[off] & 0xff) << 8;

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeBoolean(byte[] arr, int off, boolean val) {
            arr[off] = (byte)(val ? 1 : 0);
        }

        /** {@inheritDoc} */
        @Override public boolean readBoolean(byte[] arr, int off) {
            return arr[off] != 0;
        }

        /** {@inheritDoc} */
        @Override public void writeByteArray(byte[] arr, int off, byte[] val) {
            for (byte b : val)
                arr[off++] = b;
        }

        /** {@inheritDoc} */
        @Override public byte[] readByteArray(byte[] arr, int off, int len) {
            byte[] val = new byte[len];

            for (int i = 0; i < len; i++)
                val[i] = arr[off++];

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeShortArray(byte[] arr, int off, short[] val) {
            for (short s : val) {
                writeShort(arr, off, s);

                off += 2;
            }
        }

        /** {@inheritDoc} */
        @Override public short[] readShortArray(byte[] arr, int off, int len) {
            short[] val = new short[len];

            for (int i = 0; i < len; i++) {
                val[i] = readShort(arr, off);

                off += 2;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeIntArray(byte[] arr, int off, int[] val) {
            for (int i : val) {
                writeInt(arr, off, i);

                off += 4;
            }
        }

        /** {@inheritDoc} */
        @Override public int[] readIntArray(byte[] arr, int off, int len) {
            int[] val = new int[len];

            for (int i = 0; i < len; i++) {
                val[i] = readInt(arr, off);

                off += 4;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeLongArray(byte[] arr, int off, long[] val) {
            for (long l : val) {
                writeLong(arr, off, l);

                off += 8;
            }
        }

        /** {@inheritDoc} */
        @Override public long[] readLongArray(byte[] arr, int off, int len) {
            long[] val = new long[len];

            for (int i = 0; i < len; i++) {
                val[i] = readLong(arr, off);

                off += 8;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeFloatArray(byte[] arr, int off, float[] val) {
            for (float f : val) {
                writeFloat(arr, off, f);

                off += 4;
            }
        }

        /** {@inheritDoc} */
        @Override public float[] readFloatArray(byte[] arr, int off, int len) {
            float[] val = new float[len];

            for (int i = 0; i < len; i++) {
                val[i] = readFloat(arr, off);

                off += 4;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeDoubleArray(byte[] arr, int off, double[] val) {
            for (double d : val) {
                writeDouble(arr, off, d);

                off += 8;
            }
        }

        /** {@inheritDoc} */
        @Override public double[] readDoubleArray(byte[] arr, int off, int len) {
            double[] val = new double[len];

            for (int i = 0; i < len; i++) {
                val[i] = readDouble(arr, off);

                off += 8;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeCharArray(byte[] arr, int off, char[] val) {
            for (char c : val) {
                writeChar(arr, off, c);

                off += 2;
            }
        }

        /** {@inheritDoc} */
        @Override public char[] readCharArray(byte[] arr, int off, int len) {
            char[] val = new char[len];

            for (int i = 0; i < len; i++) {
                val[i] = readChar(arr, off);

                off += 2;
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override public void writeBooleanArray(byte[] arr, int off, boolean[] val) {
            for (boolean b : val)
                writeBoolean(arr, off++, b);
        }

        /** {@inheritDoc} */
        @Override public boolean[] readBooleanArray(byte[] arr, int off, int len) {
            boolean[] val = new boolean[len];

            for (int i = 0; i < len; i++)
                val[i] = readBoolean(arr, off++);

            return val;
        }
    }
}