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

/**
 * Primitives writer.
 */
abstract class GridPortablePrimitives {
    /** */
    // TODO: Other options.
    private static final GridPortablePrimitives INSTANCE = new UnsafePrimitives();

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

    /** */
    private static class UnsafePrimitives extends GridPortablePrimitives {
        /** */
        private static final Unsafe UNSAFE = GridUnsafe.unsafe();

        /** */
        private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

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
    }
}
