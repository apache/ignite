/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.util.*;
import sun.misc.*;

/**
 * Primitives writer.
 */
abstract class GridPortablePrimitivesWriter {
    /** */
    private static final GridPortablePrimitivesWriter INSTANCE = new UnsafeWriter();

    /**
     * @return Primitives writer.
     */
    static GridPortablePrimitivesWriter get() {
        return INSTANCE;
    }

    /**
     * @param val Value.
     */
    abstract void writeByte(byte[] arr, int off, byte val);

    /**
     * @param val Value.
     */
    abstract void writeShort(byte[] arr, int off, short val);

    /**
     * @param val Value.
     */
    abstract void writeInt(byte[] arr, int off, int val);

    /**
     * @param val Value.
     */
    abstract void writeLong(byte[] arr, int off, long val);

    /**
     * @param val Value.
     */
    abstract void writeFloat(byte[] arr, int off, float val);

    /**
     * @param val Value.
     */
    abstract void writeDouble(byte[] arr, int off, double val);

    /**
     * @param val Value.
     */
    abstract void writeChar(byte[] arr, int off, char val);

    /**
     * @param val Value.
     */
    abstract void writeBoolean(byte[] arr, int off, boolean val);

    /** */
    private static class UnsafeWriter extends GridPortablePrimitivesWriter {
        /** */
        protected static final Unsafe UNSAFE = GridUnsafe.unsafe();

        /** */
        protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

        /** {@inheritDoc} */
        @Override void writeByte(byte[] arr, int off, byte val) {
            UNSAFE.putByte(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override void writeShort(byte[] arr, int off, short val) {
            UNSAFE.putShort(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override void writeInt(byte[] arr, int off, int val) {
            UNSAFE.putInt(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override void writeLong(byte[] arr, int off, long val) {
            UNSAFE.putLong(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override void writeFloat(byte[] arr, int off, float val) {
            UNSAFE.putFloat(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override void writeDouble(byte[] arr, int off, double val) {
            UNSAFE.putDouble(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override void writeChar(byte[] arr, int off, char val) {
            UNSAFE.putChar(arr, BYTE_ARR_OFF + off, val);
        }

        /** {@inheritDoc} */
        @Override void writeBoolean(byte[] arr, int off, boolean val) {
            UNSAFE.putBoolean(arr, BYTE_ARR_OFF + off, val);
        }
    }
}
