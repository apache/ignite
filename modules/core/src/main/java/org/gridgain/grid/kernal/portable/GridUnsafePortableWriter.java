/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;

/**
 * Portable writer implementation based on {@code sun.misc.Unsafe}.
 */
class GridUnsafePortableWriter extends GridPortableWriterAdapter {
    @Override public void doWriteByte(byte val) throws GridPortableException {
        UNSAFE.putByte(arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(1), val);
    }

    @Override public void doWriteShort(short val) throws GridPortableException {
        UNSAFE.putShort(arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(2), val);
    }

    @Override public void doWriteInt(int val) throws GridPortableException {
        UNSAFE.putInt(arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(4), val);
    }

    @Override public void doWriteLong(long val) throws GridPortableException {
        UNSAFE.putLong(arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(8), val);
    }

    @Override public void doWriteFloat(float val) throws GridPortableException {
        UNSAFE.putFloat(arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(4), val);
    }

    @Override public void doWriteDouble(double val) throws GridPortableException {
        UNSAFE.putDouble(arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(8), val);
    }

    @Override public void doWriteChar(char val) throws GridPortableException {
        UNSAFE.putChar(arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(2), val);
    }

    @Override public void doWriteBoolean(boolean val) throws GridPortableException {
        UNSAFE.putBoolean(arr.array(), BYTE_ARR_OFF + arr.requestFreeSize(1), val);
    }
}
