/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Portable reader implementation based on {@code sun.misc.Unsafe}.
 */
class GridUnsafePortableReader extends GridPortableReaderAdapter {
    /** {@inheritDoc} */
    @Override public byte readByte(String fieldName) throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public short readShort(String fieldName) throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public int readInt(String fieldName) throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public long readLong(String fieldName) throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public float readFloat(String fieldName) throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public double readDouble(String fieldName) throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public char readChar(String fieldName) throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws GridPortableException {
        return 0; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean(String fieldName) throws GridPortableException {
        return false; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws GridPortableException {
        return false; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public String readString() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID readUuid() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T readObject(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T readObject() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> Collection<T> readCollection() throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap(String fieldName) throws GridPortableException {
        return null; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> Map<K, V> readMap() throws GridPortableException {
        return null; // TODO: implement.
    }
}
