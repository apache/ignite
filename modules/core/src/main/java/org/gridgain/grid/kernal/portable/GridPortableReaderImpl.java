/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.util.*;

/**
 * Portable reader implementation.
 */
class GridPortableReaderImpl implements GridPortableReader {
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
    private static final int INIT_RAW_OFF = 6;

    /** */
    private final byte[] arr;

    /** */
    private int rawOff = 6;

    /**
     * @param arr Array.
     */
    GridPortableReaderImpl(byte[] arr) {
        this.arr = arr;
    }

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
        return new byte[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] readByteArray() throws GridPortableException {
        return new byte[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray(String fieldName) throws GridPortableException {
        return new short[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public short[] readShortArray() throws GridPortableException {
        return new short[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray(String fieldName) throws GridPortableException {
        return new int[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public int[] readIntArray() throws GridPortableException {
        return new int[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray(String fieldName) throws GridPortableException {
        return new long[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public long[] readLongArray() throws GridPortableException {
        return new long[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray(String fieldName) throws GridPortableException {
        return new float[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public float[] readFloatArray() throws GridPortableException {
        return new float[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray(String fieldName) throws GridPortableException {
        return new double[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public double[] readDoubleArray() throws GridPortableException {
        return new double[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray(String fieldName) throws GridPortableException {
        return new char[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public char[] readCharArray() throws GridPortableException {
        return new char[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray(String fieldName) throws GridPortableException {
        return new boolean[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean[] readBooleanArray() throws GridPortableException {
        return new boolean[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray(String fieldName) throws GridPortableException {
        return new String[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public String[] readStringArray() throws GridPortableException {
        return new String[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray(String fieldName) throws GridPortableException {
        return new UUID[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public UUID[] readUuidArray() throws GridPortableException {
        return new UUID[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray(String fieldName) throws GridPortableException {
        return new Object[0]; // TODO: implement.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object[] readObjectArray() throws GridPortableException {
        return new Object[0]; // TODO: implement.
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
