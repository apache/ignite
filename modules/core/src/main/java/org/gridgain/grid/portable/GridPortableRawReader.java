/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portable;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Reader for portable objects.
 */
public interface GridPortableRawReader {
    /**
     * @return Byte value.
     * @throws GridPortableException In case of error.
     */
    public byte readByte() throws GridPortableException;

    /**
     * @return Short value.
     * @throws GridPortableException In case of error.
     */
    public short readShort() throws GridPortableException;

    /**
     * @return Integer value.
     * @throws GridPortableException In case of error.
     */
    public int readInt() throws GridPortableException;

    /**
     * @return Long value.
     * @throws GridPortableException In case of error.
     */
    public long readLong() throws GridPortableException;

    /**
     * @throws GridPortableException In case of error.
     * @return Float value.
     */
    public float readFloat() throws GridPortableException;

    /**
     * @return Double value.
     * @throws GridPortableException In case of error.
     */
    public double readDouble() throws GridPortableException;

    /**
     * @return Char value.
     * @throws GridPortableException In case of error.
     */
    public char readChar() throws GridPortableException;

    /**
     * @return Boolean value.
     * @throws GridPortableException In case of error.
     */
    public boolean readBoolean() throws GridPortableException;

    /**
     * @return String value.
     * @throws GridPortableException In case of error.
     */
    @Nullable public String readString() throws GridPortableException;

    /**
     * @return UUID.
     * @throws GridPortableException In case of error.
     */
    @Nullable public UUID readUuid() throws GridPortableException;

    /**
     * @return Date.
     * @throws GridPortableException In case of error.
     */
    @Nullable public Date readDate() throws GridPortableException;

    /**
     * @return Object.
     * @throws GridPortableException In case of error.
     */
    @Nullable public Object readObject() throws GridPortableException;

    /**
     * @return Portable object.
     * @throws GridPortableException In case of error.
     */
    @Nullable public GridPortableObject readPortable() throws GridPortableException;

    /**
     * @return Byte array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public byte[] readByteArray() throws GridPortableException;

    /**
     * @return Short array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public short[] readShortArray() throws GridPortableException;

    /**
     * @return Integer array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public int[] readIntArray() throws GridPortableException;

    /**
     * @return Long array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public long[] readLongArray() throws GridPortableException;

    /**
     * @return Float array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public float[] readFloatArray() throws GridPortableException;

    /**
     * @return Byte array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public double[] readDoubleArray() throws GridPortableException;

    /**
     * @return Char array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public char[] readCharArray() throws GridPortableException;

    /**
     * @return Boolean array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public boolean[] readBooleanArray() throws GridPortableException;

    /**
     * @return String array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public String[] readStringArray() throws GridPortableException;

    /**
     * @return UUID array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public UUID[] readUuidArray() throws GridPortableException;

    /**
     * @return Date array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public Date[] readDateArray() throws GridPortableException;

    /**
     * @return Object array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public Object[] readObjectArray() throws GridPortableException;

    /**
     * @return Collection.
     * @throws GridPortableException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection() throws GridPortableException;

    /**
     * @param colCls Collection class.
     * @return Collection.
     * @throws GridPortableException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(Class<? extends Collection<T>> colCls)
        throws GridPortableException;

    /**
     * @return Map.
     * @throws GridPortableException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap() throws GridPortableException;

    /**
     * @param mapCls Map class.
     * @return Map.
     * @throws GridPortableException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(Class<? extends Map<K, V>> mapCls) throws GridPortableException;
}
