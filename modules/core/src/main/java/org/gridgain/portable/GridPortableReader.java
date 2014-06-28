/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.portable;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Reader for portable objects.
 */
public interface GridPortableReader {
    /**
     * @param fieldName Field name.
     * @return Byte value.
     * @throws GridPortableException In case of error.
     */
    public byte readByte(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Short value.
     * @throws GridPortableException In case of error.
     */
    public short readShort(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Integer value.
     * @throws GridPortableException In case of error.
     */
    public int readInt(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Long value.
     * @throws GridPortableException In case of error.
     */
    public long readLong(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @throws GridPortableException In case of error.
     * @return Float value.
     */
    public float readFloat(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Double value.
     * @throws GridPortableException In case of error.
     */
    public double readDouble(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Char value.
     * @throws GridPortableException In case of error.
     */
    public char readChar(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Boolean value.
     * @throws GridPortableException In case of error.
     */
    public boolean readBoolean(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return String value.
     * @throws GridPortableException In case of error.
     */
    @Nullable public String readString(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return UUID.
     * @throws GridPortableException In case of error.
     */
    @Nullable public UUID readUuid(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Date.
     * @throws GridPortableException In case of error.
     */
    @Nullable public Date readDate(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Object.
     * @throws GridPortableException In case of error.
     */
    @Nullable public Object readObject(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Portable object.
     * @throws GridPortableException In case of error.
     */
    @Nullable public <T> GridPortableObject<T> readPortable(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public byte[] readByteArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Short array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public short[] readShortArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Integer array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public int[] readIntArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Long array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public long[] readLongArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Float array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public float[] readFloatArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public double[] readDoubleArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Char array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public char[] readCharArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Boolean array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public boolean[] readBooleanArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return String array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public String[] readStringArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return UUID array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public UUID[] readUuidArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Date array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public Date[] readDateArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Object array.
     * @throws GridPortableException In case of error.
     */
    @Nullable public Object[] readObjectArray(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Collection.
     * @throws GridPortableException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param colCls Collection class.
     * @return Collection.
     * @throws GridPortableException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(String fieldName, Class<? extends Collection<T>> colCls)
        throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @return Map.
     * @throws GridPortableException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(String fieldName) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param mapCls Map class.
     * @return Map.
     * @throws GridPortableException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(String fieldName, Class<? extends Map<K, V>> mapCls)
        throws GridPortableException;

    /**
     * Gets raw reader.
     *
     * @return Raw reader.
     */
    public GridPortableRawReader rawReader();
}
