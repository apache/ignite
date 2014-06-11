/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portable;

import org.jetbrains.annotations.*;

import java.io.IOException;
import java.util.*;

/**
 * Reader for portable objects.
 */
public interface GridPortableReader {
    /**
     * @param fieldName Field name.
     * @return Byte value.
     * @throws IOException In case of error.
     */
    public byte readByte(String fieldName) throws IOException;

    /**
     * @return Byte value.
     * @throws IOException In case of error.
     */
    public byte readByte() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Short value.
     * @throws IOException In case of error.
     */
    public short readShort(String fieldName) throws IOException;

    /**
     * @return Short value.
     * @throws IOException In case of error.
     */
    public short readShort() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Integer value.
     * @throws IOException In case of error.
     */
    public int readInt(String fieldName) throws IOException;

    /**
     * @return Integer value.
     * @throws IOException In case of error.
     */
    public int readInt() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Long value.
     * @throws IOException In case of error.
     */
    public long readLong(String fieldName) throws IOException;

    /**
     * @return Long value.
     * @throws IOException In case of error.
     */
    public long readLong() throws IOException;

    /**
     * @param fieldName Field name.
     * @throws IOException In case of error.
     * @return Float value.
     */
    public float readFloat(String fieldName) throws IOException;

    /**
     * @throws IOException In case of error.
     * @return Float value.
     */
    public float readFloat() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Double value.
     * @throws IOException In case of error.
     */
    public double readDouble(String fieldName) throws IOException;

    /**
     * @return Double value.
     * @throws IOException In case of error.
     */
    public double readDouble() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Char value.
     * @throws IOException In case of error.
     */
    public char readChar(String fieldName) throws IOException;

    /**
     * @return Char value.
     * @throws IOException In case of error.
     */
    public char readChar() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Boolean value.
     * @throws IOException In case of error.
     */
    public boolean readBoolean(String fieldName) throws IOException;

    /**
     * @return Boolean value.
     * @throws IOException In case of error.
     */
    public boolean readBoolean() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws IOException In case of error.
     */
    @Nullable public byte[] readByteArray(String fieldName) throws IOException;

    /**
     * @return Byte array.
     * @throws IOException In case of error.
     */
    @Nullable public byte[] readByteArray() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Short array.
     * @throws IOException In case of error.
     */
    @Nullable public short[] readShortArray(String fieldName) throws IOException;

    /**
     * @return Short array.
     * @throws IOException In case of error.
     */
    @Nullable public short[] readShortArray() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Integer array.
     * @throws IOException In case of error.
     */
    @Nullable public int[] readIntArray(String fieldName) throws IOException;

    /**
     * @return Integer array.
     * @throws IOException In case of error.
     */
    @Nullable public int[] readIntArray() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Long array.
     * @throws IOException In case of error.
     */
    @Nullable public long[] readLongArray(String fieldName) throws IOException;

    /**
     * @return Long array.
     * @throws IOException In case of error.
     */
    @Nullable public long[] readLongArray() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Float array.
     * @throws IOException In case of error.
     */
    @Nullable public float[] readFloatArray(String fieldName) throws IOException;

    /**
     * @return Float array.
     * @throws IOException In case of error.
     */
    @Nullable public float[] readFloatArray() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws IOException In case of error.
     */
    @Nullable public double[] readDoubleArray(String fieldName) throws IOException;

    /**
     * @return Byte array.
     * @throws IOException In case of error.
     */
    @Nullable public double[] readDoubleArray() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Char array.
     * @throws IOException In case of error.
     */
    @Nullable public char[] readCharArray(String fieldName) throws IOException;

    /**
     * @return Char array.
     * @throws IOException In case of error.
     */
    @Nullable public char[] readCharArray() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Boolean array.
     * @throws IOException In case of error.
     */
    @Nullable public boolean[] readBooleanArray(String fieldName) throws IOException;

    /**
     * @return Boolean array.
     * @throws IOException In case of error.
     */
    @Nullable public boolean[] readBooleanArray() throws IOException;

    /**
     * @param fieldName Field name.
     * @return String value.
     * @throws IOException In case of error.
     */
    @Nullable public String readString(String fieldName) throws IOException;

    /**
     * @return String value.
     * @throws IOException In case of error.
     */
    @Nullable public String readString() throws IOException;

    /**
     * @param fieldName Field name.
     * @return UUID.
     * @throws IOException In case of error.
     */
    @Nullable public UUID readUuid(String fieldName) throws IOException;

    /**
     * @return UUID.
     * @throws IOException In case of error.
     */
    @Nullable public UUID readUuid() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Object.
     * @throws IOException In case of error.
     */
    @Nullable public <T extends GridPortable> T readObject(String fieldName) throws IOException;

    /**
     * @return Object.
     * @throws IOException In case of error.
     */
    @Nullable public <T extends GridPortable> T readObject() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Collection.
     * @throws IOException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(String fieldName) throws IOException;

    /**
     * @return Collection.
     * @throws IOException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection() throws IOException;

    /**
     * @param fieldName Field name.
     * @return Map.
     * @throws IOException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(String fieldName) throws IOException;

    /**
     * @return Map.
     * @throws IOException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap() throws IOException;
}
