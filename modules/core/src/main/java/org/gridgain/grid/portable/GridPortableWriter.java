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
 * Writer for portable object.
 */
public interface GridPortableWriter {
    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeByte(String fieldName, byte val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeByte(byte val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeShort(String fieldName, short val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeShort(short val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeInt(String fieldName, int val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeInt(int val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeLong(String fieldName, long val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeLong(long val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeFloat(String fieldName, float val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeFloat(float val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDouble(String fieldName, double val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDouble(double val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeChar(String fieldName, char val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeChar(char val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeBoolean(String fieldName, boolean val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeBoolean(boolean val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeString(String fieldName, @Nullable String val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeString(@Nullable String val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val UUID to write.
     * @throws GridPortableException In case of error.
     */
    public void writeUuid(String fieldName, @Nullable UUID val) throws GridPortableException;

    /**
     * @param val UUID to write.
     * @throws GridPortableException In case of error.
     */
    public void writeUuid(@Nullable UUID val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param obj Value to write.
     * @throws GridPortableException In case of error.
     */
    public <T> void writeObject(String fieldName, @Nullable T obj) throws GridPortableException;

    /**
     * @param obj Value to write.
     * @throws GridPortableException In case of error.
     */
    public <T> void writeObject(@Nullable T obj) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeByteArray(String fieldName, @Nullable byte[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeByteArray(@Nullable byte[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeShortArray(String fieldName, @Nullable short[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeShortArray(@Nullable short[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeIntArray(String fieldName, @Nullable int[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeIntArray(@Nullable int[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeLongArray(String fieldName, @Nullable long[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeLongArray(@Nullable long[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeFloatArray(String fieldName, @Nullable float[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeFloatArray(@Nullable float[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDoubleArray(String fieldName, @Nullable double[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDoubleArray(@Nullable double[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeCharArray(String fieldName, @Nullable char[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeCharArray(@Nullable char[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeBooleanArray(@Nullable boolean[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeStringArray(String fieldName, @Nullable String[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeStringArray(@Nullable String[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeUuidArray(@Nullable UUID[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeObjectArray(String fieldName, @Nullable Object[] val) throws GridPortableException;

    /**
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeObjectArray(@Nullable Object[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param col Collection to write.
     * @throws GridPortableException In case of error.
     */
    public <T> void writeCollection(String fieldName, @Nullable Collection<T> col) throws GridPortableException;

    /**
     * @param col Collection to write.
     * @throws GridPortableException In case of error.
     */
    public <T> void writeCollection(@Nullable Collection<T> col) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param map Map to write.
     * @throws GridPortableException In case of error.
     */
    public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws GridPortableException;

    /**
     * @param map Map to write.
     * @throws GridPortableException In case of error.
     */
    public <K, V> void writeMap(@Nullable Map<K, V> map) throws GridPortableException;
}
