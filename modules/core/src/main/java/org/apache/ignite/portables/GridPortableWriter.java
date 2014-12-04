/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.portables;

import org.jetbrains.annotations.*;

import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * Writer for portable object used in {@link GridPortableMarshalAware} implementations.
 * Useful for the cases when user wants a fine-grained control over serialization.
 * <p>
 * Note that GridGain never writes full strings for field or type names. Instead,
 * for performance reasons, GridGain writes integer hash codes for type and field names.
 * It has been tested that hash code conflicts for the type names or the field names
 * within the same type are virtually non-existent and, to gain performance, it is safe
 * to work with hash codes. For the cases when hash codes for different types or fields
 * actually do collide, GridGain provides {@link GridPortableIdMapper} which
 * allows to override the automatically generated hash code IDs for the type and field names.
 */
public interface GridPortableWriter {
    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeByte(String fieldName, byte val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeShort(String fieldName, short val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeInt(String fieldName, int val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeLong(String fieldName, long val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeFloat(String fieldName, float val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDouble(String fieldName, double val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeChar(String fieldName, char val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeBoolean(String fieldName, boolean val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeString(String fieldName, @Nullable String val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val UUID to write.
     * @throws GridPortableException In case of error.
     */
    public void writeUuid(String fieldName, @Nullable UUID val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Date to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDate(String fieldName, @Nullable Date val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Timestamp to write.
     * @throws GridPortableException In case of error.
     */
    public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param obj Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeObject(String fieldName, @Nullable Object obj) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeByteArray(String fieldName, @Nullable byte[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeShortArray(String fieldName, @Nullable short[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeIntArray(String fieldName, @Nullable int[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeLongArray(String fieldName, @Nullable long[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeFloatArray(String fieldName, @Nullable float[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDoubleArray(String fieldName, @Nullable double[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeCharArray(String fieldName, @Nullable char[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeStringArray(String fieldName, @Nullable String[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeDateArray(String fieldName, @Nullable Date[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void writeObjectArray(String fieldName, @Nullable Object[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param col Collection to write.
     * @throws GridPortableException In case of error.
     */
    public <T> void writeCollection(String fieldName, @Nullable Collection<T> col) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param map Map to write.
     * @throws GridPortableException In case of error.
     */
    public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws GridPortableException;

    /**
     * Gets raw writer. Raw writer does not write field name hash codes, therefore,
     * making the format even more compact. However, if the raw writer is used,
     * dynamic structure changes to the portable objects are not supported.
     *
     * @return Raw writer.
     */
    public GridPortableRawWriter rawWriter();
}
