/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portables;

import org.jetbrains.annotations.*;

import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * A wrapper for {@code GridPortableObject} that allow to create copy of the portable object with some modifications.
 *
 * <p>
 * Usage:
 * <pre name=code class=java>
 * GridMutablePortableObject mutableObj = portableObj.toMutable();
 *
 * String firstName = mutableObj.field("firstName");
 * String lastName = mutableObj.field("firstName");
 * mutableObj.field("fullName", firstName + " " + lastName)
 *
 * portableObj = mutableObj.toPortableObject();
 * </pre>
 *
 * <p>
 * This class is not thread-safe.
 */
public interface GridMutablePortable {
    /**
     * Returns the value of the specified field.
     * If the value is another portable object instance of {@code GridMutablePortableObject} will be returned.
     * Arrays and collections returned from this method are modifiable.
     *
     * @param fldName Field name.
     * @return Value of the field.
     */
    public <F> F field(String fldName);

    /**
     * Sets field value.
     *
     * @param fldName Field name.
     * @param val Field value.
     */
    public void field(String fldName, @Nullable Object val);

    /**
     * @param hashCode Hash code to set.
     * @return this.
     */
    public GridMutablePortable setHashCode(int hashCode);

    /**
     * Returns hashcode of portable object.
     *
     * @return Hashcode.
     */
    public int getHashCode();

    /**
     * Returns new portable object with content from this {@code GridMutablePortableObject}.
     *
     * @return New portable object.
     */
    public GridPortableObject toPortableObject();

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldByte(String fieldName, byte val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldShort(String fieldName, short val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldInt(String fieldName, int val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldLong(String fieldName, long val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldFloat(String fieldName, float val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldDouble(String fieldName, double val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldChar(String fieldName, char val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldBoolean(String fieldName, boolean val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldString(String fieldName, @Nullable String val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val UUID to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldUuid(String fieldName, @Nullable UUID val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Date to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldDate(String fieldName, @Nullable Date val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Timestamp to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldTimestamp(String fieldName, @Nullable Timestamp val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param obj Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldObject(String fieldName, @Nullable Object obj) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldByteArray(String fieldName, @Nullable byte[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldShortArray(String fieldName, @Nullable short[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldIntArray(String fieldName, @Nullable int[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldLongArray(String fieldName, @Nullable long[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldFloatArray(String fieldName, @Nullable float[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldDoubleArray(String fieldName, @Nullable double[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldCharArray(String fieldName, @Nullable char[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldBooleanArray(String fieldName, @Nullable boolean[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldStringArray(String fieldName, @Nullable String[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldUuidArray(String fieldName, @Nullable UUID[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldDateArray(String fieldName, @Nullable Date[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void fieldObjectArray(String fieldName, @Nullable Object[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param col Collection to write.
     * @throws GridPortableException In case of error.
     */
    public <T> void fieldCollection(String fieldName, @Nullable Collection<T> col) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param map Map to write.
     * @throws GridPortableException In case of error.
     */
    public <K, V> void fieldMap(String fieldName, @Nullable Map<K, V> map) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public <T extends Enum<?>> void fieldEnum(String fieldName, T val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public <T extends Enum<?>> void fieldEnumArray(String fieldName, T[] val) throws GridPortableException;
}
