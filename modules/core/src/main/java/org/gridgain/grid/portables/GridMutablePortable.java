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
 * portableObj = mutableObj.build();
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
    public GridMutablePortable hashCode(int hashCode);

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
    public GridPortableObject build();

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void byteField(String fieldName, byte val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void shortField(String fieldName, short val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void intField(String fieldName, int val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void longField(String fieldName, long val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void floatField(String fieldName, float val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void doubleField(String fieldName, double val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void charField(String fieldName, char val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void booleanField(String fieldName, boolean val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void stringField(String fieldName, @Nullable String val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val UUID to write.
     * @throws GridPortableException In case of error.
     */
    public void uuidField(String fieldName, @Nullable UUID val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Date to write.
     * @throws GridPortableException In case of error.
     */
    public void dateField(String fieldName, @Nullable Date val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Timestamp to write.
     * @throws GridPortableException In case of error.
     */
    public void timestampField(String fieldName, @Nullable Timestamp val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param obj Value to write.
     * @throws GridPortableException In case of error.
     */
    public void objectField(String fieldName, @Nullable Object obj) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void byteArrayField(String fieldName, @Nullable byte[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void shortArrayField(String fieldName, @Nullable short[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void intArrayField(String fieldName, @Nullable int[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void longArrayField(String fieldName, @Nullable long[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void floatArrayField(String fieldName, @Nullable float[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void doubleArrayField(String fieldName, @Nullable double[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void charArrayField(String fieldName, @Nullable char[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void booleanArrayField(String fieldName, @Nullable boolean[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void stringArrayField(String fieldName, @Nullable String[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void uuidArrayField(String fieldName, @Nullable UUID[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void dateArrayField(String fieldName, @Nullable Date[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public void objectArrayField(String fieldName, @Nullable Object[] val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param col Collection to write.
     * @throws GridPortableException In case of error.
     */
    public <T> void collectionField(String fieldName, @Nullable Collection<T> col) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param map Map to write.
     * @throws GridPortableException In case of error.
     */
    public <K, V> void mapField(String fieldName, @Nullable Map<K, V> map) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public <T extends Enum<?>> void enumField(String fieldName, T val) throws GridPortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws GridPortableException In case of error.
     */
    public <T extends Enum<?>> void enumArrayField(String fieldName, T[] val) throws GridPortableException;
}
