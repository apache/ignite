/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portables;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Portable object builder.
 * <p>
 * Type ID is required.
 */
public interface GridPortableBuilder<T> {
    /**
     * Sets type ID.
     *
     * @param cls Class.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> typeId(Class<T> cls);

    /**
     * Sets type ID.
     *
     * @param clsName Class name.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> typeId(String clsName);

    /**
     * Sets type ID.
     *
     * @param typeId Type ID.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> typeId(int typeId);

    /**
     * Sets hash code.
     *
     * @param hashCode Hash code.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> hashCode(int hashCode);

    /**
     * Adds {@code byte} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> byteField(String fieldName, byte val);

    /**
     * Adds {@code short} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> shortField(String fieldName, short val);

    /**
     * Adds {@code int} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> intField(String fieldName, int val);

    /**
     * Adds {@code long} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> longField(String fieldName, long val);

    /**
     * Adds {@code float} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> floatField(String fieldName, float val);

    /**
     * Adds {@code double} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> doubleField(String fieldName, double val);

    /**
     * Adds {@code char} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> charField(String fieldName, char val);

    /**
     * Adds {@code boolean} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> booleanField(String fieldName, boolean val);

    /**
     * Adds {@link String} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> stringField(String fieldName, @Nullable String val);

    /**
     * Adds {@link UUID} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> uuidField(String fieldName, @Nullable UUID val);

    /**
     * Adds {@link Object} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> objectField(String fieldName, @Nullable Object val);

    /**
     * Adds {@code byte array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> byteArrayField(String fieldName, @Nullable byte[] val);

    /**
     * Adds {@code short array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> shortArrayField(String fieldName, @Nullable short[] val);

    /**
     * Adds {@code int array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> intArrayField(String fieldName, @Nullable int[] val);

    /**
     * Adds {@code long array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> longArrayField(String fieldName, @Nullable long[] val);

    /**
     * Adds {@code float array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> floatArrayField(String fieldName, @Nullable float[] val);

    /**
     * Adds {@code double array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> doubleArrayField(String fieldName, @Nullable double[] val);

    /**
     * Adds {@code char array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> charArrayField(String fieldName, @Nullable char[] val);

    /**
     * Adds {@code boolean array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> booleanArrayField(String fieldName, @Nullable boolean[] val);

    /**
     * Adds {@code String array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> stringArrayField(String fieldName, @Nullable String[] val);

    /**
     * Adds {@code UUID array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> uuidArrayField(String fieldName, @Nullable UUID[] val);

    /**
     * Adds {@code Object array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> objectArrayField(String fieldName, @Nullable Object[] val);

    /**
     * Adds {@link Collection} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> collectionField(String fieldName, @Nullable Collection<?> val);

    /**
     * Adds {@link Map} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder<T> mapField(String fieldName, @Nullable Map<?, ?> val);

    /**
     * Builds portable object.
     *
     * @return Portable object.
     * @throws GridPortableException In case of error.
     */
    public GridPortableObject<T> build() throws GridPortableException;
}
