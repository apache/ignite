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
 * Portable object builder.
 */
public interface GridPortableBuilder {
    /**
     * Sets type ID (required).
     *
     * @param typeId Type ID.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder typeId(int typeId);

    /**
     * Sets hash code.
     *
     * @param hashCode Hash code.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder hashCode(int hashCode);

    /**
     * Adds {@code byte} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder byteField(String fieldName, byte val);

    /**
     * Adds {@code short} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder shortField(String fieldName, short val);

    /**
     * Adds {@code int} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder intField(String fieldName, int val);

    /**
     * Adds {@code long} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder longField(String fieldName, long val);

    /**
     * Adds {@code float} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder floatField(String fieldName, float val);

    /**
     * Adds {@code double} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder doubleField(String fieldName, double val);

    /**
     * Adds {@code char} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder charField(String fieldName, char val);

    /**
     * Adds {@code boolean} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder booleanField(String fieldName, boolean val);

    /**
     * Adds {@link String} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder stringField(String fieldName, @Nullable String val);

    /**
     * Adds {@link UUID} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder uuidField(String fieldName, @Nullable UUID val);

    /**
     * Adds {@link Object} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder objectField(String fieldName, @Nullable Object val);

    /**
     * Adds {@code byte array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder byteArrayField(String fieldName, @Nullable byte[] val);

    /**
     * Adds {@code short array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder shortArrayField(String fieldName, @Nullable short[] val);

    /**
     * Adds {@code int array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder intArrayField(String fieldName, @Nullable int[] val);

    /**
     * Adds {@code long array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder longArrayField(String fieldName, @Nullable long[] val);

    /**
     * Adds {@code float array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder floatArrayField(String fieldName, @Nullable float[] val);

    /**
     * Adds {@code double array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder doubleArrayField(String fieldName, @Nullable double[] val);

    /**
     * Adds {@code char array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder charArrayField(String fieldName, @Nullable char[] val);

    /**
     * Adds {@code boolean array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder booleanArrayField(String fieldName, @Nullable boolean[] val);

    /**
     * Adds {@code String array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder stringArrayField(String fieldName, @Nullable String[] val);

    /**
     * Adds {@code UUID array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder uuidArrayField(String fieldName, @Nullable UUID[] val);

    /**
     * Adds {@code Object array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder objectArrayField(String fieldName, @Nullable Object[] val);

    /**
     * Adds {@link Collection} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder collectionField(String fieldName, @Nullable Collection<?> val);

    /**
     * Adds {@link Map} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder mapField(String fieldName, @Nullable Map<?, ?> val);

    /**
     * Adds raw {@code byte} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawByteField(byte val);

    /**
     * Adds raw {@code short} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawShortField(short val);

    /**
     * Adds raw {@code int} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawIntField(int val);

    /**
     * Adds raw {@code long} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawLongField(long val);

    /**
     * Adds raw {@code float} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawFloatField(float val);

    /**
     * Adds raw {@code double} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawDoubleField(double val);

    /**
     * Adds raw {@code char} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawCharField(char val);

    /**
     * Adds raw {@code boolean} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawBooleanField(boolean val);

    /**
     * Adds raw {@link String} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawStringField(@Nullable String val);

    /**
     * Adds raw {@link UUID} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawUuidField(@Nullable UUID val);

    /**
     * Adds raw {@link Object} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawObjectField(@Nullable Object val);

    /**
     * Adds raw {@code byte array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawByteArrayField(@Nullable byte[] val);

    /**
     * Adds raw {@code short array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawShortArrayField(@Nullable short[] val);

    /**
     * Adds raw {@code int array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawIntArrayField(@Nullable int[] val);

    /**
     * Adds raw {@code long array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawLongArrayField(@Nullable long[] val);

    /**
     * Adds raw {@code float array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawFloatArrayField(@Nullable float[] val);

    /**
     * Adds raw {@code double array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawDoubleArrayField(@Nullable double[] val);

    /**
     * Adds raw {@code char array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawCharArrayField(@Nullable char[] val);

    /**
     * Adds raw {@code boolean array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawBooleanArrayField(@Nullable boolean[] val);

    /**
     * Adds raw {@code String array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawStringArrayField(@Nullable String[] val);

    /**
     * Adds raw {@code UUID array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawUuidArrayField(@Nullable UUID[] val);

    /**
     * Adds raw {@code Object array} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawObjectArrayField(@Nullable Object[] val);

    /**
     * Adds raw {@link Collection} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawCollectionField(@Nullable Collection<?> val);

    /**
     * Adds raw {@link Map} field.
     *
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder rawMapField(@Nullable Map<?, ?> val);

    /**
     * Builds portable object.
     *
     * @return Portable object.
     */
    public GridPortableObject build();
}
