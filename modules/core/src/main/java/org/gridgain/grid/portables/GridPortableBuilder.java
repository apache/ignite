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
 * Portable object builder. Provides ability to build portable objects dynamically
 * without having class definitions.
 * <p>
 * Here is an example of how a portable object can be built dynamically:
 * <pre name=code class=java>
 * GridPortableBuilder builder = GridGain.grid().portables().builder("org.project.MyObject");
 * builder.stringField("fieldA", "A");
 * build.intField("fieldB", "B");
 *
 * GridPortableObject portableObj = builder.build();
 * </pre>
 *
 * <p>
 * Also builder can be initialized by existing portable object. This allow to change some fields without modification
 * other fields.
 * <pre name=code class=java>
 * GridPortableBuilder builder = GridGain.grid().portables().builder(person);
 * builder.stringField("name", "John");
 * person = builder.build();
 * </pre>
 * </p>
 *
 * For the cases when class definition is present
 * in the class path, it is also possible to populate a standard POJO and then
 * convert it to portable format, like so:
 * <pre name=code class=java>
 * MyObject obj = new MyObject();
 *
 * obj.setFieldA("A");
 * obj.setFieldB(123);
 *
 * GridPortableObject portableObj = GridGain.grid().portables().toPortable(obj);
 * </pre>
 * @see GridPortables#builder(int)
 * @see GridPortables#builder(String)
 * @see GridPortables#builder(GridPortableObject)
 */
public interface GridPortableBuilder {
    /**
     * Sets hash code for the portable object. If not set, GridGain will generate
     * one automatically.
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
     * Adds {@link Date} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder dateField(String fieldName, @Nullable Date val);

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
     * Adds {@code Date array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder dateArrayField(String fieldName, @Nullable Date[] val);

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
     * Adds {@link Enum} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public <T extends Enum<?>> GridPortableBuilder enumField(String fieldName, T val);

    /**
     * Adds {@link Enum array} field.
     *
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public <T extends Enum<?>> GridPortableBuilder enumArrayField(String fieldName, T[] val);

    /**
     * @param fieldName Field name.
     * @param val Value.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder portableObjectField(String fieldName, GridPortableObject val);

    /**
     * Removes field from portable object.
     *
     * @param fieldName Field name.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder removeField(String fieldName);

    /**
     * Builds portable object.
     *
     * @return Portable object.
     * @throws GridPortableException In case of error.
     */
    public GridPortableObject build() throws GridPortableException;
}
