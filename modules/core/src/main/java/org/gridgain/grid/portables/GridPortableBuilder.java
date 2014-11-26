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
 * If you need to modify nested portable object you can get builder for nested object using
 * {@link GridPortableBuilder#getField(String)}, changes made on nested builder will be taken on build parent object,
 * for example:
 *
 * <pre name=code class=java>
 * GridPortableBuilder personBuilder = grid.portables().createBuilder(personPortableObj);
 * GridPortableBuilder addressBuilder = personBuilder.field("addr");
 * addressBuilder.field("houseNumber", 15)
 *
 * personPortableObj = personBuilder.build();
 *
 * assert 15 == personPortableObj.<Person>deserialize().getAddr().getHouseNumber();
 * </pre>
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
     * Returns the value assigned to specified field.
     * If the value is a portable object instance of {@code GridPortableBuilder} will be returned, you can modify nested
     * builder.
     * Collections and maps returned from this method are modifiable.
     *
     * @param name Field name.
     * @return Value assigned to the field.
     */
    public <T> T getField(String name);

    /**
     * Sets value to the field.
     *
     * Note: This method may be used for fields that already present in the metadata, if you need to add a new field you
     * have to use methods like {@code stringField(String, String)}, {@code intField(String, String)} to specify field
     * type explicitly.
     *
     * @param name Field name.
     * @param val Field value.
     */
    public GridPortableBuilder setField(String name, @Nullable Object val);

    /**
     * Sets value to the field.
     *
     * Note: This method may be used for fields that already present in the metadata, if you need to add a new field you
     * have to use methods like {@code stringField(String, String)}, {@code intField(String, String)} to specify field
     * type explicitly.
     *
     * @param name Field name.
     * @param val Field value.
     */
    public <T> GridPortableBuilder setField(String name, @Nullable T val, GridPortableType<T> type);

    /**
     * Sets value to the field.
     *
     * Note: This method may be used for fields that already present in the metadata, if you need to add a new field you
     * have to use methods like {@code stringField(String, String)}, {@code intField(String, String)} to specify field
     * type explicitly.
     *
     * @param name Field name.
     * @param builder Field value.
     */
    public GridPortableBuilder setField(String name, @Nullable GridPortableBuilder builder);

    /**
     * Sets hash code for the portable object. If not set, GridGain will generate
     * one automatically.
     *
     * @param hashCode Hash code.
     * @return {@code this} instance for chaining.
     */
    public GridPortableBuilder hashCode(int hashCode);

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
