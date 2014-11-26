/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portables;

import java.util.*;

/**
 * Portable object field types.
 */
public class GridPortableType<T> {
    /** {@code byte} or {@code java.lang.Byte} field type. */
    public static final GridPortableType<Byte> BYTE = new GridPortableType<>(Byte.TYPE);

    /** {@code short} or {@code java.lang.Short} field type. */
    public static final GridPortableType<Short> SHORT = new GridPortableType<>(Short.TYPE);

    /** {@code int} or {@code java.lang.Integer} field type. */
    public static final GridPortableType<Integer> INT = new GridPortableType<>(Integer.TYPE);

    /** {@code long} or {@code java.lang.Long} field type. */
    public static final GridPortableType<Long> LONG = new GridPortableType<>(Long.TYPE);

    /** {@code float} or {@code java.lang.Float} field type. */
    public static final GridPortableType<Float> FLOAT = new GridPortableType<>(Float.TYPE);

    /** {@code double} or {@code java.lang.Double} field type. */
    public static final GridPortableType<Double> DOUBLE = new GridPortableType<>(Double.TYPE);

    /** {@code char} or {@code java.lang.Character} field type. */
    public static final GridPortableType<Character> CHAR = new GridPortableType<>(Character.TYPE);

    /** {@code boolean} or {@code java.lang.Boolean} field type. */
    public static final GridPortableType<Boolean> BOOLEAN = new GridPortableType<>(Boolean.TYPE);

    /** {@code java.lang.String} field type. */
    public static final GridPortableType<String> STRING = new GridPortableType<>(String.class);

    /** {@code java.util.UUID} field type. */
    public static final GridPortableType<UUID> UUID = new GridPortableType<>(java.util.UUID.class);

    /** Date field type. */
    public static final GridPortableType<Date> DATE = new GridPortableType<>(Date.class);

    /** Byte array field type. */
    public static final GridPortableType<byte[]> BYTE_ARR = new GridPortableType<>(byte[].class);

    /** Short array field type. */
    public static final GridPortableType<short[]> SHORT_ARR = new GridPortableType<>(short[].class);

    /** Integer array field type. */
    public static final GridPortableType<int[]> INT_ARR = new GridPortableType<>(int[].class);

    /** Long array field type. */
    public static final GridPortableType<long[]> LONG_ARR = new GridPortableType<>(long[].class);

    /** Float array field type. */
    public static final GridPortableType<float[]> FLOAT_ARR = new GridPortableType<>(float[].class);

    /** Double array field type. */
    public static final GridPortableType<double[]> DOUBLE_ARR = new GridPortableType<>(double[].class);

    /** Char array field type. */
    public static final GridPortableType<char[]> CHAR_ARR = new GridPortableType<>(char[].class);

    /** Boolean array field type. */
    public static final GridPortableType<boolean[]> BOOLEAN_ARR = new GridPortableType<>(boolean[].class);

    /** String array field type. */
    public static final GridPortableType<String[]> STRING_ARR = new GridPortableType<>(String[].class);

    /** UUID array field type. */
    public static final GridPortableType<UUID[]> UUID_ARR = new GridPortableType<>(UUID[].class);

    /** Date array field type. */
    public static final GridPortableType<Date[]> DATE_ARR = new GridPortableType<>(Date[].class);

    /** Object array field type. */
    public static final GridPortableType<Object[]> OBJ_ARR = new GridPortableType<>(Object[].class);

    /** Collection field type. */
    public static final GridPortableType<Collection> COLLECTION = new GridPortableType<>(Collection.class);

    /** Map field type. */
    public static final GridPortableType<Map> MAP = new GridPortableType<>(Map.class);

    /** Map entry field type. */
    public static final GridPortableType<Map.Entry> MAP_ENTRY = new GridPortableType<>(Map.Entry.class);

    /** {@code org.gridgain.grid.portables.GridPortableObject} field type. */
    public static final GridPortableType<GridPortableObject> PORTABLE_OBJ = new GridPortableType<>(
        GridPortableObject.class, "Object");

    /** Enum field type. */
    public static final GridPortableType<Enum> ENUM = new GridPortableType<>(Enum.class);

    /** Enum array field type. */
    public static final GridPortableType<Enum[]> ENUM_ARR = new GridPortableType<>(Enum[].class);

    /** Any user object field type. */
    public static final GridPortableType<Object> OBJECT = new GridPortableType<>(Object.class);

    /** */
    private final Class<T> type;

    /** */
    private final String typeName;

    /**
     * @param type Type name.
     */
    private GridPortableType(Class<T> type) {
        this(type, type.getSimpleName());
    }

    /**
     * @param type Type name.
     */
    private GridPortableType(Class<T> type, String typeName) {
        this.type = type;
        this.typeName = typeName;
    }

    /**
     * @return Type.
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * Gets type name.
     *
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }
}
