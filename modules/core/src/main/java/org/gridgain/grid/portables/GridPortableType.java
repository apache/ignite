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
 * Type of field in portable object.
 */
public class GridPortableType<T> {
    /** */
    public static final GridPortableType<Byte> BYTE = new GridPortableType<>(Byte.TYPE);

    /** */
    public static final GridPortableType<Short> SHORT = new GridPortableType<>(Short.TYPE);

    /** */
    public static final GridPortableType<Integer> INT = new GridPortableType<>(Integer.TYPE);

    /** */
    public static final GridPortableType<Long> LONG = new GridPortableType<>(Long.TYPE);

    /** */
    public static final GridPortableType<Float> FLOAT = new GridPortableType<>(Float.TYPE);

    /** */
    public static final GridPortableType<Double> DOUBLE = new GridPortableType<>(Double.TYPE);

    /** */
    public static final GridPortableType<Character> CHAR = new GridPortableType<>(Character.TYPE);

    /** */
    public static final GridPortableType<Boolean> BOOLEAN = new GridPortableType<>(Boolean.TYPE);

    /** */
    public static final GridPortableType<String> STRING = new GridPortableType<>(String.class);

    /** */
    public static final GridPortableType<UUID> UUID = new GridPortableType<>(java.util.UUID.class);

    /** */
    public static final GridPortableType<Date> DATE = new GridPortableType<>(Date.class);

    /** */
    public static final GridPortableType<byte[]> BYTE_ARR = new GridPortableType<>(byte[].class);

    /** */
    public static final GridPortableType<short[]> SHORT_ARR = new GridPortableType<>(short[].class);

    /** */
    public static final GridPortableType<int[]> INT_ARR = new GridPortableType<>(int[].class);

    /** */
    public static final GridPortableType<long[]> LONG_ARR = new GridPortableType<>(long[].class);

    /** */
    public static final GridPortableType<float[]> FLOAT_ARR = new GridPortableType<>(float[].class);

    /** */
    public static final GridPortableType<double[]> DOUBLE_ARR = new GridPortableType<>(double[].class);

    /** */
    public static final GridPortableType<char[]> CHAR_ARR = new GridPortableType<>(char[].class);

    /** */
    public static final GridPortableType<boolean[]> BOOLEAN_ARR = new GridPortableType<>(boolean[].class);

    /** */
    public static final GridPortableType<String[]> STRING_ARR = new GridPortableType<>(String[].class);

    /** */
    public static final GridPortableType<UUID[]> UUID_ARR = new GridPortableType<>(UUID[].class);

    /** */
    public static final GridPortableType<Date[]> DATE_ARR = new GridPortableType<>(Date[].class);

    /** */
    public static final GridPortableType<Object[]> OBJ_ARR = new GridPortableType<>(Object[].class);

    /** */
    public static final GridPortableType<Collection> COLLECTION = new GridPortableType<>(Collection.class);

    /** */
    public static final GridPortableType<Map> MAP = new GridPortableType<>(Map.class);

    /** */
    public static final GridPortableType<Map.Entry> MAP_ENTRY = new GridPortableType<>(Map.Entry.class);

    /** */
    public static final GridPortableType<GridPortableObject> PORTABLE_OBJ = new GridPortableType<>(
        GridPortableObject.class, "Object");

    /** */
    public static final GridPortableType<Enum> ENUM = new GridPortableType<>(Enum.class);

    /** */
    public static final GridPortableType<Enum[]> ENUM_ARR = new GridPortableType<>(Enum[].class);

    /** */
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
     *
     * @return
     */
    public String getTypeName() {
        return typeName;
    }
}
