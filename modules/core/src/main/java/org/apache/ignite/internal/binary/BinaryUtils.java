/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.binary;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryCollectionFactory;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryMapFactory;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_USE_BINARY_ARRAYS;
import static org.apache.ignite.internal.util.GridUnsafe.align;

/**
 * Binary utils.
 */
public class BinaryUtils {
    /**
     * Actual file name "{type_id}.classname{platform_id}".
     * Where {@code type_id} is integer type id and {@code platform_id} is byte from {@link PlatformType}
     */
    public static final String MAPPING_FILE_EXTENSION = ".classname";

    /** */
    public static final Map<Class<?>, Byte> PLAIN_CLASS_TO_FLAG;

    /** */
    public static final Map<Byte, Class<?>> FLAG_TO_CLASS;

    /** */
    public static final boolean USE_STR_SERIALIZATION_VER_2 = IgniteSystemProperties.getBoolean(
        IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2, false);

    /** Default value of {@link IgniteSystemProperties#IGNITE_USE_BINARY_ARRAYS}. */
    public static final boolean DFLT_IGNITE_USE_BINARY_ARRAYS = false;

    /** Value of {@link IgniteSystemProperties#IGNITE_USE_BINARY_ARRAYS}. */
    private static boolean USE_BINARY_ARRAYS =
        IgniteSystemProperties.getBoolean(IGNITE_USE_BINARY_ARRAYS, DFLT_IGNITE_USE_BINARY_ARRAYS);

    /** Map from class to associated write replacer. */
    private static final Map<Class, BinaryWriteReplacer> CLS_TO_WRITE_REPLACER = Map.of(
        TreeMap.class, new BinaryTreeMapWriteReplacer(),
        TreeSet.class, new BinaryTreeSetWriteReplacer()
    );

    /** {@code true} if serialized value of this type cannot contain references to objects. */
    private static final boolean[] PLAIN_TYPE_FLAG = new boolean[102];

    /** Binary classes. */
    private static final Collection<Class<?>> BINARY_CLS;

    /** Class for SingletonList obtained at runtime. */
    static final Class<? extends Collection> SINGLETON_LIST_CLS = Collections.singletonList(null).getClass();

    /** Flag: user type. */
    static final short FLAG_USR_TYP = 0x0001;

    /** Flag: only raw data exists. */
    static final short FLAG_HAS_SCHEMA = 0x0002;

    /** Flag indicating that object has raw data. */
    static final short FLAG_HAS_RAW = 0x0004;

    /** Flag: offsets take 1 byte. */
    static final short FLAG_OFFSET_ONE_BYTE = 0x0008;

    /** Flag: offsets take 2 bytes. */
    static final short FLAG_OFFSET_TWO_BYTES = 0x0010;

    /** Flag: compact footer, no field IDs. */
    public static final short FLAG_COMPACT_FOOTER = 0x0020;

    /** Flag: raw data contains .NET type information. Always 0 in Java. Keep it here for information only. */
    @SuppressWarnings("unused")
    public static final short FLAG_CUSTOM_DOTNET_TYPE = 0x0040;

    /** Offset which fits into 1 byte. */
    static final int OFFSET_1 = 1;

    /** Offset which fits into 2 bytes. */
    static final int OFFSET_2 = 2;

    /** Offset which fits into 4 bytes. */
    static final int OFFSET_4 = 4;

    /** Field ID length. */
    static final int FIELD_ID_LEN = 4;

    /** Whether to sort field in binary objects (doesn't affect Binarylizable). */
    public static boolean FIELDS_SORTED_ORDER =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_BINARY_SORT_OBJECT_FIELDS);

    /** Field type names. */
    private static final String[] FIELD_TYPE_NAMES;

    /** FNV1 hash offset basis. */
    private static final int FNV1_OFFSET_BASIS = 0x811C9DC5;

    /** FNV1 hash prime. */
    private static final int FNV1_PRIME = 0x01000193;

    /*
     * Static class initializer.
     */
    static {
        Map<Class<?>, Byte> clsToFlag = new HashMap<>();

        clsToFlag.put(Byte.class, GridBinaryMarshaller.BYTE);
        clsToFlag.put(Short.class, GridBinaryMarshaller.SHORT);
        clsToFlag.put(Integer.class, GridBinaryMarshaller.INT);
        clsToFlag.put(Long.class, GridBinaryMarshaller.LONG);
        clsToFlag.put(Float.class, GridBinaryMarshaller.FLOAT);
        clsToFlag.put(Double.class, GridBinaryMarshaller.DOUBLE);
        clsToFlag.put(Character.class, GridBinaryMarshaller.CHAR);
        clsToFlag.put(Boolean.class, GridBinaryMarshaller.BOOLEAN);
        clsToFlag.put(BigDecimal.class, GridBinaryMarshaller.DECIMAL);
        clsToFlag.put(String.class, GridBinaryMarshaller.STRING);
        clsToFlag.put(UUID.class, GridBinaryMarshaller.UUID);
        clsToFlag.put(Date.class, GridBinaryMarshaller.DATE);
        clsToFlag.put(Timestamp.class, GridBinaryMarshaller.TIMESTAMP);
        clsToFlag.put(Time.class, GridBinaryMarshaller.TIME);

        clsToFlag.put(byte[].class, GridBinaryMarshaller.BYTE_ARR);
        clsToFlag.put(short[].class, GridBinaryMarshaller.SHORT_ARR);
        clsToFlag.put(int[].class, GridBinaryMarshaller.INT_ARR);
        clsToFlag.put(long[].class, GridBinaryMarshaller.LONG_ARR);
        clsToFlag.put(float[].class, GridBinaryMarshaller.FLOAT_ARR);
        clsToFlag.put(double[].class, GridBinaryMarshaller.DOUBLE_ARR);
        clsToFlag.put(char[].class, GridBinaryMarshaller.CHAR_ARR);
        clsToFlag.put(boolean[].class, GridBinaryMarshaller.BOOLEAN_ARR);
        clsToFlag.put(BigDecimal[].class, GridBinaryMarshaller.DECIMAL_ARR);
        clsToFlag.put(String[].class, GridBinaryMarshaller.STRING_ARR);
        clsToFlag.put(UUID[].class, GridBinaryMarshaller.UUID_ARR);
        clsToFlag.put(Date[].class, GridBinaryMarshaller.DATE_ARR);
        clsToFlag.put(Timestamp[].class, GridBinaryMarshaller.TIMESTAMP_ARR);
        clsToFlag.put(Time[].class, GridBinaryMarshaller.TIME_ARR);

        clsToFlag.put(byte.class, GridBinaryMarshaller.BYTE);
        clsToFlag.put(short.class, GridBinaryMarshaller.SHORT);
        clsToFlag.put(int.class, GridBinaryMarshaller.INT);
        clsToFlag.put(long.class, GridBinaryMarshaller.LONG);
        clsToFlag.put(float.class, GridBinaryMarshaller.FLOAT);
        clsToFlag.put(double.class, GridBinaryMarshaller.DOUBLE);
        clsToFlag.put(char.class, GridBinaryMarshaller.CHAR);
        clsToFlag.put(boolean.class, GridBinaryMarshaller.BOOLEAN);

        PLAIN_CLASS_TO_FLAG = Map.copyOf(clsToFlag);

        FLAG_TO_CLASS = PLAIN_CLASS_TO_FLAG.entrySet()
            .stream()
            .filter(e -> !e.getKey().isPrimitive())
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getValue, Map.Entry::getKey));

        BINARY_CLS = Set.copyOf(FLAG_TO_CLASS.values());

        for (byte b : new byte[] {
            GridBinaryMarshaller.BYTE, GridBinaryMarshaller.SHORT, GridBinaryMarshaller.INT, GridBinaryMarshaller.LONG,
            GridBinaryMarshaller.FLOAT, GridBinaryMarshaller.DOUBLE, GridBinaryMarshaller.CHAR, GridBinaryMarshaller.BOOLEAN,
            GridBinaryMarshaller.DECIMAL, GridBinaryMarshaller.STRING, GridBinaryMarshaller.UUID, GridBinaryMarshaller.DATE,
            GridBinaryMarshaller.TIMESTAMP, GridBinaryMarshaller.TIME, GridBinaryMarshaller.BYTE_ARR, GridBinaryMarshaller.SHORT_ARR,
            GridBinaryMarshaller.INT_ARR, GridBinaryMarshaller.LONG_ARR, GridBinaryMarshaller.FLOAT_ARR, GridBinaryMarshaller.DOUBLE_ARR,
            GridBinaryMarshaller.TIME_ARR, GridBinaryMarshaller.CHAR_ARR, GridBinaryMarshaller.BOOLEAN_ARR,
            GridBinaryMarshaller.DECIMAL_ARR, GridBinaryMarshaller.STRING_ARR, GridBinaryMarshaller.UUID_ARR, GridBinaryMarshaller.DATE_ARR,
            GridBinaryMarshaller.TIMESTAMP_ARR, GridBinaryMarshaller.ENUM, GridBinaryMarshaller.ENUM_ARR, GridBinaryMarshaller.NULL}) {

            PLAIN_TYPE_FLAG[b] = true;
        }

        FIELD_TYPE_NAMES = new String[104];

        FIELD_TYPE_NAMES[GridBinaryMarshaller.BYTE] = "byte";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.SHORT] = "short";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.INT] = "int";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.LONG] = "long";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.BOOLEAN] = "boolean";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.FLOAT] = "float";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.DOUBLE] = "double";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.CHAR] = "char";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.UUID] = "UUID";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.DECIMAL] = "decimal";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.STRING] = "String";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.DATE] = "Date";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.TIMESTAMP] = "Timestamp";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.TIME] = "Time";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.ENUM] = "Enum";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.OBJ] = "Object";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.BINARY_OBJ] = "Object";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.COL] = "Collection";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.MAP] = "Map";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.CLASS] = "Class";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.BYTE_ARR] = "byte[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.SHORT_ARR] = "short[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.INT_ARR] = "int[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.LONG_ARR] = "long[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.BOOLEAN_ARR] = "boolean[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.FLOAT_ARR] = "float[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.DOUBLE_ARR] = "double[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.CHAR_ARR] = "char[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.UUID_ARR] = "UUID[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.DECIMAL_ARR] = "decimal[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.STRING_ARR] = "String[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.DATE_ARR] = "Date[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.TIMESTAMP_ARR] = "Timestamp[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.TIME_ARR] = "Time[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.OBJ_ARR] = "Object[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.ENUM_ARR] = "Enum[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.BINARY_ENUM] = "Enum";
    }

    /**
     * Check if user type flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    static boolean isUserType(short flags) {
        return isFlagSet(flags, FLAG_USR_TYP);
    }

    /**
     * Check if raw-only flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    public static boolean hasSchema(short flags) {
        return isFlagSet(flags, FLAG_HAS_SCHEMA);
    }

    /**
     * Check if raw-only flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    static boolean hasRaw(short flags) {
        return isFlagSet(flags, FLAG_HAS_RAW);
    }

    /**
     * Check if "no-field-ids" flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    static boolean isCompactFooter(short flags) {
        return isFlagSet(flags, FLAG_COMPACT_FOOTER);
    }

    /**
     * Check whether particular flag is set.
     *
     * @param flags Flags.
     * @param flag Flag.
     * @return {@code True} if flag is set in flags.
     */
    static boolean isFlagSet(short flags, short flag) {
        return (flags & flag) == flag;
    }

    /**
     * Schema initial ID.
     *
     * @return ID.
     */
    static int schemaInitialId() {
        return FNV1_OFFSET_BASIS;
    }

    /**
     * Update schema ID when new field is added.
     *
     * @param schemaId Current schema ID.
     * @param fieldId Field ID.
     * @return New schema ID.
     */
    static int updateSchemaId(int schemaId, int fieldId) {
        schemaId = schemaId ^ (fieldId & 0xFF);
        schemaId = schemaId * FNV1_PRIME;
        schemaId = schemaId ^ ((fieldId >> 8) & 0xFF);
        schemaId = schemaId * FNV1_PRIME;
        schemaId = schemaId ^ ((fieldId >> 16) & 0xFF);
        schemaId = schemaId * FNV1_PRIME;
        schemaId = schemaId ^ ((fieldId >> 24) & 0xFF);
        schemaId = schemaId * FNV1_PRIME;

        return schemaId;
    }

    /**
     * @param typeId Field type ID.
     * @return Field type name or {@code null} if unknown.
     */
    public static String fieldTypeName(int typeId) {
        if (typeId < 0 || typeId >= FIELD_TYPE_NAMES.length)
            return null;

        return FIELD_TYPE_NAMES[typeId];
    }

    /**
     * Write value with flag. e.g. writePlainObject(writer, (byte)77) will write two byte: {BYTE, 77}.
     *
     * @param writer W
     * @param val Value.
     */
    public static void writePlainObject(BinaryWriterEx writer, Object val) {
        Byte flag = PLAIN_CLASS_TO_FLAG.get(val.getClass());

        if (flag == null)
            throw new IllegalArgumentException("Can't write object with type: " + val.getClass());

        switch (flag) {
            case GridBinaryMarshaller.BYTE:
                writer.writeByte(flag);
                writer.writeByte((Byte)val);

                break;

            case GridBinaryMarshaller.SHORT:
                writer.writeByte(flag);
                writer.writeShort((Short)val);

                break;

            case GridBinaryMarshaller.INT:
                writer.writeByte(flag);
                writer.writeInt((Integer)val);

                break;

            case GridBinaryMarshaller.LONG:
                writer.writeByte(flag);
                writer.writeLong((Long)val);

                break;

            case GridBinaryMarshaller.FLOAT:
                writer.writeByte(flag);
                writer.writeFloat((Float)val);

                break;

            case GridBinaryMarshaller.DOUBLE:
                writer.writeByte(flag);
                writer.writeDouble((Double)val);

                break;

            case GridBinaryMarshaller.CHAR:
                writer.writeByte(flag);
                writer.writeChar((Character)val);

                break;

            case GridBinaryMarshaller.BOOLEAN:
                writer.writeByte(flag);
                writer.writeBoolean((Boolean)val);

                break;

            case GridBinaryMarshaller.DECIMAL:
                writer.writeDecimal((BigDecimal)val);

                break;

            case GridBinaryMarshaller.STRING:
                writer.writeString((String)val);

                break;

            case GridBinaryMarshaller.UUID:
                writer.writeUuid((UUID)val);

                break;

            case GridBinaryMarshaller.DATE:
                writer.writeDate((Date)val);

                break;

            case GridBinaryMarshaller.TIMESTAMP:
                writer.writeTimestamp((Timestamp)val);

                break;

            case GridBinaryMarshaller.TIME:
                writer.writeTime((Time)val);

                break;

            case GridBinaryMarshaller.BYTE_ARR:
                writer.writeByteArray((byte[])val);

                break;

            case GridBinaryMarshaller.SHORT_ARR:
                writer.writeShortArray((short[])val);

                break;

            case GridBinaryMarshaller.INT_ARR:
                writer.writeIntArray((int[])val);

                break;

            case GridBinaryMarshaller.LONG_ARR:
                writer.writeLongArray((long[])val);

                break;

            case GridBinaryMarshaller.FLOAT_ARR:
                writer.writeFloatArray((float[])val);

                break;

            case GridBinaryMarshaller.DOUBLE_ARR:
                writer.writeDoubleArray((double[])val);

                break;

            case GridBinaryMarshaller.CHAR_ARR:
                writer.writeCharArray((char[])val);

                break;

            case GridBinaryMarshaller.BOOLEAN_ARR:
                writer.writeBooleanArray((boolean[])val);

                break;

            case GridBinaryMarshaller.DECIMAL_ARR:
                writer.writeDecimalArray((BigDecimal[])val);

                break;

            case GridBinaryMarshaller.STRING_ARR:
                writer.writeStringArray((String[])val);

                break;

            case GridBinaryMarshaller.UUID_ARR:
                writer.writeUuidArray((UUID[])val);

                break;

            case GridBinaryMarshaller.DATE_ARR:
                writer.writeDateArray((Date[])val);

                break;

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                writer.writeTimestampArray((Timestamp[])val);

                break;

            case GridBinaryMarshaller.TIME_ARR:
                writer.writeTimeArray((Time[])val);

                break;

            default:
                throw new IllegalArgumentException("Can't write object with type: " + val.getClass());
        }
    }

    /**
     * @param obj Value to unwrap.
     * @return Unwrapped value.
     */
    public static Object unwrapTemporary(Object obj) {
        if (obj instanceof BinaryObjectOffheapImpl)
            return ((BinaryObjectOffheapImpl)obj).heapCopy();

        return obj;
    }

    /**
     * @return {@code true} if content of serialized value cannot contain references to other object.
     */
    public static boolean isPlainType(int type) {
        return type > 0 && type < PLAIN_TYPE_FLAG.length && PLAIN_TYPE_FLAG[type];
    }

    /**
     * Checks whether an array type values can or can not contain references to other object.
     *
     * @param type Array type.
     * @return {@code true} if content of serialized array value cannot contain references to other object.
     */
    public static boolean isPlainArrayType(int type) {
        return (type >= GridBinaryMarshaller.BYTE_ARR && type <= GridBinaryMarshaller.DATE_ARR)
            || type == GridBinaryMarshaller.TIMESTAMP_ARR || type == GridBinaryMarshaller.TIME_ARR;
    }

    /**
     * @param cls Class.
     * @return Binary field type.
     */
    public static byte typeByClass(Class<?> cls) {
        Byte type = PLAIN_CLASS_TO_FLAG.get(cls);

        if (type != null)
            return type;

        if (U.isEnum(cls))
            return GridBinaryMarshaller.ENUM;

        if (cls.isArray())
            return cls.getComponentType().isEnum() || cls.getComponentType() == Enum.class ?
                GridBinaryMarshaller.ENUM_ARR : GridBinaryMarshaller.OBJ_ARR;

        if (cls == BinaryArray.class)
            return GridBinaryMarshaller.OBJ_ARR;

        if (cls == BinaryEnumArray.class)
            return GridBinaryMarshaller.ENUM_ARR;

        if (isSpecialCollection(cls))
            return GridBinaryMarshaller.COL;

        if (isSpecialMap(cls))
            return GridBinaryMarshaller.MAP;

        return GridBinaryMarshaller.OBJ;
    }

    /**
     * Tells whether provided type is binary.
     *
     * @param cls Class to check.
     * @return Whether type is binary.
     */
    public static boolean isBinaryType(Class<?> cls) {
        assert cls != null;

        return BinaryObject.class.isAssignableFrom(cls) ||
            Proxy.class.isAssignableFrom(cls) ||
            BINARY_CLS.contains(cls);
    }

    /**
     * @param map Map to check.
     * @return {@code True} if this map type is supported.
     */
    public static boolean knownMap(Object map) {
        Class<?> cls = map == null ? null : map.getClass();

        return cls == HashMap.class ||
            cls == LinkedHashMap.class ||
            cls == ConcurrentHashMap.class;
    }

    /**
     * Attempts to create a new map of the same known type. Will return null if map type is not supported.
     *
     * @param map Map.
     * @return New map of the same type or null.
     */
    public static <K, V> Map<K, V> newKnownMap(Object map) {
        Class<?> cls = map == null ? null : map.getClass();

        if (cls == HashMap.class)
            return U.newHashMap(((Map)map).size());
        else if (cls == LinkedHashMap.class)
            return U.newLinkedHashMap(((Map)map).size());
        else if (cls == ConcurrentHashMap.class)
            return new ConcurrentHashMap<>(((Map)map).size());

        return null;
    }

    /**
     * Attempts to create a new map of the same type as {@code map} has. Otherwise returns new {@code HashMap}
     * instance.
     *
     * @param map Original map.
     * @return New map.
     */
    public static <K, V> Map<K, V> newMap(Map<K, V> map) {
        if (map instanceof LinkedHashMap)
            return U.newLinkedHashMap(map.size());
        else if (map instanceof TreeMap)
            return new TreeMap<>(((TreeMap<Object, Object>)map).comparator());
        else if (map instanceof ConcurrentHashMap)
            return new ConcurrentHashMap<>(map.size());

        return U.newHashMap(map.size());
    }

    /**
     * @param col Collection to check.
     * @return True if this is a collection of a known type.
     */
    public static boolean knownCollection(Object col) {
        Class<?> cls = col == null ? null : col.getClass();

        return cls == HashSet.class ||
            cls == LinkedHashSet.class ||
            cls == ConcurrentSkipListSet.class ||
            cls == ArrayList.class ||
            cls == LinkedList.class ||
            cls == SINGLETON_LIST_CLS;
    }

    /**
     * @param arr Array to check.
     * @return {@code true} if this array is of a known type.
     */
    public static boolean knownArray(Object arr) {
        if (arr == null)
            return false;

        Class<?> cls = arr.getClass();

        return cls == byte[].class || cls == short[].class || cls == int[].class || cls == long[].class ||
            cls == float[].class || cls == double[].class || cls == char[].class || cls == boolean[].class ||
            cls == String[].class || cls == UUID[].class || cls == Date[].class || cls == Timestamp[].class ||
            cls == BigDecimal[].class || cls == Time[].class;
    }

    /**
     * Attempts to create a new collection of the same known type. Will return null if collection type is unknown.
     *
     * @param col Collection.
     * @return New empty collection.
     */
    public static <V> Collection<V> newKnownCollection(Object col) {
        Class<?> cls = col == null ? null : col.getClass();

        if (cls == HashSet.class)
            return U.newHashSet(((Collection)col).size());
        else if (cls == LinkedHashSet.class)
            return U.newLinkedHashSet(((Collection)col).size());
        else if (cls == ConcurrentSkipListSet.class)
            return new ConcurrentSkipListSet<>(((ConcurrentSkipListSet<Object>)col).comparator());
        else if (cls == ArrayList.class)
            return new ArrayList<>(((Collection)col).size());
        else if (cls == LinkedList.class)
            return new LinkedList<>();
        else if (cls == SINGLETON_LIST_CLS)
            return new MutableSingletonList<>();

        return null;
    }

    /**
     * Check protocol version.
     *
     * @param protoVer Protocol version.
     */
    public static void checkProtocolVersion(byte protoVer) {
        if (GridBinaryMarshaller.PROTO_VER != protoVer)
            throw new BinaryObjectException("Unsupported protocol version: " + protoVer);
    }

    /**
     * Get binary object length.
     *
     * @param in Input stream.
     * @param start Start position.
     * @return Length.
     */
    public static int length(BinaryPositionReadable in, int start) {
        return in.readIntPositioned(start + GridBinaryMarshaller.TOTAL_LEN_POS);
    }

    /** */
    static int dataStartRelative(BinaryPositionReadable in, int start) {
        int typeId = in.readIntPositioned(start + GridBinaryMarshaller.TYPE_ID_POS);

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            // Gets the length of the type name which is stored as string.
            int len = in.readIntPositioned(start + GridBinaryMarshaller.DFLT_HDR_LEN + /** object type */1);

            return GridBinaryMarshaller.DFLT_HDR_LEN + /** object type */1 + /** string length */ 4 + len;
        }
        else
            return GridBinaryMarshaller.DFLT_HDR_LEN;
    }

    /**
     * Get footer start of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Footer start.
     */
    private static int footerStartRelative(BinaryPositionReadable in, int start) {
        short flags = in.readShortPositioned(start + GridBinaryMarshaller.FLAGS_POS);

        if (hasSchema(flags))
            // Schema exists, use offset.
            return in.readIntPositioned(start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);
        else
            // No schema, footer start equals to object end.
            return length(in, start);
    }

    /**
     * Get object's footer.
     *
     * @param in Input stream.
     * @param start Start position.
     * @return Footer start.
     */
    public static int footerStartAbsolute(BinaryPositionReadable in, int start) {
        return footerStartRelative(in, start) + start;
    }

    /**
     * Get object's footer.
     *
     * @param in Input stream.
     * @param start Start position.
     * @return Footer.
     */
    public static IgniteBiTuple<Integer, Integer> footerAbsolute(BinaryPositionReadable in, int start) {
        short flags = in.readShortPositioned(start + GridBinaryMarshaller.FLAGS_POS);

        int footerEnd = length(in, start);

        if (hasSchema(flags)) {
            // Schema exists.
            int footerStart = in.readIntPositioned(start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);

            if (hasRaw(flags))
                footerEnd -= 4;

            assert footerStart <= footerEnd;

            return F.t(start + footerStart, start + footerEnd);
        }
        else
            // No schema.
            return F.t(start + footerEnd, start + footerEnd);
    }

    /**
     * Get relative raw offset of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Raw offset.
     */
    private static int rawOffsetRelative(BinaryPositionReadable in, int start) {
        short flags = in.readShortPositioned(start + GridBinaryMarshaller.FLAGS_POS);

        int len = length(in, start);

        if (hasSchema(flags)) {
            // Schema exists.
            if (hasRaw(flags))
                // Raw offset is set, it is at the very end of the object.
                return in.readIntPositioned(start + len - 4);
            else
                // Raw offset is not set, so just return schema offset.
                return in.readIntPositioned(start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);
        }
        else
            // No schema, raw offset is located on schema offset position.
            return in.readIntPositioned(start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);
    }

    /**
     * Get absolute raw offset of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Raw offset.
     */
    public static int rawOffsetAbsolute(BinaryPositionReadable in, int start) {
        return start + rawOffsetRelative(in, start);
    }

    /**
     * Get offset length for the given flags.
     *
     * @param flags Flags.
     * @return Offset size.
     */
    public static int fieldOffsetLength(short flags) {
        if ((flags & FLAG_OFFSET_ONE_BYTE) == FLAG_OFFSET_ONE_BYTE)
            return OFFSET_1;
        else if ((flags & FLAG_OFFSET_TWO_BYTES) == FLAG_OFFSET_TWO_BYTES)
            return OFFSET_2;
        else
            return OFFSET_4;
    }

    /**
     * Get field ID length.
     *
     * @param flags Flags.
     * @return Field ID length.
     */
    public static int fieldIdLength(short flags) {
        return isCompactFooter(flags) ? 0 : FIELD_ID_LEN;
    }

    /**
     * Get relative field offset.
     *
     * @param stream Stream.
     * @param pos Position.
     * @param fieldOffsetSize Field offset size.
     * @return Relative field offset.
     */
    public static int fieldOffsetRelative(BinaryPositionReadable stream, int pos, int fieldOffsetSize) {
        int res;

        if (fieldOffsetSize == OFFSET_1)
            res = (int)stream.readBytePositioned(pos) & 0xFF;
        else if (fieldOffsetSize == OFFSET_2)
            res = (int)stream.readShortPositioned(pos) & 0xFFFF;
        else
            res = stream.readIntPositioned(pos);

        return res;
    }

    /**
     * Merge old and new metas.
     *
     * @param oldMeta Old meta.
     * @param newMeta New meta.
     * @return New meta if old meta was null, old meta if no changes detected, merged meta otherwise.
     * @throws BinaryObjectException If merge failed due to metadata conflict.
     */
    public static BinaryMetadata mergeMetadata(@Nullable BinaryMetadata oldMeta, BinaryMetadata newMeta) {
        return mergeMetadata(oldMeta, newMeta, null);
    }

    /**
     * Merge old and new metas.
     *
     * @param oldMeta Old meta.
     * @param newMeta New meta.
     * @param changedSchemas Set for holding changed schemas.
     * @return New meta if old meta was null, old meta if no changes detected, merged meta otherwise.
     * @throws BinaryObjectException If merge failed due to metadata conflict.
     */
    public static BinaryMetadata mergeMetadata(@Nullable BinaryMetadata oldMeta, BinaryMetadata newMeta,
        @Nullable Set<Integer> changedSchemas) {
        assert newMeta != null;

        if (oldMeta == null) {
            if (changedSchemas != null) {
                for (BinarySchema schema : newMeta.schemas())
                    changedSchemas.add(schema.schemaId());
            }

            return newMeta;
        }
        else {
            assert oldMeta.typeId() == newMeta.typeId();

            // Check type name.
            if (!Objects.equals(oldMeta.typeName(), newMeta.typeName())) {
                throw new BinaryObjectException(
                    "Two binary types have duplicate type ID [" + "typeId=" + oldMeta.typeId() +
                        ", typeName1=" + oldMeta.typeName() + ", typeName2=" + newMeta.typeName() + ']'
                );
            }

            // Check affinity field names.
            if (!Objects.equals(oldMeta.affinityKeyFieldName(), newMeta.affinityKeyFieldName())) {
                throw new BinaryObjectException(
                    "Binary type has different affinity key fields [" + "typeName=" + newMeta.typeName() +
                        ", affKeyFieldName1=" + oldMeta.affinityKeyFieldName() +
                        ", affKeyFieldName2=" + newMeta.affinityKeyFieldName() + ']'
                );
            }

            // Check enum flag.
            if (oldMeta.isEnum() != newMeta.isEnum()) {
                if (oldMeta.isEnum())
                    throw new BinaryObjectException("Binary type already registered as enum: " +
                        newMeta.typeName());
                else
                    throw new BinaryObjectException("Binary type already registered as non-enum: " +
                        newMeta.typeName());
            }

            // Check and merge fields.
            Map<String, BinaryFieldMetadata> mergedFields;

            if (FIELDS_SORTED_ORDER)
                mergedFields = new TreeMap<>(oldMeta.fieldsMap());
            else
                mergedFields = new LinkedHashMap<>(oldMeta.fieldsMap());

            Map<String, BinaryFieldMetadata> newFields = newMeta.fieldsMap();

            boolean changed = false;

            Map<String, Integer> mergedEnumMap = null;

            if (!F.isEmpty(newMeta.enumMap())) {
                mergedEnumMap = mergeEnumValues(oldMeta.typeName(), oldMeta.enumMap(), newMeta.enumMap());

                changed = mergedEnumMap.size() > oldMeta.enumMap().size();
            }

            for (Map.Entry<String, BinaryFieldMetadata> newField : newFields.entrySet()) {
                BinaryFieldMetadata oldFieldMeta = mergedFields.put(newField.getKey(), newField.getValue());

                if (oldFieldMeta == null)
                    changed = true;
                else {
                    String oldFieldTypeName = fieldTypeName(oldFieldMeta.typeId());
                    String newFieldTypeName = fieldTypeName(newField.getValue().typeId());

                    if (!Objects.equals(oldFieldTypeName, newFieldTypeName)) {
                        throw new BinaryObjectException(
                            "Type '" + oldMeta.typeName() + "' with typeId " + oldMeta.typeId()
                                + " has a different/incorrect type for field '" + newField.getKey()
                                + "'. Expected '" + oldFieldTypeName + "' but '" + newFieldTypeName
                                + "' was provided. The type of an existing field can not be changed. " +
                                "Use a different field name or follow this procedure to reuse the current name:\n" +
                                "- Delete data records that use the old field type;\n" +
                                "- Remove metadata by the command: " +
                                "'control.sh --meta remove --typeId " + oldMeta.typeId() + "'."
                        );
                    }
                }
            }

            // Check and merge schemas.
            Collection<BinarySchema> mergedSchemas = new HashSet<>(oldMeta.schemas());

            for (BinarySchema newSchema : newMeta.schemas()) {
                if (mergedSchemas.add(newSchema)) {
                    changed = true;

                    if (changedSchemas != null)
                        changedSchemas.add(newSchema.schemaId());
                }
            }

            // Return either old meta if no changes detected, or new merged meta.
            return changed ? new BinaryMetadata(oldMeta.typeId(), oldMeta.typeName(), mergedFields,
                oldMeta.affinityKeyFieldName(), mergedSchemas, oldMeta.isEnum(), mergedEnumMap) : oldMeta;
        }
    }

    /**
     * @param cls Class.
     * @return Mode.
     */
    static BinaryWriteMode mode(Class<?> cls) {
        assert cls != null;

        // Primitives.
        if (cls == byte.class)
            return BinaryWriteMode.P_BYTE;
        else if (cls == boolean.class)
            return BinaryWriteMode.P_BOOLEAN;
        else if (cls == short.class)
            return BinaryWriteMode.P_SHORT;
        else if (cls == char.class)
            return BinaryWriteMode.P_CHAR;
        else if (cls == int.class)
            return BinaryWriteMode.P_INT;
        else if (cls == long.class)
            return BinaryWriteMode.P_LONG;
        else if (cls == float.class)
            return BinaryWriteMode.P_FLOAT;
        else if (cls == double.class)
            return BinaryWriteMode.P_DOUBLE;

        // Boxed primitives.
        else if (cls == Byte.class)
            return BinaryWriteMode.BYTE;
        else if (cls == Boolean.class)
            return BinaryWriteMode.BOOLEAN;
        else if (cls == Short.class)
            return BinaryWriteMode.SHORT;
        else if (cls == Character.class)
            return BinaryWriteMode.CHAR;
        else if (cls == Integer.class)
            return BinaryWriteMode.INT;
        else if (cls == Long.class)
            return BinaryWriteMode.LONG;
        else if (cls == Float.class)
            return BinaryWriteMode.FLOAT;
        else if (cls == Double.class)
            return BinaryWriteMode.DOUBLE;

        // The rest types.
        else if (cls == BigDecimal.class)
            return BinaryWriteMode.DECIMAL;
        else if (cls == String.class)
            return BinaryWriteMode.STRING;
        else if (cls == UUID.class)
            return BinaryWriteMode.UUID;
        else if (cls == Date.class)
            return BinaryWriteMode.DATE;
        else if (cls == Timestamp.class)
            return BinaryWriteMode.TIMESTAMP;
        else if (cls == Time.class)
            return BinaryWriteMode.TIME;
        else if (cls == byte[].class)
            return BinaryWriteMode.BYTE_ARR;
        else if (cls == short[].class)
            return BinaryWriteMode.SHORT_ARR;
        else if (cls == int[].class)
            return BinaryWriteMode.INT_ARR;
        else if (cls == long[].class)
            return BinaryWriteMode.LONG_ARR;
        else if (cls == float[].class)
            return BinaryWriteMode.FLOAT_ARR;
        else if (cls == double[].class)
            return BinaryWriteMode.DOUBLE_ARR;
        else if (cls == char[].class)
            return BinaryWriteMode.CHAR_ARR;
        else if (cls == boolean[].class)
            return BinaryWriteMode.BOOLEAN_ARR;
        else if (cls == BigDecimal[].class)
            return BinaryWriteMode.DECIMAL_ARR;
        else if (cls == String[].class)
            return BinaryWriteMode.STRING_ARR;
        else if (cls == UUID[].class)
            return BinaryWriteMode.UUID_ARR;
        else if (cls == Date[].class)
            return BinaryWriteMode.DATE_ARR;
        else if (cls == Timestamp[].class)
            return BinaryWriteMode.TIMESTAMP_ARR;
        else if (cls == Time[].class)
            return BinaryWriteMode.TIME_ARR;
        else if (cls.isArray())
            return cls.getComponentType().isEnum() ? BinaryWriteMode.ENUM_ARR : BinaryWriteMode.OBJECT_ARR;
        else if (cls == BinaryArray.class)
            return BinaryWriteMode.OBJECT_ARR;
        else if (cls == BinaryEnumArray.class)
            return BinaryWriteMode.ENUM_ARR;
        else if (cls == BinaryObjectImpl.class)
            return BinaryWriteMode.BINARY_OBJ;
        else if (Binarylizable.class.isAssignableFrom(cls))
            return BinaryWriteMode.BINARY;
        else if (isSpecialCollection(cls))
            return BinaryWriteMode.COL;
        else if (isSpecialMap(cls))
            return BinaryWriteMode.MAP;
        else if (U.isEnum(cls))
            return BinaryWriteMode.ENUM;
        else if (cls == BinaryEnumObjectImpl.class)
            return BinaryWriteMode.BINARY_ENUM;
        else if (cls == Class.class)
            return BinaryWriteMode.CLASS;
        else if (Proxy.class.isAssignableFrom(cls))
            return BinaryWriteMode.PROXY;
        else
            return BinaryWriteMode.OBJECT;
    }

    /**
     * Check if class represents a collection which must be treated specially.
     *
     * @param cls Class.
     * @return {@code True} if this is a special collection class.
     */
    public static boolean isSpecialCollection(Class cls) {
        return ArrayList.class.equals(cls) || LinkedList.class.equals(cls) || SINGLETON_LIST_CLS.equals(cls) ||
            HashSet.class.equals(cls) || LinkedHashSet.class.equals(cls);
    }

    /**
     * Check if class represents a map which must be treated specially.
     *
     * @param cls Class.
     * @return {@code True} if this is a special map class.
     */
    public static boolean isSpecialMap(Class cls) {
        return HashMap.class.equals(cls) || LinkedHashMap.class.equals(cls);
    }

    /**
     * @return Value.
     */
    static byte[] doReadByteArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readByteArray(len);
    }

    /**
     * @return Value.
     */
    static boolean[] doReadBooleanArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readBooleanArray(len);
    }

    /**
     * @return Value.
     */
    static short[] doReadShortArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readShortArray(len);
    }

    /**
     * @return Value.
     */
    static char[] doReadCharArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readCharArray(len);
    }

    /**
     * @return Value.
     */
    static int[] doReadIntArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readIntArray(len);
    }

    /**
     * @return Value.
     */
    static long[] doReadLongArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readLongArray(len);
    }

    /**
     * @return Value.
     */
    static float[] doReadFloatArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readFloatArray(len);
    }

    /**
     * @return Value.
     */
    static double[] doReadDoubleArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readDoubleArray(len);
    }

    /**
     * @return Value.
     */
    static BigDecimal doReadDecimal(BinaryInputStream in) {
        int scale = in.readInt();
        byte[] mag = doReadByteArray(in);

        boolean negative = mag[0] < 0;

        if (negative)
            mag[0] &= 0x7F;

        BigInteger intVal = new BigInteger(mag);

        if (negative)
            intVal = intVal.negate();

        return new BigDecimal(intVal, scale);
    }

    /**
     * @return Value.
     */
    static String doReadString(BinaryInputStream in) {
        if (!in.hasArray()) {
            byte[] arr = doReadByteArray(in);

            if (USE_STR_SERIALIZATION_VER_2)
                return utf8BytesToStr(arr, 0, arr.length);
            else
                return new String(arr, UTF_8);
        }

        int strLen = in.readInt();

        int pos = in.position();

        // String will copy necessary array part for us.
        String res;

        if (USE_STR_SERIALIZATION_VER_2) {
            res = utf8BytesToStr(in.array(), pos, strLen);
        }
        else {
            res = new String(in.array(), pos, strLen, UTF_8);
        }

        in.position(pos + strLen);

        return res;
    }

    /**
     * @return Value.
     */
    static UUID doReadUuid(BinaryInputStream in) {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * @return Value.
     */
    static Date doReadDate(BinaryInputStream in) {
        long time = in.readLong();

        return new Date(time);
    }

    /**
     * @return Value.
     */
    static Timestamp doReadTimestamp(BinaryInputStream in) {
        long time = in.readLong();
        int nanos = in.readInt();

        Timestamp ts = new Timestamp(time);

        ts.setNanos(ts.getNanos() + nanos);

        return ts;
    }

    /**
     * @return Value.
     */
    static Time doReadTime(BinaryInputStream in) {
        long time = in.readLong();

        return new Time(time);
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    static BigDecimal[] doReadDecimalArray(BinaryInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        BigDecimal[] arr = new BigDecimal[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.DECIMAL)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadDecimal(in);
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    public static String[] doReadStringArray(BinaryInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        String[] arr = new String[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.STRING)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadString(in);
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    static UUID[] doReadUuidArray(BinaryInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        UUID[] arr = new UUID[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.UUID)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadUuid(in);
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    static Date[] doReadDateArray(BinaryInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        Date[] arr = new Date[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.DATE)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadDate(in);
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    static Timestamp[] doReadTimestampArray(BinaryInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        Timestamp[] arr = new Timestamp[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.TIMESTAMP)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadTimestamp(in);
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    static Time[] doReadTimeArray(BinaryInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        Time[] arr = new Time[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.TIME)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadTime(in);
            }
        }

        return arr;
    }

    /**
     * @return Value.
     */
    static BinaryObject doReadBinaryObject(BinaryInputStream in, BinaryContext ctx, boolean detach) {
        if (in.offheapPointer() > 0) {
            int len = in.readInt();

            int pos = in.position();

            in.position(in.position() + len);

            int start = in.readInt();

            return new BinaryObjectOffheapImpl(ctx, in.offheapPointer() + pos, start, len);
        }
        else {
            byte[] arr = doReadByteArray(in);
            int start = in.readInt();

            BinaryObjectImpl binO = new BinaryObjectImpl(ctx, arr, start);

            if (detach)
                return (BinaryObject)detach(binO);

            return binO;
        }
    }

    /**
     * @param in Binary input stream.
     * @param ctx Binary context.
     * @param ldr Class loader.
     * @return Class object specified at the input stream.
     * @throws BinaryObjectException If failed.
     */
    static Class doReadClass(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr)
        throws BinaryObjectException {
        return doReadClass(in, ctx, ldr, true);
    }

    /**
     * @param in Binary input stream.
     * @param ctx Binary context.
     * @param ldr Class loader.
     * @param deserialize Doesn't load the class when the flag is {@code false}. Class information is skipped.
     * @return Class object specified at the input stream if {@code deserialize == true}. Otherwise returns {@code null}
     * @throws BinaryObjectException If failed.
     */
    private static Class doReadClass(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr, boolean deserialize)
        throws BinaryObjectException {
        int typeId = in.readInt();

        if (!deserialize) {
            // Skip class name at the stream.
            if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
                doReadClassName(in);

            return null;
        }

        return doReadClass(in, ctx, ldr, typeId);
    }

    /**
     * @return Value.
     */
    @SuppressWarnings("ConstantConditions")
    static Object doReadProxy(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles) {
        Class<?>[] intfs = new Class<?>[in.readInt()];

        for (int i = 0; i < intfs.length; i++)
            intfs[i] = doReadClass(in, ctx, ldr);

        InvocationHandler ih = (InvocationHandler)doReadObject(in, ctx, ldr, handles);

        return Proxy.newProxyInstance(ldr != null ? ldr : U.gridClassLoader(), intfs, ih);
    }

    /**
     * Read plain type.
     *
     * @param in Input stream.
     * @return Plain type.
     */
    private static EnumType doReadEnumType(BinaryInputStream in) {
        int typeId = in.readInt();

        if (typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            return new EnumType(typeId, null);
        else {
            String clsName = doReadClassName(in);

            return new EnumType(GridBinaryMarshaller.UNREGISTERED_TYPE_ID, clsName);
        }
    }

    /**
     * @param in Input stream.
     * @return Class name.
     */
    static String doReadClassName(BinaryInputStream in) {
        byte flag = in.readByte();

        if (flag != GridBinaryMarshaller.STRING)
            throw new BinaryObjectException("Failed to read class name [position=" + (in.position() - 1) + ']');

        return doReadString(in);
    }

    /**
     * @param in Binary input stream.
     * @param ctx Binary context.
     * @param ldr Class loader.
     * @param typeId Type id.
     * @return Class object specified at the input stream.
     * @throws BinaryObjectException If failed.
     */
    static Class doReadClass(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr, int typeId)
        throws BinaryObjectException {
        Class cls;

        if (typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            cls = ctx.descriptorForTypeId(true, typeId, ldr, false).describedClass();
        else {
            String clsName = doReadClassName(in);
            boolean useCache = GridBinaryMarshaller.USE_CACHE.get();

            try {
                cls = U.forName(clsName, ldr, null);
            }
            catch (ClassNotFoundException e) {
                throw new BinaryInvalidTypeException("Failed to load the class: " + clsName, e);
            }

            // forces registering of class by type id, at least locally
            if (useCache)
                ctx.registerType(cls, false, false);
        }

        return cls;
    }

    /**
     * Resolve the class.
     *
     * @param ctx Binary context.
     * @param typeId Type ID.
     * @param clsName Class name.
     * @param ldr Class loaded.
     * @return Resovled class.
     */
    static Class resolveClass(BinaryContext ctx, int typeId, @Nullable String clsName,
        @Nullable ClassLoader ldr, boolean registerMeta) {
        Class cls;

        if (typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            cls = ctx.descriptorForTypeId(true, typeId, ldr, registerMeta).describedClass();
        else {
            try {
                cls = U.forName(clsName, ldr, null);
            }
            catch (ClassNotFoundException e) {
                throw new BinaryInvalidTypeException("Failed to load the class: " + clsName, e);
            }

            ctx.registerType(cls, false, false);
        }

        return cls;
    }

    /**
     * Read binary enum.
     *
     * @param in Input stream.
     * @param ctx Binary context.
     * @return Enum.
     */
    static BinaryEnumObjectImpl doReadBinaryEnum(BinaryInputStream in, BinaryContext ctx) {
        return doReadBinaryEnum(in, ctx, doReadEnumType(in));
    }

    /**
     * Read binary enum.
     *
     * @param in Input stream.
     * @param ctx Binary context.
     * @param type Plain type.
     * @return Enum.
     */
    private static BinaryEnumObjectImpl doReadBinaryEnum(BinaryInputStream in, BinaryContext ctx,
        EnumType type) {
        return new BinaryEnumObjectImpl(ctx, type.typeId, type.clsName, in.readInt());
    }

    /**
     * Read binary enum.
     *
     * @param ord Ordinal.
     * @param ctx Context.
     * @param typeId Type ID.
     */
    public static BinaryObjectEx binaryEnum(int ord, BinaryContext ctx, int typeId) {
        return new BinaryEnumObjectImpl(ctx, typeId, null, ord);
    }

    /**
     * @param ctx Context.
     * @param arr Array.
     */
    public static BinaryObjectEx binaryEnum(BinaryContext ctx, byte[] arr) {
        return new BinaryEnumObjectImpl(ctx, arr);
    }

    /**
     * @param register Register method.
     */
    public static void registerMessages(BiConsumer<Short, Supplier<Message>> register) {
        register.accept((short)113, BinaryObjectImpl::new);
        register.accept((short)119, BinaryEnumObjectImpl::new);
    }

    /** */
    public static BinaryObjectEx binaryObject(BinaryContext ctx, byte[] arr, int start) {
        return new BinaryObjectImpl(ctx, arr, start);
    }

    /** */
    public static BinaryObjectEx binaryObject(BinaryContext ctx, byte[] bytes) {
        return new BinaryObjectImpl(ctx, bytes);
    }

    /** */
    public static BinaryObject binaryObject(BinaryContext ctx, byte[] valBytes, CacheObjectContext coCtx) {
        return new BinaryObjectImpl(ctx, valBytes, coCtx);
    }

    /**
     * Read binary enum array.
     *
     * @param in Input stream.
     * @param ctx Binary context.
     * @return Enum array.
     */
    private static Object[] doReadBinaryEnumArray(BinaryInputStream in, BinaryContext ctx) {
        int len = in.readInt();

        Object[] arr = (Object[])Array.newInstance(BinaryEnumObjectImpl.class, len);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else
                arr[i] = doReadBinaryEnum(in, ctx, doReadEnumType(in));
        }

        return arr;
    }

    /**
     * Having target class in place we simply read ordinal and create final representation.
     *
     * @param cls Enum class.
     * @param useCache True if class loader cache will be used, false otherwise.
     * @return Value.
     */
    static Enum<?> doReadEnum(BinaryInputStream in, Class<?> cls, boolean useCache) throws BinaryObjectException {
        assert cls != null;

        if (!cls.isEnum())
            throw new BinaryObjectException("Class does not represent enum type: " + cls.getName());

        int ord = in.readInt();

        if (useCache)
            return BinaryEnumCache.get(cls, ord);
        else
            return uncachedEnumValue(cls, ord);
    }

    /**
     * Get value for the given class without any caching.
     *
     * @param cls Class.
     */
    private static <T> T uncachedEnumValue(Class<?> cls, int ord) throws BinaryObjectException {
        assert cls != null;

        if (ord >= 0) {
            Object[] vals = cls.getEnumConstants();

            if (ord < vals.length)
                return (T)vals[ord];
            else
                throw new BinaryObjectException("Failed to get enum value for ordinal (do you have correct class " +
                    "version?) [cls=" + cls.getName() + ", ordinal=" + ord + ", totalValues=" + vals.length + ']');
        }
        else
            return null;
    }

    /**
     * @param cls Enum class.
     * @return Value.
     */
    static Object[] doReadEnumArray(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr, Class<?> cls)
        throws BinaryObjectException {
        int len = in.readInt();

        Object[] arr = (Object[])Array.newInstance(cls, len);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else
                arr[i] = doReadEnum(in, doReadClass(in, ctx, ldr), GridBinaryMarshaller.USE_CACHE.get());
        }

        return arr;
    }

    /**
     * Read object serialized using optimized marshaller.
     *
     * @return Result.
     */
    public static Object doReadOptimized(BinaryInputStream in, BinaryContext ctx, @Nullable ClassLoader clsLdr) {
        int len = in.readInt();

        ByteArrayInputStream input = new ByteArrayInputStream(in.array(), in.position(), len);

        try {
            return ctx.optimizedMarsh().unmarshal(input, U.resolveClassLoader(clsLdr, ctx.classLoader()));
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException("Failed to unmarshal object with optimized marshaller", e);
        }
        finally {
            in.position(in.position() + len);
        }
    }

    /**
     * @return Object.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable static Object doReadObject(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles) throws BinaryObjectException {
        return reader(ctx, in, ldr, handles.handles(), false, true).deserialize();
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable static Object unmarshal(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr)
        throws BinaryObjectException {
        return unmarshal(in, ctx, ldr, new BinaryReaderHandlesHolderImpl());
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable static Object unmarshal(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles) throws BinaryObjectException {
        return unmarshal(in, ctx, ldr, handles, false, false);
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable static Object unmarshal(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean detach, boolean deserialize) throws BinaryObjectException {
        int start = in.position();

        byte flag = in.readByte();

        switch (flag) {
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.HANDLE: {
                int handlePos = start - in.readInt();

                Object obj = handles.getHandle(handlePos);

                if (obj == null) {
                    int retPos = in.position();

                    in.position(handlePos);

                    obj = unmarshal(in, ctx, ldr, handles, detach, deserialize);

                    in.position(retPos);
                }

                return obj;
            }

            case GridBinaryMarshaller.OBJ: {
                checkProtocolVersion(in.readByte());

                int len = length(in, start);

                BinaryObjectExImpl po;

                if (detach) {
                    BinaryObjectImpl binObj = new BinaryObjectImpl(ctx, in.array(), start);

                    binObj.detachAllowed(true);

                    po = binObj.detach(!handles.isEmpty());
                }
                else {
                    if (in.offheapPointer() == 0)
                        po = new BinaryObjectImpl(ctx, in.array(), start);
                    else
                        po = new BinaryObjectOffheapImpl(ctx, in.offheapPointer(), start,
                            in.remaining() + in.position());
                }

                in.position(start + len);

                handles.setHandle(po, start);

                return po;
            }

            case GridBinaryMarshaller.BYTE:
                return in.readByte();

            case GridBinaryMarshaller.SHORT:
                return in.readShort();

            case GridBinaryMarshaller.INT:
                return in.readInt();

            case GridBinaryMarshaller.LONG:
                return in.readLong();

            case GridBinaryMarshaller.FLOAT:
                return in.readFloat();

            case GridBinaryMarshaller.DOUBLE:
                return in.readDouble();

            case GridBinaryMarshaller.CHAR:
                return in.readChar();

            case GridBinaryMarshaller.BOOLEAN:
                return in.readBoolean();

            case GridBinaryMarshaller.DECIMAL:
                return doReadDecimal(in);

            case GridBinaryMarshaller.STRING:
                return doReadString(in);

            case GridBinaryMarshaller.UUID:
                return doReadUuid(in);

            case GridBinaryMarshaller.DATE:
                return doReadDate(in);

            case GridBinaryMarshaller.TIMESTAMP:
                return doReadTimestamp(in);

            case GridBinaryMarshaller.TIME:
                return doReadTime(in);

            case GridBinaryMarshaller.BYTE_ARR:
                return doReadByteArray(in);

            case GridBinaryMarshaller.SHORT_ARR:
                return doReadShortArray(in);

            case GridBinaryMarshaller.INT_ARR:
                return doReadIntArray(in);

            case GridBinaryMarshaller.LONG_ARR:
                return doReadLongArray(in);

            case GridBinaryMarshaller.FLOAT_ARR:
                return doReadFloatArray(in);

            case GridBinaryMarshaller.DOUBLE_ARR:
                return doReadDoubleArray(in);

            case GridBinaryMarshaller.CHAR_ARR:
                return doReadCharArray(in);

            case GridBinaryMarshaller.BOOLEAN_ARR:
                return doReadBooleanArray(in);

            case GridBinaryMarshaller.DECIMAL_ARR:
                return doReadDecimalArray(in);

            case GridBinaryMarshaller.STRING_ARR:
                return doReadStringArray(in);

            case GridBinaryMarshaller.UUID_ARR:
                return doReadUuidArray(in);

            case GridBinaryMarshaller.DATE_ARR:
                return doReadDateArray(in);

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                return doReadTimestampArray(in);

            case GridBinaryMarshaller.TIME_ARR:
                return doReadTimeArray(in);

            case GridBinaryMarshaller.OBJ_ARR:
                if (useBinaryArrays() && !deserialize)
                    return doReadBinaryArray(in, ctx, ldr, handles, detach, deserialize, false);
                else
                    return doReadObjectArray(in, ctx, ldr, handles, detach, deserialize);

            case GridBinaryMarshaller.COL:
                return doReadCollection(in, ctx, ldr, handles, detach, deserialize, null);

            case GridBinaryMarshaller.MAP:
                return doReadMap(in, ctx, ldr, handles, detach, deserialize, null);

            case GridBinaryMarshaller.BINARY_OBJ:
                return doReadBinaryObject(in, ctx, detach);

            case GridBinaryMarshaller.ENUM:
            case GridBinaryMarshaller.BINARY_ENUM:
                return doReadBinaryEnum(in, ctx, doReadEnumType(in));

            case GridBinaryMarshaller.ENUM_ARR:
                if (useBinaryArrays() && !deserialize)
                    return doReadBinaryArray(in, ctx, ldr, handles, detach, deserialize, true);
                else {
                    doReadEnumType(in); // Simply skip this part as we do not need it.

                    return doReadBinaryEnumArray(in, ctx);
                }

            case GridBinaryMarshaller.CLASS:
                return doReadClass(in, ctx, ldr);

            case GridBinaryMarshaller.PROXY:
                return doReadProxy(in, ctx, ldr, handles);

            case GridBinaryMarshaller.OPTM_MARSH:
                return doReadOptimized(in, ctx, ldr);

            default:
                throw new BinaryObjectException("Invalid flag value: " + flag);
        }
    }

    /**
     * Unmarshall JDBC supported type.
     * @param type Type.
     * @param reader Binary reader.
     * @param binObjAllow Allow to read non plaint objects.
     * @param keepBinary Whether to deserialize objects or keep in binary format.
     * @return Read object.
     */
    public static Object unmarshallJdbc(byte type, BinaryReaderEx reader, boolean binObjAllow, boolean keepBinary) {
        switch (type) {
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.BOOLEAN:
                return reader.readBoolean();

            case GridBinaryMarshaller.BYTE:
                return reader.readByte();

            case GridBinaryMarshaller.CHAR:
                return reader.readChar();

            case GridBinaryMarshaller.SHORT:
                return reader.readShort();

            case GridBinaryMarshaller.INT:
                return reader.readInt();

            case GridBinaryMarshaller.LONG:
                return reader.readLong();

            case GridBinaryMarshaller.FLOAT:
                return reader.readFloat();

            case GridBinaryMarshaller.DOUBLE:
                return reader.readDouble();

            case GridBinaryMarshaller.STRING:
                return doReadString(reader.in());

            case GridBinaryMarshaller.DECIMAL:
                return doReadDecimal(reader.in());

            case GridBinaryMarshaller.UUID:
                return doReadUuid(reader.in());

            case GridBinaryMarshaller.TIME:
                return doReadTime(reader.in());

            case GridBinaryMarshaller.TIMESTAMP:
                return doReadTimestamp(reader.in());

            case GridBinaryMarshaller.DATE:
                return doReadDate(reader.in());

            case GridBinaryMarshaller.BOOLEAN_ARR:
                return doReadBooleanArray(reader.in());

            case GridBinaryMarshaller.BYTE_ARR:
                return doReadByteArray(reader.in());

            case GridBinaryMarshaller.CHAR_ARR:
                return doReadCharArray(reader.in());

            case GridBinaryMarshaller.SHORT_ARR:
                return doReadShortArray(reader.in());

            case GridBinaryMarshaller.INT_ARR:
                return doReadIntArray(reader.in());

            case GridBinaryMarshaller.LONG_ARR:
                return doReadLongArray(reader.in());

            case GridBinaryMarshaller.FLOAT_ARR:
                return doReadFloatArray(reader.in());

            case GridBinaryMarshaller.DOUBLE_ARR:
                return doReadDoubleArray(reader.in());

            case GridBinaryMarshaller.STRING_ARR:
                return doReadStringArray(reader.in());

            case GridBinaryMarshaller.DECIMAL_ARR:
                return doReadDecimalArray(reader.in());

            case GridBinaryMarshaller.UUID_ARR:
                return doReadUuidArray(reader.in());

            case GridBinaryMarshaller.TIME_ARR:
                return doReadTimeArray(reader.in());

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                return doReadTimestampArray(reader.in());

            case GridBinaryMarshaller.DATE_ARR:
                return doReadDateArray(reader.in());

            default:
                reader.in().position(reader.in().position() - 1);

                if (binObjAllow) {
                    Object res = reader.readObjectDetached();

                    return !keepBinary && res instanceof BinaryObject
                        ? ((BinaryObject)res).deserialize()
                        : res;
                }
                else
                    throw new BinaryObjectException("Custom objects are not supported");
        }
    }

    /**
     * @param in Binary input stream.
     * @param ctx Binary context.
     * @param ldr Class loader.
     * @param handles Holder for handles.
     * @param detach Detach flag.
     * @param deserialize Deep flag.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    static Object[] doReadObjectArray(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean detach, boolean deserialize) throws BinaryObjectException {
        int hPos = positionForHandle(in);

        Class compType = doReadClass(in, ctx, ldr, deserialize);

        int len = in.readInt();

        Object[] arr = (deserialize && !BinaryObject.class.isAssignableFrom(compType))
            ? (Object[])Array.newInstance(compType, len)
            : new Object[len];

        handles.setHandle(arr, hPos);

        for (int i = 0; i < len; i++) {
            Object res = deserializeOrUnmarshal(in, ctx, ldr, handles, detach, deserialize);

            if (deserialize && useBinaryArrays() && res instanceof BinaryObject)
                arr[i] = ((BinaryObject)res).deserialize(ldr);
            else
                arr[i] = res;
        }

        return arr;
    }

    /**
     * @param in Binary input stream.
     * @param ctx Binary context.
     * @param ldr Class loader.
     * @param handles Holder for handles.
     * @param detach Detach flag.
     * @param deserialize Deep flag.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private static BinaryArray doReadBinaryArray(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean detach, boolean deserialize, boolean isEnumArray) {
        int hPos = positionForHandle(in);

        int compTypeId = in.readInt();
        String compClsName = null;

        if (compTypeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            compClsName = doReadClassName(in);

        int len = in.readInt();

        Object[] arr = new Object[len];

        BinaryArray res = isEnumArray
            ? new BinaryEnumArray(ctx, compTypeId, compClsName, arr)
            : new BinaryArray(ctx, compTypeId, compClsName, arr);

        handles.setHandle(res, hPos);

        for (int i = 0; i < len; i++)
            arr[i] = deserializeOrUnmarshal(in, ctx, ldr, handles, detach, deserialize);

        return res;
    }

    /**
     * @param deserialize Deep flag.
     * @param factory Collection factory.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    static Collection<?> doReadCollection(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean detach, boolean deserialize, BinaryCollectionFactory factory)
        throws BinaryObjectException {
        int hPos = positionForHandle(in);

        int size = in.readInt();

        assert size >= 0;

        byte colType = in.readByte();

        Collection<Object> col;

        if (factory != null)
            col = factory.create(size);
        else {
            switch (colType) {
                case GridBinaryMarshaller.ARR_LIST:
                    col = new ArrayList<>(size);

                    break;

                case GridBinaryMarshaller.LINKED_LIST:
                    col = new LinkedList<>();

                    break;

                case GridBinaryMarshaller.SINGLETON_LIST:
                    col = new MutableSingletonList<>();

                    break;

                case GridBinaryMarshaller.HASH_SET:
                    col = U.newHashSet(size);

                    break;

                case GridBinaryMarshaller.LINKED_HASH_SET:
                    col = U.newLinkedHashSet(size);

                    break;

                case GridBinaryMarshaller.USER_SET:
                    col = U.newHashSet(size);

                    break;

                case GridBinaryMarshaller.USER_COL:
                    col = new ArrayList<>(size);

                    break;

                default:
                    throw new BinaryObjectException("Invalid collection type: " + colType);
            }
        }

        handles.setHandle(col, hPos);

        for (int i = 0; i < size; i++)
            col.add(deserializeOrUnmarshal(in, ctx, ldr, handles, detach, deserialize));

        return colType == GridBinaryMarshaller.SINGLETON_LIST ? U.convertToSingletonList(col) : col;
    }

    /**
     * @param deserialize Deep flag.
     * @param factory Map factory.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    static Map<?, ?> doReadMap(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean detach, boolean deserialize, BinaryMapFactory factory)
        throws BinaryObjectException {
        int hPos = positionForHandle(in);

        int size = in.readInt();

        assert size >= 0;

        byte mapType = in.readByte();

        Map<Object, Object> map;

        if (factory != null)
            map = factory.create(size);
        else {
            switch (mapType) {
                case GridBinaryMarshaller.HASH_MAP:
                    map = U.newHashMap(size);

                    break;

                case GridBinaryMarshaller.LINKED_HASH_MAP:
                    map = U.newLinkedHashMap(size);

                    break;

                case GridBinaryMarshaller.USER_COL:
                    map = U.newHashMap(size);

                    break;

                default:
                    throw new BinaryObjectException("Invalid map type: " + mapType);
            }
        }

        handles.setHandle(map, hPos);

        for (int i = 0; i < size; i++) {
            Object key = deserializeOrUnmarshal(in, ctx, ldr, handles, detach, deserialize);
            Object val = deserializeOrUnmarshal(in, ctx, ldr, handles, detach, deserialize);

            map.put(key, val);
        }

        return map;
    }

    /**
     * Deserialize or unmarshal the object.
     *
     * @param deserialize Deserialize.
     * @return Result.
     */
    private static Object deserializeOrUnmarshal(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean detach, boolean deserialize) {
        return deserialize ? doReadObject(in, ctx, ldr, handles) : unmarshal(in, ctx, ldr, handles, detach, deserialize);
    }

    /**
     * Get position to be used for handle. We assume here that the hdr byte was read, hence subtract -1.
     *
     * @return Position for handle.
     */
    static int positionForHandle(BinaryInputStream in) {
        return in.position() - 1;
    }

    /**
     * Check if class is binarylizable.
     *
     * @param cls Class.
     * @return {@code True} if binarylizable.
     */
    static boolean isBinarylizable(Class cls) {
        for (Class c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
            if (Binarylizable.class.isAssignableFrom(c))
                return true;
        }

        return false;
    }

    /**
     * Determines whether class contains custom Java serialization logic.
     *
     * @param cls Class.
     * @return {@code true} if custom Java serialization logic exists, {@code false} otherwise.
     */
    @SuppressWarnings("unchecked")
    static boolean isCustomJavaSerialization(Class cls) {
        for (Class c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
            if (Externalizable.class.isAssignableFrom(c))
                return true;

            try {
                Method writeObj = c.getDeclaredMethod("writeObject", ObjectOutputStream.class);
                Method readObj = c.getDeclaredMethod("readObject", ObjectInputStream.class);

                if (!Modifier.isStatic(writeObj.getModifiers()) && !Modifier.isStatic(readObj.getModifiers()) &&
                    writeObj.getReturnType() == void.class && readObj.getReturnType() == void.class)
                    return true;
            }
            catch (NoSuchMethodException ignored) {
                // No-op.
            }
        }

        return false;
    }

    /**
     * Create qualified field name.
     *
     * @param cls Class.
     * @param fieldName Field name.
     * @return Qualified field name.
     */
    static String qualifiedFieldName(Class cls, String fieldName) {
        return cls.getName() + "." + fieldName;
    }

    /**
     * Reconstructs string from UTF-8 bytes.
     *
     * @param arr array Byte array.
     * @param off offset Offset in the array.
     * @param len length Byte array lenght.
     * @return string Resulting string.
     */
    static String utf8BytesToStr(byte[] arr, int off, int len) {
        int c, charArrCnt = 0, total = off + len;
        int c2, c3;
        char[] res = new char[len];

        // try reading ascii
        while (off < total) {
            c = (int)arr[off] & 0xff;

            if (c > 127)
                break;

            off++;

            res[charArrCnt++] = (char)c;
        }

        // read other
        while (off < total) {
            c = (int)arr[off] & 0xff;

            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx*/
                    off++;

                    res[charArrCnt++] = (char)c;

                    break;
                case 12:
                case 13:
                    /* 110x xxxx   10xx xxxx*/
                    off += 2;

                    if (off > total)
                        throw new BinaryObjectException("Malformed input: partial character at end");

                    c2 = (int)arr[off - 1];

                    if ((c2 & 0xC0) != 0x80)
                        throw new BinaryObjectException("Malformed input around byte: " + off);

                    res[charArrCnt++] = (char)(((c & 0x1F) << 6) | (c2 & 0x3F));

                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    off += 3;

                    if (off > total)
                        throw new BinaryObjectException("Malformed input: partial character at end");

                    c2 = (int)arr[off - 2];

                    c3 = (int)arr[off - 1];

                    if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80))
                        throw new BinaryObjectException("Malformed input around byte: " + (off - 1));

                    res[charArrCnt++] = (char)(((c & 0x0F) << 12) |
                        ((c2 & 0x3F) << 6) | (c3 & 0x3F));

                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new BinaryObjectException("Malformed input around byte: " + off);
            }
        }

        return len == charArrCnt ? new String(res) : new String(res, 0, charArrCnt);
    }

    /**
     * Converts the string into UTF-8 byte array considering special symbols like the surrogates.
     *
     * @param val String to convert.
     * @return Resulting byte array.
     */
    static byte[] strToUtf8Bytes(String val) {
        int strLen = val.length();
        int utfLen = 0;
        int c, cnt;

        // Determine length of resulting byte array.
        for (cnt = 0; cnt < strLen; cnt++) {
            c = val.charAt(cnt);

            if (c >= 0x0001 && c <= 0x007F)
                utfLen++;
            else if (c > 0x07FF)
                utfLen += 3;
            else
                utfLen += 2;
        }

        byte[] arr = new byte[utfLen];

        int position = 0;

        for (cnt = 0; cnt < strLen; cnt++) {
            c = val.charAt(cnt);

            if (c >= 0x0001 && c <= 0x007F)
                arr[position++] = (byte)c;
            else if (c > 0x07FF) {
                arr[position++] = (byte)(0xE0 | (c >> 12) & 0x0F);
                arr[position++] = (byte)(0x80 | (c >> 6) & 0x3F);
                arr[position++] = (byte)(0x80 | (c & 0x3F));
            }
            else {
                arr[position++] = (byte)(0xC0 | ((c >> 6) & 0x1F));
                arr[position++] = (byte)(0x80 | (c & 0x3F));
            }
        }

        return arr;
    }

    /**
     * Create binary type proxy.
     *
     * @param ctx Context.
     * @param obj Binary object.
     * @return Binary type proxy.
     */
    static BinaryType typeProxy(BinaryContext ctx, BinaryObjectEx obj) {
        if (ctx == null)
            throw new BinaryObjectException("BinaryContext is not set for the object.");

        String clsName = isBinaryEnumObject(obj) ? obj.enumClassName() : null;

        return new BinaryTypeProxy(ctx, obj.typeId(), clsName);
    }

    /**
     * Create binary type which is used by users.
     *
     * @param ctx Context.
     * @param obj Binary object.
     * @return Binary type.
     */
    static BinaryType type(BinaryContext ctx, BinaryObjectEx obj) {
        if (ctx == null)
            throw new BinaryObjectException("BinaryContext is not set for the object.");

        return ctx.metadata(obj.typeId());
    }

    /** @param fileName Name of file with marshaller mapping information. */
    public static int mappedTypeId(String fileName) {
        try {
            return Integer.parseInt(fileName.substring(0, fileName.indexOf(MAPPING_FILE_EXTENSION)));
        }
        catch (NumberFormatException e) {
            throw new IgniteException("Reading marshaller mapping from file "
                + fileName
                + " failed; type ID is expected to be numeric.", e);
        }
    }

    /** @param fileName Name of file with marshaller mapping information. */
    public static byte mappedFilePlatformId(String fileName) {
        try {
            return Byte.parseByte(fileName.substring(fileName.length() - 1));
        }
        catch (NumberFormatException e) {
            throw new IgniteException("Reading marshaller mapping from file "
                + fileName
                + " failed; last symbol of file name is expected to be numeric.", e);
        }
    }

    /** @param file File. */
    public static String readMapping(File file) {
        try (FileInputStream in = new FileInputStream(file)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                if (file.length() == 0)
                    return null;

                return reader.readLine();
            }

        }
        catch (IOException ignored) {
            return null;
        }
    }

    /**
     * @param platformId Platform id.
     * @param typeId Type id.
     */
    public static String mappingFileName(byte platformId, int typeId) {
        return typeId + MAPPING_FILE_EXTENSION + platformId;
    }

    /**
     * Get predefined write-replacer associated with class.
     *
     * @param cls Class.
     * @return Write replacer.
     */
    static BinaryWriteReplacer writeReplacer(Class cls) {
        return cls != null ? CLS_TO_WRITE_REPLACER.get(cls) : null;
    }

    /**
     * Checks enum values mapping.
     *
     * @param typeName Name of the type.
     * @param enumValues Enum name to ordinal mapping.
     * @throws BinaryObjectException
     */
    public static void validateEnumValues(String typeName, @Nullable Map<String, Integer> enumValues)
            throws BinaryObjectException {

        if (enumValues == null)
            return;

        Map<Integer, String> tmpMap = new LinkedHashMap<>(enumValues.size());
        for (Map.Entry<String, Integer> e: enumValues.entrySet()) {

            String prevName = tmpMap.put(e.getValue(), e.getKey());

            if (prevName != null)
                throw new BinaryObjectException("Conflicting enum values. Name '" + e.getKey() +
                        "' uses ordinal value (" + e.getValue() +
                        ") that is also used for name '" + prevName +
                        "' [typeName='" + typeName + "']");
        }
    }

    /**
     * Merges enum value mappings and checks for conflicts.
     *
     * Possible conflicts:
     * - same name is used for different ordinal values.
     * - ordinal value is used more than once.
     *
     * @param typeName Name of the type.
     * @param oldValues Old enum value mapping.
     * @param newValues New enum value mapping.
     * @throws BinaryObjectException in case of name or value conflict.
     */
    private static Map<String, Integer> mergeEnumValues(String typeName,
            @Nullable Map<String, Integer> oldValues,
            Map<String, Integer> newValues)
            throws BinaryObjectException {

        assert newValues != null;

        int size = (oldValues != null) ? oldValues.size() + newValues.size() : newValues.size();

        Map<Integer, String> revMap = new LinkedHashMap<>(size);
        Map<String, Integer> mergedMap = new LinkedHashMap<>(size);

        if (oldValues != null) {
            //assuming that old values were validated earlier once.
            for (Map.Entry<String, Integer> e : oldValues.entrySet()) {
                revMap.put(e.getValue(), e.getKey());
                mergedMap.put(e.getKey(), e.getValue());
            }
        }

        for (Map.Entry<String, Integer> e: newValues.entrySet()) {
            String prevName = revMap.put(e.getValue(), e.getKey());

            if (prevName != null && !prevName.equals(e.getKey()))
                throw new BinaryObjectException("Conflicting enum values. Name '" + e.getKey() +
                        "' uses ordinal value (" + e.getValue() +
                        ") that is also used for name '" + prevName +
                        "' [typeName='" + typeName + "']");

            Integer prevVal = mergedMap.put(e.getKey(), e.getValue());

            if (prevVal != null && !prevVal.equals(e.getValue()))
                throw new BinaryObjectException("Conflicting enum values. Value (" + e.getValue() +
                        ") has name '" + e.getKey() +
                        "' that is also used for value '" + prevVal +
                        "' [typeName='" + typeName + "']");
        }

        return mergedMap;
    }

    /**
     * @param obj {@link BinaryArray} or {@code Object[]}.
     * @return Objects array.
     */
    public static Object[] rawArrayFromBinary(Object obj) {
        if (isBinaryArray(obj))
            // We want raw data(no deserialization).
            return ((BinaryObjectEx)obj).array();
        else
            // This can happen even in BinaryArray.USE_TYPED_ARRAY = true.
            // In case user pass special array type to arguments, String[], for example.
            return (Object[])obj;
    }

    /** */
    public static boolean isObjectArray(Class<?> cls) {
        return Object[].class == cls || BinaryArray.class == cls || BinaryEnumArray.class == cls;
    }

    /** @return Type name of the specified object. If {@link BinaryObject} was specified its type will be returned. */
    public static String typeName(Object obj) {
        return obj instanceof BinaryObject
            ? ((BinaryObject)obj).type().typeName()
            : obj == null ? null : obj.getClass().getSimpleName();
    }

    /**
     * Clears binary caches.
     */
    public static void clearCache() {
        BinaryEnumCache.clear();
    }

    /**
     * Gets the schema.
     *
     * @param cacheObjProc Cache object processor.
     * @param typeId Type id.
     * @param schemaId Schema id.
     */
    public static int[] getSchema(CacheObjectBinaryProcessorImpl cacheObjProc, int typeId, int schemaId) {
        assert cacheObjProc != null;

        BinarySchemaRegistry schemaReg = cacheObjProc.binaryContext().schemaRegistry(typeId);
        BinarySchema schema = schemaReg.schema(schemaId);

        if (schema == null) {
            BinaryTypeImpl meta = (BinaryTypeImpl)cacheObjProc.metadata(typeId);

            if (meta != null) {
                for (BinarySchema typeSchema : meta.metadata().schemas()) {
                    if (schemaId == typeSchema.schemaId()) {
                        schema = typeSchema;
                        break;
                    }
                }
            }

            if (schema != null)
                schemaReg.addSchema(schemaId, schema);
        }

        return schema == null ? null : schema.fieldIds();
    }

    /**
     * @return Unwrap function for object size calculation.
     */
    public static Map<Class<?>, Function<Object, Object>> unwrapFuncForSizeCalc() {
        return Map.of(
            BinaryArray.class, bo -> ((BinaryArray)bo).array(),
            BinaryEnumArray.class, bo -> ((BinaryArray)bo).array()
        );
    }

    /**
     * @return Map of function returning size of the object.
     */
    public static Map<Class<?>, ToIntFunction<Object>> sizeProviders() {
        return Map.of(
            BinaryObjectOffheapImpl.class, obj -> 0, // No extra heap memory.
            BinaryObjectImpl.class, new ToIntFunction<>() {
                private final long byteArrOffset = GridUnsafe.arrayBaseOffset(byte[].class);

                @Override public int applyAsInt(Object bo) {
                    return (int)align(byteArrOffset + ((BinaryObjectImpl)bo).bytes().length);
                }
            },
            BinaryEnumObjectImpl.class, bo -> ((BinaryObject)bo).size()
        );
    }

    /**
     * @param val Value to check.
     * @return {@code True} if {@code val} instance of {@link BinaryEnumArray}.
     */
    public static boolean isBinaryEnumArray(Object val) {
        return val instanceof BinaryEnumArray;
    }

    /**
     * @param val Value to check.
     * @return {@code True} if {@code val} instance of binary Enum object.
     */
    public static boolean isBinaryEnumObject(Object val) {
        return val instanceof BinaryEnumObjectImpl;
    }

    /**
     * @param cls Class to check.
     * @return {@code True} if {@code val} is assignable to binary Enum object.
     */
    public static boolean isAssignableToBinaryEnumObject(Class<?> cls) {
        return BinaryEnumObjectImpl.class.isAssignableFrom(cls);
    }

    /**
     * Creates reader instance.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param forUnmarshal {@code True} if reader is needed to unmarshal object.
     */
    public static BinaryReaderEx reader(BinaryContext ctx, BinaryInputStream in, ClassLoader ldr, boolean forUnmarshal) {
        return new BinaryReaderExImpl(ctx, in, ldr, forUnmarshal);
    }

    /**
     * Creates reader instance.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param reader BinaryReaderEx.
     * @param forUnmarshal {@code True} if reader is need to unmarshal object.
     */
    public static BinaryReaderEx reader(BinaryContext ctx,
        BinaryInputStream in,
        ClassLoader ldr,
        BinaryReaderEx reader,
        boolean forUnmarshal) {
        return reader(ctx, in, ldr, reader.handles(), forUnmarshal);
    }

    /**
     * Creates reader instance.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param hnds Context.
     * @param forUnmarshal {@code True} if reader is need to unmarshal object.
     */
    static BinaryReaderEx reader(BinaryContext ctx,
                                        BinaryInputStream in,
                                        ClassLoader ldr,
                                        @Nullable BinaryReaderHandles hnds,
                                        boolean forUnmarshal) {
        return new BinaryReaderExImpl(ctx, in, ldr, hnds, forUnmarshal);
    }

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param skipHdrCheck Whether to skip header check.
     * @param forUnmarshal {@code True} if reader is need to unmarshal object.
     */
    public static BinaryReaderEx reader(BinaryContext ctx,
        BinaryInputStream in,
        ClassLoader ldr,
        boolean skipHdrCheck,
        boolean forUnmarshal) {
        return reader(ctx, in, ldr, null, skipHdrCheck, forUnmarshal);
    }

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     * @param hnds Context.
     * @param skipHdrCheck Whether to skip header check.
     * @param forUnmarshal {@code True} if reader is need to unmarshal object.
     */
    static BinaryReaderEx reader(BinaryContext ctx,
                                        BinaryInputStream in,
                                        ClassLoader ldr,
                                        @Nullable BinaryReaderHandles hnds,
                                        boolean skipHdrCheck,
                                        boolean forUnmarshal) {
        return new BinaryReaderExImpl(ctx, in, ldr, hnds, skipHdrCheck, forUnmarshal);
    }

    /**
     * @param ctx Context.
     * @return Writer instance.
     */
    public static BinaryWriterEx writer(BinaryContext ctx) {
        BinaryThreadLocalContext locCtx = BinaryThreadLocalContext.get();

        return new BinaryWriterExImpl(ctx, BinaryStreams.outputStream((int)U.KB, locCtx.chunk()), locCtx.schemaHolder(), null);
    }

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @return Writer instance.
     */
    public static BinaryWriterEx writer(BinaryContext ctx, BinaryOutputStream out) {
        return new BinaryWriterExImpl(ctx, out, BinaryThreadLocalContext.get().schemaHolder(), null);
    }

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @return Writer instance.
     */
    public static BinaryWriterEx writer(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema) {
        return new BinaryWriterExImpl(ctx, out, schema, null);
    }

    /** @return Instance of caching handler. */
    public static BinaryMetadataHandler cachingMetadataHandler() {
        return BinaryCachingMetadataHandler.create();
    }

    /** @return Instance of noop handler. */
    public static BinaryMetadataHandler noopMetadataHandler() {
        return BinaryNoopMetadataHandler.instance();
    }

    /**
     * @param val Value to check.
     * @return {@code True} if {@code val} instance of {@link BinaryObjectExImpl}.
     */
    public static boolean isBinaryObjectExImpl(Object val) {
        return val instanceof BinaryObjectExImpl;
    }

    /**
     * @param val Value to check.
     * @return {@code True} if {@code val} instance of {@link BinaryObjectImpl}.
     */
    public static boolean isBinaryObjectImpl(Object val) {
        return val instanceof BinaryObjectImpl;
    }

    /**
     * @param val Value to check.
     * @return {@code True} if {@code val} instance of {@link BinaryArray}.
     */
    public static boolean isBinaryArray(Object val) {
        return val instanceof BinaryArray;
    }

    /** @return {@code True} if typed arrays should be used, {@code false} otherwise. */
    public static boolean useBinaryArrays() {
        return USE_BINARY_ARRAYS;
    }

    /**
     * Initialize {@link #USE_BINARY_ARRAYS} value with
     * {@link IgniteSystemProperties#IGNITE_USE_BINARY_ARRAYS} system property value.
     *
     * This method invoked using reflection in tests.
     */
    public static void initUseBinaryArrays() {
        USE_BINARY_ARRAYS = IgniteSystemProperties.getBoolean(IGNITE_USE_BINARY_ARRAYS, DFLT_IGNITE_USE_BINARY_ARRAYS);
    }

    /**
     * @param fieldId Field id.
     * @return {@link BinaryObjectExImpl#field(int)} value or {@code null} if object not instance of {@link BinaryObjectExImpl}.
     */
    public static Object field(Object obj, int fieldId) {
        if (!(obj instanceof BinaryObjectExImpl))
            return null;

        return ((BinaryObjectExImpl)obj).field(fieldId);
    }

    /**
     * Check for arrays equality.
     *
     * @param a1 Value 1.
     * @param a2 Value 2.
     * @return {@code True} if arrays equal.
     */
    public static boolean arrayEq(Object a1, Object a2) {
        if (a1 == a2)
            return true;

        if (a1 == null || a2 == null)
            return a1 != null || a2 != null;

        if (a1.getClass() != a2.getClass())
            return false;

        if (a1 instanceof byte[])
            return Arrays.equals((byte[])a1, (byte[])a2);
        else if (a1 instanceof boolean[])
            return Arrays.equals((boolean[])a1, (boolean[])a2);
        else if (a1 instanceof short[])
            return Arrays.equals((short[])a1, (short[])a2);
        else if (a1 instanceof char[])
            return Arrays.equals((char[])a1, (char[])a2);
        else if (a1 instanceof int[])
            return Arrays.equals((int[])a1, (int[])a2);
        else if (a1 instanceof long[])
            return Arrays.equals((long[])a1, (long[])a2);
        else if (a1 instanceof float[])
            return Arrays.equals((float[])a1, (float[])a2);
        else if (a1 instanceof double[])
            return Arrays.equals((double[])a1, (double[])a2);
        else if (isBinaryArray(a1))
            return a1.equals(a2);

        return Arrays.deepEquals((Object[])a1, (Object[])a2);
    }

    /**
     * Compare two objects for DML operation.
     *
     * @param first First.
     * @param second Second.
     * @return Comparison result.
     */
    public static int compareForDml(Object first, Object second) {
        return BinaryObjectImpl.compareForDml(first, second);
    }

    /**
     * @param o Object to detach.
     * @return Detached object.
     */
    public static Object detach(Object o) {
        ((BinaryObjectImpl)o).detachAllowed(true);
        return ((BinaryObjectImpl)o).detach();
    }

    /**
     * @param o
     */
    public static void detachAllowedIfPossible(Object o) {
        if (isBinaryObjectImpl(o))
            ((BinaryObjectImpl)o).detachAllowed(true);
    }

    /**
     * @param meta Binary metadata.
     * @return Schemas identifiers of the specified {@link BinaryMetadata}.
     */
    public static Collection<T2<Integer, int[]>> schemasAndFieldsIds(BinaryMetadata meta) {
        return F.viewReadOnly(meta.schemas(), s -> new T2<>(s.schemaId(), s.fieldIds()));
    }

    /**
     * Gets field by its order.
     *
     * @param reader Reader.
     * @param order Order.
     */
    public static int fieldId(BinaryReaderEx reader, int order) {
        return ((BinaryReaderExImpl)reader).getOrCreateSchema().fieldId(order);
    }

    /**
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param fields Fields map.
     * @param affKeyFieldName Affinity key field name.
     * @param schemasAndFieldIds Schemas and fields identifiers.
     * @param isEnum Enum flag.
     * @param enumMap Enum name to ordinal mapping.
     * @return New instance of {@link BinaryMetadata}.
     */
    public static BinaryMetadata binaryMetadata(
        int typeId,
        String typeName,
        @Nullable Map<String, BinaryFieldMetadata> fields,
        @Nullable String affKeyFieldName,
        @Nullable Collection<T2<Integer, List<Integer>>> schemasAndFieldIds,
        boolean isEnum,
        @Nullable Map<String, Integer> enumMap) {
        List<BinarySchema> schemas = F.isEmpty(schemasAndFieldIds) ? null : schemasAndFieldIds.stream()
            .map(t -> new BinarySchema(t.get1(), t.get2()))
            .collect(Collectors.toList());

        return new BinaryMetadata(typeId, typeName, fields, affKeyFieldName, schemas, isEnum, enumMap);
    }

    /**
     * Enum type.
     */
    private static class EnumType {
        /** Type ID. */
        private final int typeId;

        /** Class name. */
        private final String clsName;

        /**
         * Constructor.
         *
         * @param typeId Type ID.
         * @param clsName Class name.
         */
        public EnumType(int typeId, @Nullable String clsName) {
            assert typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID && clsName == null ||
                typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID && clsName != null;

            this.typeId = typeId;
            this.clsName = clsName;
        }
    }
}
