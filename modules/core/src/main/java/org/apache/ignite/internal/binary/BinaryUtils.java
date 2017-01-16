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

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryCollectionFactory;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryMapFactory;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.binary.builder.BinaryLazyValue;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Binary utils.
 */
public class BinaryUtils {
    /** */
    public static final Map<Class<?>, Byte> PLAIN_CLASS_TO_FLAG = new HashMap<>();

    /** */
    public static final Map<Byte, Class<?>> FLAG_TO_CLASS = new HashMap<>();

    /** */
    public static final boolean USE_STR_SERIALIZATION_VER_2 = IgniteSystemProperties.getBoolean(
        IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2, false);

    /** Map from class to associated write replacer. */
    public static final Map<Class, BinaryWriteReplacer> CLS_TO_WRITE_REPLACER = new HashMap<>();

    /** {@code true} if serialized value of this type cannot contain references to objects. */
    private static final boolean[] PLAIN_TYPE_FLAG = new boolean[102];

    /** Binary classes. */
    private static final Collection<Class<?>> BINARY_CLS = new HashSet<>();

    /** Flag: user type. */
    public static final short FLAG_USR_TYP = 0x0001;

    /** Flag: only raw data exists. */
    public static final short FLAG_HAS_SCHEMA = 0x0002;

    /** Flag indicating that object has raw data. */
    public static final short FLAG_HAS_RAW = 0x0004;

    /** Flag: offsets take 1 byte. */
    public static final short FLAG_OFFSET_ONE_BYTE = 0x0008;

    /** Flag: offsets take 2 bytes. */
    public static final short FLAG_OFFSET_TWO_BYTES = 0x0010;

    /** Flag: compact footer, no field IDs. */
    public static final short FLAG_COMPACT_FOOTER = 0x0020;

    /** Flag: no hash code has been set. */
    public static final short FLAG_EMPTY_HASH_CODE = 0x0040;

    /** Offset which fits into 1 byte. */
    public static final int OFFSET_1 = 1;

    /** Offset which fits into 2 bytes. */
    public static final int OFFSET_2 = 2;

    /** Offset which fits into 4 bytes. */
    public static final int OFFSET_4 = 4;

    /** Field ID length. */
    public static final int FIELD_ID_LEN = 4;

    /** Whether to skip TreeMap/TreeSet wrapping. */
    public static final boolean WRAP_TREES =
        !IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_BINARY_DONT_WRAP_TREE_STRUCTURES);

    /** Whether to sort field in binary objects (doesn't affect Binarylizable). */
    public static final boolean FIELDS_SORTED_ORDER =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_BINARY_SORT_OBJECT_FIELDS);

    /** Field type names. */
    private static final String[] FIELD_TYPE_NAMES;

    /** FNV1 hash offset basis. */
    private static final int FNV1_OFFSET_BASIS = 0x811C9DC5;

    /** FNV1 hash prime. */
    private static final int FNV1_PRIME = 0x01000193;

    /**
     * Static class initializer.
     */
    static {
        PLAIN_CLASS_TO_FLAG.put(Byte.class, GridBinaryMarshaller.BYTE);
        PLAIN_CLASS_TO_FLAG.put(Short.class, GridBinaryMarshaller.SHORT);
        PLAIN_CLASS_TO_FLAG.put(Integer.class, GridBinaryMarshaller.INT);
        PLAIN_CLASS_TO_FLAG.put(Long.class, GridBinaryMarshaller.LONG);
        PLAIN_CLASS_TO_FLAG.put(Float.class, GridBinaryMarshaller.FLOAT);
        PLAIN_CLASS_TO_FLAG.put(Double.class, GridBinaryMarshaller.DOUBLE);
        PLAIN_CLASS_TO_FLAG.put(Character.class, GridBinaryMarshaller.CHAR);
        PLAIN_CLASS_TO_FLAG.put(Boolean.class, GridBinaryMarshaller.BOOLEAN);
        PLAIN_CLASS_TO_FLAG.put(BigDecimal.class, GridBinaryMarshaller.DECIMAL);
        PLAIN_CLASS_TO_FLAG.put(String.class, GridBinaryMarshaller.STRING);
        PLAIN_CLASS_TO_FLAG.put(UUID.class, GridBinaryMarshaller.UUID);
        PLAIN_CLASS_TO_FLAG.put(Date.class, GridBinaryMarshaller.DATE);
        PLAIN_CLASS_TO_FLAG.put(Timestamp.class, GridBinaryMarshaller.TIMESTAMP);

        PLAIN_CLASS_TO_FLAG.put(byte[].class, GridBinaryMarshaller.BYTE_ARR);
        PLAIN_CLASS_TO_FLAG.put(short[].class, GridBinaryMarshaller.SHORT_ARR);
        PLAIN_CLASS_TO_FLAG.put(int[].class, GridBinaryMarshaller.INT_ARR);
        PLAIN_CLASS_TO_FLAG.put(long[].class, GridBinaryMarshaller.LONG_ARR);
        PLAIN_CLASS_TO_FLAG.put(float[].class, GridBinaryMarshaller.FLOAT_ARR);
        PLAIN_CLASS_TO_FLAG.put(double[].class, GridBinaryMarshaller.DOUBLE_ARR);
        PLAIN_CLASS_TO_FLAG.put(char[].class, GridBinaryMarshaller.CHAR_ARR);
        PLAIN_CLASS_TO_FLAG.put(boolean[].class, GridBinaryMarshaller.BOOLEAN_ARR);
        PLAIN_CLASS_TO_FLAG.put(BigDecimal[].class, GridBinaryMarshaller.DECIMAL_ARR);
        PLAIN_CLASS_TO_FLAG.put(String[].class, GridBinaryMarshaller.STRING_ARR);
        PLAIN_CLASS_TO_FLAG.put(UUID[].class, GridBinaryMarshaller.UUID_ARR);
        PLAIN_CLASS_TO_FLAG.put(Date[].class, GridBinaryMarshaller.DATE_ARR);
        PLAIN_CLASS_TO_FLAG.put(Timestamp[].class, GridBinaryMarshaller.TIMESTAMP_ARR);

        for (Map.Entry<Class<?>, Byte> entry : PLAIN_CLASS_TO_FLAG.entrySet())
            FLAG_TO_CLASS.put(entry.getValue(), entry.getKey());

        PLAIN_CLASS_TO_FLAG.put(byte.class, GridBinaryMarshaller.BYTE);
        PLAIN_CLASS_TO_FLAG.put(short.class, GridBinaryMarshaller.SHORT);
        PLAIN_CLASS_TO_FLAG.put(int.class, GridBinaryMarshaller.INT);
        PLAIN_CLASS_TO_FLAG.put(long.class, GridBinaryMarshaller.LONG);
        PLAIN_CLASS_TO_FLAG.put(float.class, GridBinaryMarshaller.FLOAT);
        PLAIN_CLASS_TO_FLAG.put(double.class, GridBinaryMarshaller.DOUBLE);
        PLAIN_CLASS_TO_FLAG.put(char.class, GridBinaryMarshaller.CHAR);
        PLAIN_CLASS_TO_FLAG.put(boolean.class, GridBinaryMarshaller.BOOLEAN);

        for (byte b : new byte[] {
            GridBinaryMarshaller.BYTE, GridBinaryMarshaller.SHORT, GridBinaryMarshaller.INT, GridBinaryMarshaller.LONG, GridBinaryMarshaller.FLOAT, GridBinaryMarshaller.DOUBLE,
            GridBinaryMarshaller.CHAR, GridBinaryMarshaller.BOOLEAN, GridBinaryMarshaller.DECIMAL, GridBinaryMarshaller.STRING, GridBinaryMarshaller.UUID, GridBinaryMarshaller.DATE, GridBinaryMarshaller.TIMESTAMP,
            GridBinaryMarshaller.BYTE_ARR, GridBinaryMarshaller.SHORT_ARR, GridBinaryMarshaller.INT_ARR, GridBinaryMarshaller.LONG_ARR, GridBinaryMarshaller.FLOAT_ARR, GridBinaryMarshaller.DOUBLE_ARR,
            GridBinaryMarshaller.CHAR_ARR, GridBinaryMarshaller.BOOLEAN_ARR, GridBinaryMarshaller.DECIMAL_ARR, GridBinaryMarshaller.STRING_ARR, GridBinaryMarshaller.UUID_ARR, GridBinaryMarshaller.DATE_ARR, GridBinaryMarshaller.TIMESTAMP_ARR,
            GridBinaryMarshaller.ENUM, GridBinaryMarshaller.ENUM_ARR, GridBinaryMarshaller.NULL}) {

            PLAIN_TYPE_FLAG[b] = true;
        }

        BINARY_CLS.add(Byte.class);
        BINARY_CLS.add(Short.class);
        BINARY_CLS.add(Integer.class);
        BINARY_CLS.add(Long.class);
        BINARY_CLS.add(Float.class);
        BINARY_CLS.add(Double.class);
        BINARY_CLS.add(Character.class);
        BINARY_CLS.add(Boolean.class);
        BINARY_CLS.add(String.class);
        BINARY_CLS.add(UUID.class);
        BINARY_CLS.add(Date.class);
        BINARY_CLS.add(Timestamp.class);
        BINARY_CLS.add(BigDecimal.class);
        BINARY_CLS.add(byte[].class);
        BINARY_CLS.add(short[].class);
        BINARY_CLS.add(int[].class);
        BINARY_CLS.add(long[].class);
        BINARY_CLS.add(float[].class);
        BINARY_CLS.add(double[].class);
        BINARY_CLS.add(char[].class);
        BINARY_CLS.add(boolean[].class);
        BINARY_CLS.add(String[].class);
        BINARY_CLS.add(UUID[].class);
        BINARY_CLS.add(Date[].class);
        BINARY_CLS.add(Timestamp[].class);
        BINARY_CLS.add(BigDecimal[].class);

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
        FIELD_TYPE_NAMES[GridBinaryMarshaller.OBJ_ARR] = "Object[]";
        FIELD_TYPE_NAMES[GridBinaryMarshaller.ENUM_ARR] = "Enum[]";

        if (wrapTrees()) {
            CLS_TO_WRITE_REPLACER.put(TreeMap.class, new BinaryTreeMapWriteReplacer());
            CLS_TO_WRITE_REPLACER.put(TreeSet.class, new BinaryTreeSetWriteReplacer());
        }
    }

    /**
     * Check if user type flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    public static boolean isUserType(short flags) {
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
    public static boolean hasRaw(short flags) {
        return isFlagSet(flags, FLAG_HAS_RAW);
    }

    /**
     * Check if "no-field-ids" flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    public static boolean isCompactFooter(short flags) {
        return isFlagSet(flags, FLAG_COMPACT_FOOTER);
    }

    /**
     * Check whether particular flag is set.
     *
     * @param flags Flags.
     * @param flag Flag.
     * @return {@code True} if flag is set in flags.
     */
    public static boolean isFlagSet(short flags, short flag) {
        return (flags & flag) == flag;
    }

    /**
     * Schema initial ID.
     *
     * @return ID.
     */
    public static int schemaInitialId() {
        return FNV1_OFFSET_BASIS;
    }

    /**
     * Update schema ID when new field is added.
     *
     * @param schemaId Current schema ID.
     * @param fieldId Field ID.
     * @return New schema ID.
     */
    public static int updateSchemaId(int schemaId, int fieldId) {
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
     * @return Field type name.
     */
    public static String fieldTypeName(int typeId) {
        assert typeId >= 0 && typeId < FIELD_TYPE_NAMES.length : typeId;

        String typeName = FIELD_TYPE_NAMES[typeId];

        assert typeName != null : typeId;

        return typeName;
    }

    /**
     * Write value with flag. e.g. writePlainObject(writer, (byte)77) will write two byte: {BYTE, 77}.
     *
     * @param writer W
     * @param val Value.
     */
    public static void writePlainObject(BinaryWriterExImpl writer, Object val) {
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
                writer.doWriteDecimal((BigDecimal)val);

                break;

            case GridBinaryMarshaller.STRING:
                writer.doWriteString((String)val);

                break;

            case GridBinaryMarshaller.UUID:
                writer.doWriteUuid((UUID)val);

                break;

            case GridBinaryMarshaller.DATE:
                writer.doWriteDate((Date)val);

                break;

            case GridBinaryMarshaller.TIMESTAMP:
                writer.doWriteTimestamp((Timestamp)val);

                break;

            case GridBinaryMarshaller.BYTE_ARR:
                writer.doWriteByteArray((byte[])val);

                break;

            case GridBinaryMarshaller.SHORT_ARR:
                writer.doWriteShortArray((short[])val);

                break;

            case GridBinaryMarshaller.INT_ARR:
                writer.doWriteIntArray((int[])val);

                break;

            case GridBinaryMarshaller.LONG_ARR:
                writer.doWriteLongArray((long[])val);

                break;

            case GridBinaryMarshaller.FLOAT_ARR:
                writer.doWriteFloatArray((float[])val);

                break;

            case GridBinaryMarshaller.DOUBLE_ARR:
                writer.doWriteDoubleArray((double[])val);

                break;

            case GridBinaryMarshaller.CHAR_ARR:
                writer.doWriteCharArray((char[])val);

                break;

            case GridBinaryMarshaller.BOOLEAN_ARR:
                writer.doWriteBooleanArray((boolean[])val);

                break;

            case GridBinaryMarshaller.DECIMAL_ARR:
                writer.doWriteDecimalArray((BigDecimal[])val);

                break;

            case GridBinaryMarshaller.STRING_ARR:
                writer.doWriteStringArray((String[])val);

                break;

            case GridBinaryMarshaller.UUID_ARR:
                writer.doWriteUuidArray((UUID[])val);

                break;

            case GridBinaryMarshaller.DATE_ARR:
                writer.doWriteDateArray((Date[])val);

                break;

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                writer.doWriteTimestampArray((Timestamp[])val);

                break;

            default:
                throw new IllegalArgumentException("Can't write object with type: " + val.getClass());
        }
    }

    /**
     * @param obj Value to unwrap.
     * @return Unwrapped value.
     */
    public static Object unwrapLazy(@Nullable Object obj) {
        if (obj instanceof BinaryLazyValue)
            return ((BinaryLazyValue)obj).value();

        return obj;
    }

    /**
     * @param delegate Iterator to delegate.
     * @return New iterator.
     */
    public static Iterator<Object> unwrapLazyIterator(final Iterator<Object> delegate) {
        return new Iterator<Object>() {
            @Override public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override public Object next() {
                return unwrapLazy(delegate.next());
            }

            @Override public void remove() {
                delegate.remove();
            }
        };
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
        return (type >= GridBinaryMarshaller.BYTE_ARR && type <= GridBinaryMarshaller.DATE_ARR) || type == GridBinaryMarshaller.TIMESTAMP_ARR;
    }

    /**
     * @param cls Class.
     * @return Binary field type.
     */
    public static byte typeByClass(Class<?> cls) {
        Byte type = PLAIN_CLASS_TO_FLAG.get(cls);

        if (type != null)
            return type;

        if (cls.isEnum())
            return GridBinaryMarshaller.ENUM;

        if (cls.isArray())
            return cls.getComponentType().isEnum() || cls.getComponentType() == Enum.class ? GridBinaryMarshaller.ENUM_ARR : GridBinaryMarshaller.OBJ_ARR;

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
     * @return Whether tree structures should be wrapped.
     */
    public static boolean wrapTrees() {
        return WRAP_TREES;
    }

    /**
     * @param map Map to check.
     * @return {@code True} if this map type is supported.
     */
    public static boolean knownMap(Object map) {
        Class<?> cls = map == null ? null : map.getClass();

        return cls == HashMap.class ||
            cls == LinkedHashMap.class ||
            (!wrapTrees() && cls == TreeMap.class) ||
            cls == ConcurrentHashMap8.class ||
            cls == ConcurrentHashMap.class;
    }

    /**
     * Attempts to create a new map of the same known type. Will return null if map type is not supported.
     *
     * @param map Map.
     * @return New map of the same type or null.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> newKnownMap(Object map) {
        Class<?> cls = map == null ? null : map.getClass();

        if (cls == HashMap.class)
            return U.newHashMap(((Map)map).size());
        else if (cls == LinkedHashMap.class)
            return U.newLinkedHashMap(((Map)map).size());
        else if (!wrapTrees() && cls == TreeMap.class)
            return new TreeMap<>(((TreeMap<Object, Object>)map).comparator());
        else if (cls == ConcurrentHashMap8.class)
            return new ConcurrentHashMap8<>(U.capacity(((Map)map).size()));
        else if (cls == ConcurrentHashMap.class)
            return new ConcurrentHashMap<>(U.capacity(((Map)map).size()));

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
        else if (map instanceof ConcurrentHashMap8)
            return new ConcurrentHashMap8<>(U.capacity(map.size()));
        else if (map instanceof ConcurrentHashMap)
            return new ConcurrentHashMap<>(U.capacity(map.size()));

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
            (!wrapTrees() && cls == TreeSet.class) ||
            cls == ConcurrentSkipListSet.class ||
            cls == ArrayList.class ||
            cls == LinkedList.class;
    }

    /**
     * @param arr Array to check.
     * @return {@code true} if this array is of a known type.
     */
    public static boolean knownArray(Object arr) {
        if (arr == null)
            return false;

        Class<?> cls =  arr.getClass();

        return cls == byte[].class || cls == short[].class || cls == int[].class || cls == long[].class ||
            cls == float[].class || cls == double[].class || cls == char[].class || cls == boolean[].class ||
            cls == String[].class || cls == UUID[].class || cls == Date[].class || cls == Timestamp[].class ||
            cls == BigDecimal[].class;
    }

    /**
     * Attempts to create a new collection of the same known type. Will return null if collection type is unknown.
     *
     * @param col Collection.
     * @return New empty collection.
     */
    @SuppressWarnings("unchecked")
    public static <V> Collection<V> newKnownCollection(Object col) {
        Class<?> cls = col == null ? null : col.getClass();

        if (cls == HashSet.class)
            return U.newHashSet(((Collection)col).size());
        else if (cls == LinkedHashSet.class)
            return U.newLinkedHashSet(((Collection)col).size());
        else if (!wrapTrees() && cls == TreeSet.class)
            return new TreeSet<>(((TreeSet<Object>)col).comparator());
        else if (cls == ConcurrentSkipListSet.class)
            return new ConcurrentSkipListSet<>(((ConcurrentSkipListSet<Object>)col).comparator());
        else if (cls == ArrayList.class)
            return new ArrayList<>(((Collection)col).size());
        else if (cls == LinkedList.class)
            return new LinkedList<>();

        return null;
    }

    /**
     * Attempts to create a new set of the same type as {@code set} has. Otherwise returns new {@code HashSet}
     * instance.
     *
     * @param set Original set.
     * @return New set.
     */
    public static <V> Set<V> newSet(Set<V> set) {
        if (set instanceof LinkedHashSet)
            return U.newLinkedHashSet(set.size());
        else if (set instanceof TreeSet)
            return new TreeSet<>(((TreeSet<Object>)set).comparator());
        else if (set instanceof ConcurrentSkipListSet)
            return new ConcurrentSkipListSet<>(((ConcurrentSkipListSet<Object>)set).comparator());

        return U.newHashSet(set.size());
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

    /**
     * Get footer start of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Footer start.
     */
    public static int footerStartRelative(BinaryPositionReadable in, int start) {
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
    public static int rawOffsetRelative(BinaryPositionReadable in, int start) {
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
        assert newMeta != null;

        if (oldMeta == null)
            return newMeta;
        else {
            assert oldMeta.typeId() == newMeta.typeId();

            // Check type name.
            if (!F.eq(oldMeta.typeName(), newMeta.typeName())) {
                throw new BinaryObjectException(
                    "Two binary types have duplicate type ID [" + "typeId=" + oldMeta.typeId() +
                        ", typeName1=" + oldMeta.typeName() + ", typeName2=" + newMeta.typeName() + ']'
                );
            }

            // Check affinity field names.
            if (!F.eq(oldMeta.affinityKeyFieldName(), newMeta.affinityKeyFieldName())) {
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
            Map<String, Integer> mergedFields;

            if (FIELDS_SORTED_ORDER)
                mergedFields = new TreeMap<>(oldMeta.fieldsMap());
            else
                mergedFields = new LinkedHashMap<>(oldMeta.fieldsMap());

            Map<String, Integer> newFields = newMeta.fieldsMap();

            boolean changed = false;

            for (Map.Entry<String, Integer> newField : newFields.entrySet()) {
                Integer oldFieldType = mergedFields.put(newField.getKey(), newField.getValue());

                if (oldFieldType == null)
                    changed = true;
                else {
                    String oldFieldTypeName = fieldTypeName(oldFieldType);
                    String newFieldTypeName = fieldTypeName(newField.getValue());

                    if (!F.eq(oldFieldTypeName, newFieldTypeName)) {
                        throw new BinaryObjectException(
                            "Binary type has different field types [" + "typeName=" + oldMeta.typeName() +
                                ", fieldName=" + newField.getKey() +
                                ", fieldTypeName1=" + oldFieldTypeName +
                                ", fieldTypeName2=" + newFieldTypeName + ']'
                        );
                    }
                }
            }

            // Check and merge schemas.
            Collection<BinarySchema> mergedSchemas = new HashSet<>(oldMeta.schemas());

            for (BinarySchema newSchema : newMeta.schemas()) {
                if (mergedSchemas.add(newSchema))
                    changed = true;
            }

            // Return either old meta if no changes detected, or new merged meta.
            return changed ? new BinaryMetadata(oldMeta.typeId(), oldMeta.typeName(), mergedFields,
                oldMeta.affinityKeyFieldName(), mergedSchemas, oldMeta.isEnum()) : oldMeta;
        }
    }

    /**
     * @param cls Class.
     * @return Mode.
     */
    @SuppressWarnings("IfMayBeConditional")
    public static BinaryWriteMode mode(Class<?> cls) {
        assert cls != null;

        /** Primitives. */
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

        /** Boxed primitives. */
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

        /** The rest types. */
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
        else if (cls.isArray())
            return cls.getComponentType().isEnum() ? BinaryWriteMode.ENUM_ARR : BinaryWriteMode.OBJECT_ARR;
        else if (cls == BinaryObjectImpl.class)
            return BinaryWriteMode.BINARY_OBJ;
        else if (Binarylizable.class.isAssignableFrom(cls))
            return BinaryWriteMode.BINARY;
        else if (isSpecialCollection(cls))
            return BinaryWriteMode.COL;
        else if (isSpecialMap(cls))
            return BinaryWriteMode.MAP;
        else if (cls.isEnum())
            return BinaryWriteMode.ENUM;
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
        return ArrayList.class.equals(cls) || LinkedList.class.equals(cls) ||
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
    public static byte[] doReadByteArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readByteArray(len);
    }

    /**
     * @return Value.
     */
    public static boolean[] doReadBooleanArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readBooleanArray(len);
    }

    /**
     * @return Value.
     */
    public static short[] doReadShortArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readShortArray(len);
    }

    /**
     * @return Value.
     */
    public static char[] doReadCharArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readCharArray(len);
    }

    /**
     * @return Value.
     */
    public static int[] doReadIntArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readIntArray(len);
    }

    /**
     * @return Value.
     */
    public static long[] doReadLongArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readLongArray(len);
    }

    /**
     * @return Value.
     */
    public static float[] doReadFloatArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readFloatArray(len);
    }

    /**
     * @return Value.
     */
    public static double[] doReadDoubleArray(BinaryInputStream in) {
        int len = in.readInt();

        return in.readDoubleArray(len);
    }

    /**
     * @return Value.
     */
    public static BigDecimal doReadDecimal(BinaryInputStream in) {
        int scale = in.readInt();
        byte[] mag = doReadByteArray(in);

        BigInteger intVal = new BigInteger(mag);

        if (scale < 0) {
            scale &= 0x7FFFFFFF;

            intVal = intVal.negate();
        }

        return new BigDecimal(intVal, scale);
    }

    /**
     * @return Value.
     */
    public static String doReadString(BinaryInputStream in) {
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
    public static UUID doReadUuid(BinaryInputStream in) {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * @return Value.
     */
    public static Date doReadDate(BinaryInputStream in) {
        long time = in.readLong();

        return new Date(time);
    }

    /**
     * @return Value.
     */
    public static Timestamp doReadTimestamp(BinaryInputStream in) {
        long time = in.readLong();
        int nanos = in.readInt();

        Timestamp ts = new Timestamp(time);

        ts.setNanos(ts.getNanos() + nanos);

        return ts;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    public static BigDecimal[] doReadDecimalArray(BinaryInputStream in) throws BinaryObjectException {
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
    public static UUID[] doReadUuidArray(BinaryInputStream in) throws BinaryObjectException {
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
    public static Date[] doReadDateArray(BinaryInputStream in) throws BinaryObjectException {
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
    public static Timestamp[] doReadTimestampArray(BinaryInputStream in) throws BinaryObjectException {
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
     */
    public static BinaryObject doReadBinaryObject(BinaryInputStream in, BinaryContext ctx) {
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

            return new BinaryObjectImpl(ctx, arr, start);
        }
    }

    /**
     * @return Value.
     */
    public static Class doReadClass(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr)
        throws BinaryObjectException {
        int typeId = in.readInt();

        return doReadClass(in, ctx, ldr, typeId);
    }

    /**
     * @return Value.
     */
    @SuppressWarnings("ConstantConditions")
    public static Object doReadProxy(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
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
    public static String doReadClassName(BinaryInputStream in) {
        byte flag = in.readByte();

        if (flag != GridBinaryMarshaller.STRING)
            throw new BinaryObjectException("Failed to read class name [position=" + (in.position() - 1) + ']');

        return doReadString(in);
    }

    /**
     * @param typeId Type id.
     * @return Value.
     */
    public static Class doReadClass(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr, int typeId)
        throws BinaryObjectException {
        Class cls;

        if (typeId == GridBinaryMarshaller.OBJECT_TYPE_ID)
            return Object.class;

        if (typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            cls = ctx.descriptorForTypeId(true, typeId, ldr, false).describedClass();
        else {
            String clsName = doReadClassName(in);

            try {
                cls = U.forName(clsName, ldr);
            }
            catch (ClassNotFoundException e) {
                throw new BinaryInvalidTypeException("Failed to load the class: " + clsName, e);
            }

            // forces registering of class by type id, at least locally
            ctx.descriptorForClass(cls, true);
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
    public static Class resolveClass(BinaryContext ctx, int typeId, @Nullable String clsName,
        @Nullable ClassLoader ldr, boolean deserialize) {
        Class cls;

        if (typeId == GridBinaryMarshaller.OBJECT_TYPE_ID)
            return Object.class;

        if (typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            cls = ctx.descriptorForTypeId(true, typeId, ldr, deserialize).describedClass();
        else {
            try {
                cls = U.forName(clsName, ldr);
            }
            catch (ClassNotFoundException e) {
                throw new BinaryInvalidTypeException("Failed to load the class: " + clsName, e);
            }

            // forces registering of class by type id, at least locally
            ctx.descriptorForClass(cls, true);
        }

        return cls;
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
     * Read binary enum array.
     *
     * @param in Input stream.
     * @param ctx Binary context.
     * @return Enum array.
     */
    private static Object[] doReadBinaryEnumArray(BinaryInputStream in, BinaryContext ctx) {
        int len = in.readInt();

        Object[] arr = (Object[])Array.newInstance(BinaryObject.class, len);

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
     * @return Value.
     */
    public static Enum<?> doReadEnum(BinaryInputStream in, Class<?> cls) throws BinaryObjectException {
        assert cls != null;

        if (!cls.isEnum())
            throw new BinaryObjectException("Class does not represent enum type: " + cls.getName());

        int ord = in.readInt();

        return BinaryEnumCache.get(cls, ord);
    }

    /**
     * @param cls Enum class.
     * @return Value.
     */
    public static Object[] doReadEnumArray(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr, Class<?> cls)
        throws BinaryObjectException {
        int len = in.readInt();

        Object[] arr = (Object[])Array.newInstance(cls, len);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else
                arr[i] = doReadEnum(in, doReadClass(in, ctx, ldr));
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
            return ctx.optimizedMarsh().unmarshal(input, U.resolveClassLoader(clsLdr, ctx.configuration()));
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
    @Nullable public static Object doReadObject(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles) throws BinaryObjectException {
        return new BinaryReaderExImpl(ctx, in, ldr, handles.handles(), true).deserialize();
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public static Object unmarshal(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr)
        throws BinaryObjectException {
        return unmarshal(in, ctx, ldr, new BinaryReaderHandlesHolderImpl());
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public static Object unmarshal(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles) throws BinaryObjectException {
        return unmarshal(in, ctx, ldr, handles, false);
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public static Object unmarshal(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean detach) throws BinaryObjectException {
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

                    obj = unmarshal(in, ctx, ldr, handles);

                    in.position(retPos);
                }

                return obj;
            }

            case GridBinaryMarshaller.OBJ: {
                checkProtocolVersion(in.readByte());

                int len = length(in, start);

                BinaryObjectExImpl po;

                if (detach) {
                    // In detach mode we simply copy object's content.
                    in.position(start);

                    po = new BinaryObjectImpl(ctx, in.readByteArray(len), 0);
                }
                else {
                    if (in.offheapPointer() == 0)
                        po = new BinaryObjectImpl(ctx, in.array(), start);
                    else
                        po = new BinaryObjectOffheapImpl(ctx, in.offheapPointer(), start,
                            in.remaining() + in.position());

                    in.position(start + po.length());
                }

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

            case GridBinaryMarshaller.OBJ_ARR:
                return doReadObjectArray(in, ctx, ldr, handles, false);

            case GridBinaryMarshaller.COL:
                return doReadCollection(in, ctx, ldr, handles, false, null);

            case GridBinaryMarshaller.MAP:
                return doReadMap(in, ctx, ldr, handles, false, null);

            case GridBinaryMarshaller.BINARY_OBJ:
                return doReadBinaryObject(in, ctx);

            case GridBinaryMarshaller.ENUM:
                return doReadBinaryEnum(in, ctx, doReadEnumType(in));

            case GridBinaryMarshaller.ENUM_ARR:
                doReadEnumType(in); // Simply skip this part as we do not need it.

                return doReadBinaryEnumArray(in, ctx);

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
     * @param deserialize Deep flag.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    public static Object[] doReadObjectArray(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean deserialize) throws BinaryObjectException {
        int hPos = positionForHandle(in);

        Class compType = doReadClass(in, ctx, ldr);

        int len = in.readInt();

        Object[] arr = deserialize ? (Object[])Array.newInstance(compType, len) : new Object[len];

        handles.setHandle(arr, hPos);

        for (int i = 0; i < len; i++)
            arr[i] = deserializeOrUnmarshal(in, ctx, ldr, handles, deserialize);

        return arr;
    }

    /**
     * @param deserialize Deep flag.
     * @param factory Collection factory.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    public static Collection<?> doReadCollection(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean deserialize, BinaryCollectionFactory factory)
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
            col.add(deserializeOrUnmarshal(in, ctx, ldr, handles, deserialize));

        return col;
    }

    /**
     * @param deserialize Deep flag.
     * @param factory Map factory.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    public static Map<?, ?> doReadMap(BinaryInputStream in, BinaryContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean deserialize, BinaryMapFactory factory)
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
            Object key = deserializeOrUnmarshal(in, ctx, ldr, handles, deserialize);
            Object val = deserializeOrUnmarshal(in, ctx, ldr, handles, deserialize);

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
        BinaryReaderHandlesHolder handles, boolean deserialize) {
        return deserialize ? doReadObject(in, ctx, ldr, handles) : unmarshal(in, ctx, ldr, handles);
    }

    /**
     * Get position to be used for handle. We assume here that the hdr byte was read, hence subtract -1.
     *
     * @return Position for handle.
     */
    public static int positionForHandle(BinaryInputStream in) {
        return in.position() - 1;
    }

    /**
     * Check if class is binarylizable.
     *
     * @param cls Class.
     * @return {@code True} if binarylizable.
     */
    public static boolean isBinarylizable(Class cls) {
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
    public static boolean isCustomJavaSerialization(Class cls) {
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
    public static String qualifiedFieldName(Class cls, String fieldName) {
        return cls.getName() + "." + fieldName;
    }

    /**
     * Write {@code IgniteUuid} instance.
     *
     * @param out Writer.
     * @param val Value.
     */
    public static void writeIgniteUuid(BinaryRawWriter out, @Nullable IgniteUuid val) {
        if (val != null) {
            out.writeBoolean(true);

            out.writeLong(val.globalId().getMostSignificantBits());
            out.writeLong(val.globalId().getLeastSignificantBits());
            out.writeLong(val.localId());
        }
        else
            out.writeBoolean(false);
    }

    /**
     * Read {@code IgniteUuid} instance.
     *
     * @param in Reader.
     * @return Value.
     */
    @Nullable public static IgniteUuid readIgniteUuid(BinaryRawReader in) {
        if (in.readBoolean()) {
            UUID globalId = new UUID(in.readLong(), in.readLong());

            return new IgniteUuid(globalId, in.readLong());
        }
        else
            return null;
    }

    /**
     * Reconstructs string from UTF-8 bytes.
     *
     * @param arr array Byte array.
     * @param off offset Offset in the array.
     * @param len length Byte array lenght.
     * @return string Resulting string.
     */
    public static String utf8BytesToStr(byte[] arr, int off, int len) {
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
    public static byte[] strToUtf8Bytes(String val) {
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
                arr[position++] = (byte)(0x80 | (c  & 0x3F));
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
    public static BinaryType typeProxy(BinaryContext ctx, BinaryObjectEx obj) {
        if (ctx == null)
            throw new BinaryObjectException("BinaryContext is not set for the object.");

        String clsName = obj instanceof BinaryEnumObjectImpl ? ((BinaryEnumObjectImpl)obj).className() : null;

        return new BinaryTypeProxy(ctx, obj.typeId(), clsName);
    }

    /**
     * Create binary type which is used by users.
     *
     * @param ctx Context.
     * @param obj Binary object.
     * @return Binary type.
     */
    public static BinaryType type(BinaryContext ctx, BinaryObjectEx obj) {
        if (ctx == null)
            throw new BinaryObjectException("BinaryContext is not set for the object.");

        return ctx.metadata(obj.typeId());
    }

    /**
     * Get predefined write-replacer associated with class.
     *
     * @param cls Class.
     * @return Write replacer.
     */
    public static BinaryWriteReplacer writeReplacer(Class cls) {
        return cls != null ? CLS_TO_WRITE_REPLACER.get(cls) : null;
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
