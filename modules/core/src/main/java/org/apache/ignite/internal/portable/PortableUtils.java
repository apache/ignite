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

package org.apache.ignite.internal.portable;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryCollectionFactory;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryMapFactory;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.portable.builder.PortableLazyValue;
import org.apache.ignite.internal.portable.streams.PortableInputStream;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.lang.reflect.Array;
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ARR_LIST;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CLASS;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.COL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DATE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DATE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DECIMAL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DECIMAL_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DOUBLE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DOUBLE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ENUM;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.ENUM_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLOAT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLOAT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.HANDLE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.HASH_MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.HASH_SET;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LINKED_HASH_MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LINKED_HASH_SET;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LINKED_LIST;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.NULL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJECT_TYPE_ID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OPTM_MARSH;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PORTABLE_OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PROTO_VER;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UNREGISTERED_TYPE_ID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.USER_COL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.USER_SET;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.UUID_ARR;

/**
 * Portable utils.
 */
public class PortableUtils {
    /** */
    public static final Map<Class<?>, Byte> PLAIN_CLASS_TO_FLAG = new HashMap<>();

    /** */
    public static final Map<Byte, Class<?>> FLAG_TO_CLASS = new HashMap<>();

    /** {@code true} if serialized value of this type cannot contain references to objects. */
    private static final boolean[] PLAIN_TYPE_FLAG = new boolean[102];

    /** Portable classes. */
    private static final Collection<Class<?>> PORTABLE_CLS = new HashSet<>();

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

    /** Offset which fits into 1 byte. */
    public static final int OFFSET_1 = 1;

    /** Offset which fits into 2 bytes. */
    public static final int OFFSET_2 = 2;

    /** Offset which fits into 4 bytes. */
    public static final int OFFSET_4 = 4;

    /** Field ID length. */
    public static final int FIELD_ID_LEN = 4;

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
        PLAIN_CLASS_TO_FLAG.put(Byte.class, GridPortableMarshaller.BYTE);
        PLAIN_CLASS_TO_FLAG.put(Short.class, GridPortableMarshaller.SHORT);
        PLAIN_CLASS_TO_FLAG.put(Integer.class, GridPortableMarshaller.INT);
        PLAIN_CLASS_TO_FLAG.put(Long.class, GridPortableMarshaller.LONG);
        PLAIN_CLASS_TO_FLAG.put(Float.class, GridPortableMarshaller.FLOAT);
        PLAIN_CLASS_TO_FLAG.put(Double.class, GridPortableMarshaller.DOUBLE);
        PLAIN_CLASS_TO_FLAG.put(Character.class, GridPortableMarshaller.CHAR);
        PLAIN_CLASS_TO_FLAG.put(Boolean.class, GridPortableMarshaller.BOOLEAN);
        PLAIN_CLASS_TO_FLAG.put(BigDecimal.class, GridPortableMarshaller.DECIMAL);
        PLAIN_CLASS_TO_FLAG.put(String.class, GridPortableMarshaller.STRING);
        PLAIN_CLASS_TO_FLAG.put(UUID.class, GridPortableMarshaller.UUID);
        PLAIN_CLASS_TO_FLAG.put(Date.class, GridPortableMarshaller.DATE);
        PLAIN_CLASS_TO_FLAG.put(Timestamp.class, GridPortableMarshaller.TIMESTAMP);

        PLAIN_CLASS_TO_FLAG.put(byte[].class, GridPortableMarshaller.BYTE_ARR);
        PLAIN_CLASS_TO_FLAG.put(short[].class, GridPortableMarshaller.SHORT_ARR);
        PLAIN_CLASS_TO_FLAG.put(int[].class, GridPortableMarshaller.INT_ARR);
        PLAIN_CLASS_TO_FLAG.put(long[].class, GridPortableMarshaller.LONG_ARR);
        PLAIN_CLASS_TO_FLAG.put(float[].class, GridPortableMarshaller.FLOAT_ARR);
        PLAIN_CLASS_TO_FLAG.put(double[].class, GridPortableMarshaller.DOUBLE_ARR);
        PLAIN_CLASS_TO_FLAG.put(char[].class, GridPortableMarshaller.CHAR_ARR);
        PLAIN_CLASS_TO_FLAG.put(boolean[].class, GridPortableMarshaller.BOOLEAN_ARR);
        PLAIN_CLASS_TO_FLAG.put(BigDecimal[].class, GridPortableMarshaller.DECIMAL_ARR);
        PLAIN_CLASS_TO_FLAG.put(String[].class, GridPortableMarshaller.STRING_ARR);
        PLAIN_CLASS_TO_FLAG.put(UUID[].class, GridPortableMarshaller.UUID_ARR);
        PLAIN_CLASS_TO_FLAG.put(Date[].class, GridPortableMarshaller.DATE_ARR);
        PLAIN_CLASS_TO_FLAG.put(Timestamp[].class, GridPortableMarshaller.TIMESTAMP_ARR);

        for (Map.Entry<Class<?>, Byte> entry : PLAIN_CLASS_TO_FLAG.entrySet())
            FLAG_TO_CLASS.put(entry.getValue(), entry.getKey());

        PLAIN_CLASS_TO_FLAG.put(byte.class, GridPortableMarshaller.BYTE);
        PLAIN_CLASS_TO_FLAG.put(short.class, GridPortableMarshaller.SHORT);
        PLAIN_CLASS_TO_FLAG.put(int.class, GridPortableMarshaller.INT);
        PLAIN_CLASS_TO_FLAG.put(long.class, GridPortableMarshaller.LONG);
        PLAIN_CLASS_TO_FLAG.put(float.class, GridPortableMarshaller.FLOAT);
        PLAIN_CLASS_TO_FLAG.put(double.class, GridPortableMarshaller.DOUBLE);
        PLAIN_CLASS_TO_FLAG.put(char.class, GridPortableMarshaller.CHAR);
        PLAIN_CLASS_TO_FLAG.put(boolean.class, GridPortableMarshaller.BOOLEAN);

        for (byte b : new byte[] {
            BYTE, SHORT, INT, LONG, FLOAT, DOUBLE,
            CHAR, BOOLEAN, DECIMAL, STRING, UUID, DATE, TIMESTAMP,
            BYTE_ARR, SHORT_ARR, INT_ARR, LONG_ARR, FLOAT_ARR, DOUBLE_ARR,
            CHAR_ARR, BOOLEAN_ARR, DECIMAL_ARR, STRING_ARR, UUID_ARR, DATE_ARR, TIMESTAMP_ARR,
            ENUM, ENUM_ARR, NULL}) {

            PLAIN_TYPE_FLAG[b] = true;
        }

        PORTABLE_CLS.add(Byte.class);
        PORTABLE_CLS.add(Short.class);
        PORTABLE_CLS.add(Integer.class);
        PORTABLE_CLS.add(Long.class);
        PORTABLE_CLS.add(Float.class);
        PORTABLE_CLS.add(Double.class);
        PORTABLE_CLS.add(Character.class);
        PORTABLE_CLS.add(Boolean.class);
        PORTABLE_CLS.add(String.class);
        PORTABLE_CLS.add(UUID.class);
        PORTABLE_CLS.add(Date.class);
        PORTABLE_CLS.add(Timestamp.class);
        PORTABLE_CLS.add(BigDecimal.class);
        PORTABLE_CLS.add(byte[].class);
        PORTABLE_CLS.add(short[].class);
        PORTABLE_CLS.add(int[].class);
        PORTABLE_CLS.add(long[].class);
        PORTABLE_CLS.add(float[].class);
        PORTABLE_CLS.add(double[].class);
        PORTABLE_CLS.add(char[].class);
        PORTABLE_CLS.add(boolean[].class);
        PORTABLE_CLS.add(String[].class);
        PORTABLE_CLS.add(UUID[].class);
        PORTABLE_CLS.add(Date[].class);
        PORTABLE_CLS.add(Timestamp[].class);
        PORTABLE_CLS.add(BigDecimal[].class);

        FIELD_TYPE_NAMES = new String[104];

        FIELD_TYPE_NAMES[BYTE] = "byte";
        FIELD_TYPE_NAMES[SHORT] = "short";
        FIELD_TYPE_NAMES[INT] = "int";
        FIELD_TYPE_NAMES[LONG] = "long";
        FIELD_TYPE_NAMES[BOOLEAN] = "boolean";
        FIELD_TYPE_NAMES[FLOAT] = "float";
        FIELD_TYPE_NAMES[DOUBLE] = "double";
        FIELD_TYPE_NAMES[CHAR] = "char";
        FIELD_TYPE_NAMES[UUID] = "UUID";
        FIELD_TYPE_NAMES[DECIMAL] = "decimal";
        FIELD_TYPE_NAMES[STRING] = "String";
        FIELD_TYPE_NAMES[DATE] = "Date";
        FIELD_TYPE_NAMES[TIMESTAMP] = "Timestamp";
        FIELD_TYPE_NAMES[ENUM] = "Enum";
        FIELD_TYPE_NAMES[OBJ] = "Object";
        FIELD_TYPE_NAMES[PORTABLE_OBJ] = "Object";
        FIELD_TYPE_NAMES[COL] = "Collection";
        FIELD_TYPE_NAMES[MAP] = "Map";
        FIELD_TYPE_NAMES[CLASS] = "Class";
        FIELD_TYPE_NAMES[BYTE_ARR] = "byte[]";
        FIELD_TYPE_NAMES[SHORT_ARR] = "short[]";
        FIELD_TYPE_NAMES[INT_ARR] = "int[]";
        FIELD_TYPE_NAMES[LONG_ARR] = "long[]";
        FIELD_TYPE_NAMES[BOOLEAN_ARR] = "boolean[]";
        FIELD_TYPE_NAMES[FLOAT_ARR] = "float[]";
        FIELD_TYPE_NAMES[DOUBLE_ARR] = "double[]";
        FIELD_TYPE_NAMES[CHAR_ARR] = "char[]";
        FIELD_TYPE_NAMES[UUID_ARR] = "UUID[]";
        FIELD_TYPE_NAMES[DECIMAL_ARR] = "decimal[]";
        FIELD_TYPE_NAMES[STRING_ARR] = "String[]";
        FIELD_TYPE_NAMES[DATE_ARR] = "Date[]";
        FIELD_TYPE_NAMES[TIMESTAMP_ARR] = "Timestamp[]";
        FIELD_TYPE_NAMES[OBJ_ARR] = "Object[]";
        FIELD_TYPE_NAMES[ENUM_ARR] = "Enum[]";
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
    private static boolean isFlagSet(short flags, short flag) {
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
     * @param typeName Field type name.
     * @return Field type ID;
     */
    @SuppressWarnings("StringEquality")
    public static int fieldTypeId(String typeName) {
        for (int i = 0; i < FIELD_TYPE_NAMES.length; i++) {
            String typeName0 = FIELD_TYPE_NAMES[i];

            if (typeName.equals(typeName0))
                return i;
        }

        throw new IllegalArgumentException("Invalid metadata type name: " + typeName);
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
            case BYTE:
                writer.writeByte(flag);
                writer.writeByte((Byte)val);

                break;

            case SHORT:
                writer.writeByte(flag);
                writer.writeShort((Short)val);

                break;

            case INT:
                writer.writeByte(flag);
                writer.writeInt((Integer)val);

                break;

            case LONG:
                writer.writeByte(flag);
                writer.writeLong((Long)val);

                break;

            case FLOAT:
                writer.writeByte(flag);
                writer.writeFloat((Float)val);

                break;

            case DOUBLE:
                writer.writeByte(flag);
                writer.writeDouble((Double)val);

                break;

            case CHAR:
                writer.writeByte(flag);
                writer.writeChar((Character)val);

                break;

            case BOOLEAN:
                writer.writeByte(flag);
                writer.writeBoolean((Boolean)val);

                break;

            case DECIMAL:
                writer.doWriteDecimal((BigDecimal)val);

                break;

            case STRING:
                writer.doWriteString((String)val);

                break;

            case UUID:
                writer.doWriteUuid((UUID)val);

                break;

            case DATE:
                writer.doWriteDate((Date)val);

                break;

            case TIMESTAMP:
                writer.doWriteTimestamp((Timestamp) val);

                break;

            case BYTE_ARR:
                writer.doWriteByteArray((byte[])val);

                break;

            case SHORT_ARR:
                writer.doWriteShortArray((short[])val);

                break;

            case INT_ARR:
                writer.doWriteIntArray((int[])val);

                break;

            case LONG_ARR:
                writer.doWriteLongArray((long[])val);

                break;

            case FLOAT_ARR:
                writer.doWriteFloatArray((float[])val);

                break;

            case DOUBLE_ARR:
                writer.doWriteDoubleArray((double[])val);

                break;

            case CHAR_ARR:
                writer.doWriteCharArray((char[])val);

                break;

            case BOOLEAN_ARR:
                writer.doWriteBooleanArray((boolean[])val);

                break;

            case DECIMAL_ARR:
                writer.doWriteDecimalArray((BigDecimal[])val);

                break;

            case STRING_ARR:
                writer.doWriteStringArray((String[])val);

                break;

            case UUID_ARR:
                writer.doWriteUuidArray((UUID[])val);

                break;

            case DATE_ARR:
                writer.doWriteDateArray((Date[])val);

                break;

            case TIMESTAMP_ARR:
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
        if (obj instanceof PortableLazyValue)
            return ((PortableLazyValue)obj).value();

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
        return (type >= BYTE_ARR && type <= DATE_ARR) || type == TIMESTAMP_ARR;
    }

    /**
     * @param cls Class.
     * @return Portable field type.
     */
    public static byte typeByClass(Class<?> cls) {
        Byte type = PLAIN_CLASS_TO_FLAG.get(cls);

        if (type != null)
            return type;

        if (cls.isEnum())
            return ENUM;

        if (cls.isArray())
            return cls.getComponentType().isEnum() || cls.getComponentType() == Enum.class ? ENUM_ARR : OBJ_ARR;

        if (isSpecialCollection(cls))
            return COL;

        if (isSpecialMap(cls))
            return MAP;

        return OBJ;
    }

    /**
     * Tells whether provided type is portable.
     *
     * @param cls Class to check.
     * @return Whether type is portable.
     */
    public static boolean isPortableType(Class<?> cls) {
        assert cls != null;

        return BinaryObject.class.isAssignableFrom(cls) ||
            PORTABLE_CLS.contains(cls) ||
            cls.isEnum() ||
            (cls.isArray() && cls.getComponentType().isEnum());
    }

    /**
     * Attempts to create a new map of the same type as {@code map} has. Otherwise returns new {@code HashMap} instance.
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
     * Attempts to create a new set of the same type as {@code set} has. Otherwise returns new {@code HashSet} instance.
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
        if (PROTO_VER != protoVer)
            throw new BinaryObjectException("Unsupported protocol version: " + protoVer);
    }

    /**
     * Get portable object length.
     *
     * @param in Input stream.
     * @param start Start position.
     * @return Length.
     */
    public static int length(PortablePositionReadable in, int start) {
        return in.readIntPositioned(start + GridPortableMarshaller.TOTAL_LEN_POS);
    }

    /**
     * Get footer start of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Footer start.
     */
    public static int footerStartRelative(PortablePositionReadable in, int start) {
        short flags = in.readShortPositioned(start + GridPortableMarshaller.FLAGS_POS);

        if (hasSchema(flags))
            // Schema exists, use offset.
            return in.readIntPositioned(start + GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS);
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
    public static int footerStartAbsolute(PortablePositionReadable in, int start) {
        return footerStartRelative(in, start) + start;
    }

    /**
     * Get object's footer.
     *
     * @param in Input stream.
     * @param start Start position.
     * @return Footer.
     */
    public static IgniteBiTuple<Integer, Integer> footerAbsolute(PortablePositionReadable in, int start) {
        short flags = in.readShortPositioned(start + GridPortableMarshaller.FLAGS_POS);

        int footerEnd = length(in, start);

        if (hasSchema(flags)) {
            // Schema exists.
            int footerStart = in.readIntPositioned(start + GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS);

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
    public static int rawOffsetRelative(PortablePositionReadable in, int start) {
        short flags = in.readShortPositioned(start + GridPortableMarshaller.FLAGS_POS);

        int len = length(in, start);

        if (hasSchema(flags)){
            // Schema exists.
            if (hasRaw(flags))
                // Raw offset is set, it is at the very end of the object.
                return in.readIntPositioned(start + len - 4);
            else
                // Raw offset is not set, so just return schema offset.
                return in.readIntPositioned(start + GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS);
        }
        else
            // No schema, raw offset is located on schema offset position.
            return in.readIntPositioned(start + GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS);
    }

    /**
     * Get absolute raw offset of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Raw offset.
     */
    public static int rawOffsetAbsolute(PortablePositionReadable in, int start) {
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
    public static int fieldOffsetRelative(PortablePositionReadable stream, int pos, int fieldOffsetSize) {
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
                    "Two portable types have duplicate type ID [" + "typeId=" + oldMeta.typeId() +
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
            boolean changed = false;

            Map<String, Integer> mergedFields = new HashMap<>(oldMeta.fieldsMap());
            Map<String, Integer> newFields = newMeta.fieldsMap();

            for (Map.Entry<String, Integer> newField : newFields.entrySet()) {
                Integer oldFieldType = mergedFields.put(newField.getKey(), newField.getValue());

                if (oldFieldType == null)
                    changed = true;
                else if (!F.eq(oldFieldType, newField.getValue())) {
                    throw new BinaryObjectException(
                        "Binary type has different field types [" + "typeName=" + oldMeta.typeName() +
                            ", fieldName=" + newField.getKey() +
                            ", fieldTypeName1=" + fieldTypeName(oldFieldType) +
                            ", fieldTypeName2=" + fieldTypeName(newField.getValue()) + ']'
                    );
                }
            }

            // Check and merge schemas.
            Collection<PortableSchema> mergedSchemas = new HashSet<>(oldMeta.schemas());

            for (PortableSchema newSchema : newMeta.schemas()) {
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
            return BinaryWriteMode.PORTABLE_OBJ;
        else if (Binarylizable.class.isAssignableFrom(cls))
            return BinaryWriteMode.PORTABLE;
        else if (Externalizable.class.isAssignableFrom(cls))
            return BinaryWriteMode.EXTERNALIZABLE;
        else if (isSpecialCollection(cls))
            return BinaryWriteMode.COL;
        else if (isSpecialMap(cls))
            return BinaryWriteMode.MAP;
        else if (cls.isEnum())
            return BinaryWriteMode.ENUM;
        else if (cls == Class.class)
            return BinaryWriteMode.CLASS;
        else
            return BinaryWriteMode.OBJECT;
    }

    /**
     * Check if class represents a collection which must be treated specially.
     *
     * @param cls Class.
     * @return {@code True} if this is a special collection class.
     */
    private static boolean isSpecialCollection(Class cls) {
        return ArrayList.class.equals(cls) || LinkedList.class.equals(cls) ||
            HashSet.class.equals(cls) || LinkedHashSet.class.equals(cls);
    }

    /**
     * Check if class represents a map which must be treated specially.
     *
     * @param cls Class.
     * @return {@code True} if this is a special map class.
     */
    private static boolean isSpecialMap(Class cls) {
        return HashMap.class.equals(cls) || LinkedHashMap.class.equals(cls);
    }

    /**
     * @return Value.
     */
    public static byte[] doReadByteArray(PortableInputStream in) {
        int len = in.readInt();

        return in.readByteArray(len);
    }

    /**
     * @return Value.
     */
    public static boolean[] doReadBooleanArray(PortableInputStream in) {
        int len = in.readInt();

        return in.readBooleanArray(len);
    }

    /**
     * @return Value.
     */
    public static short[] doReadShortArray(PortableInputStream in) {
        int len = in.readInt();

        return in.readShortArray(len);
    }

    /**
     * @return Value.
     */
    public static char[] doReadCharArray(PortableInputStream in) {
        int len = in.readInt();

        return in.readCharArray(len);
    }

    /**
     * @return Value.
     */
    public static int[] doReadIntArray(PortableInputStream in) {
        int len = in.readInt();

        return in.readIntArray(len);
    }

    /**
     * @return Value.
     */
    public static long[] doReadLongArray(PortableInputStream in) {
        int len = in.readInt();

        return in.readLongArray(len);
    }

    /**
     * @return Value.
     */
    public static float[] doReadFloatArray(PortableInputStream in) {
        int len = in.readInt();

        return in.readFloatArray(len);
    }

    /**
     * @return Value.
     */
    public static double[] doReadDoubleArray(PortableInputStream in) {
        int len = in.readInt();

        return in.readDoubleArray(len);
    }

    /**
     * @return Value.
     */
    public static BigDecimal doReadDecimal(PortableInputStream in) {
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
    public static String doReadString(PortableInputStream in) {
        if (!in.hasArray())
            return new String(doReadByteArray(in), UTF_8);

        int strLen = in.readInt();

        int pos = in.position();

        // String will copy necessary array part for us.
        String res = new String(in.array(), pos, strLen, UTF_8);

        in.position(pos + strLen);

        return res;
    }

    /**
     * @return Value.
     */
    public static UUID doReadUuid(PortableInputStream in) {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * @return Value.
     */
    public static Date doReadDate(PortableInputStream in) {
        long time = in.readLong();

        return new Date(time);
    }

    /**
     * @return Value.
     */
    public static Timestamp doReadTimestamp(PortableInputStream in) {
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
    public static BigDecimal[] doReadDecimalArray(PortableInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        BigDecimal[] arr = new BigDecimal[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != DECIMAL)
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
    public static String[] doReadStringArray(PortableInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        String[] arr = new String[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != STRING)
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
    public static UUID[] doReadUuidArray(PortableInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        UUID[] arr = new UUID[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != UUID)
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
    public static Date[] doReadDateArray(PortableInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        Date[] arr = new Date[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != DATE)
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
    public static Timestamp[] doReadTimestampArray(PortableInputStream in) throws BinaryObjectException {
        int len = in.readInt();

        Timestamp[] arr = new Timestamp[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else {
                if (flag != TIMESTAMP)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadTimestamp(in);
            }
        }

        return arr;
    }

    /**
     * @return Value.
     */
    public static BinaryObject doReadPortableObject(PortableInputStream in, PortableContext ctx) {
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
    public static Class doReadClass(PortableInputStream in, PortableContext ctx, ClassLoader ldr)
        throws BinaryObjectException {
        int typeId = in.readInt();

        return doReadClass(in, ctx, ldr, typeId);
    }

    /**
     * Read plain type.
     *
     * @param in Input stream.
     * @return Plain type.
     */
    private static EnumType doReadEnumType(PortableInputStream in) {
        int typeId = in.readInt();

        if (typeId != UNREGISTERED_TYPE_ID)
            return new EnumType(typeId, null);
        else {
            String clsName = doReadClassName(in);

            return new EnumType(UNREGISTERED_TYPE_ID, clsName);
        }
    }

    /**
     * @param in Input stream.
     * @return Class name.
     */
    private static String doReadClassName(PortableInputStream in) {
        byte flag = in.readByte();

        if (flag != STRING)
            throw new BinaryObjectException("Failed to read class name [position=" + (in.position() - 1) + ']');

        return doReadString(in);
    }

    /**
     * @param typeId Type id.
     * @return Value.
     */
    public static Class doReadClass(PortableInputStream in, PortableContext ctx, ClassLoader ldr, int typeId)
        throws BinaryObjectException {
        Class cls;

        if (typeId == OBJECT_TYPE_ID)
            return Object.class;

        if (typeId != UNREGISTERED_TYPE_ID)
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
     * @param ctx Portable context.
     * @param typeId Type ID.
     * @param clsName Class name.
     * @param ldr Class loaded.
     * @return Resovled class.
     */
    public static Class resolveClass(PortableContext ctx, int typeId, @Nullable String clsName,
        @Nullable ClassLoader ldr, boolean deserialize) {
        Class cls;

        if (typeId == OBJECT_TYPE_ID)
            return Object.class;

        if (typeId != UNREGISTERED_TYPE_ID)
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
     * Read portable enum.
     *
     * @param in Input stream.
     * @param ctx Portable context.
     * @param type Plain type.
     * @return Enum.
     */
    private static BinaryEnumObjectImpl doReadPortableEnum(PortableInputStream in, PortableContext ctx,
        EnumType type) {
        return new BinaryEnumObjectImpl(ctx, type.typeId, type.clsName, in.readInt());
    }

    /**
     * Read portable enum array.
     *
     * @param in Input stream.
     * @param ctx Portable context.
     * @return Enum array.
     */
    private static Object[] doReadPortableEnumArray(PortableInputStream in, PortableContext ctx) {
        int len = in.readInt();

        Object[] arr = (Object[]) Array.newInstance(BinaryObject.class, len);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
                arr[i] = null;
            else
                arr[i] = doReadPortableEnum(in, ctx, doReadEnumType(in));
        }

        return arr;
    }

    /**
     * Having target class in place we simply read ordinal and create final representation.
     *
     * @param cls Enum class.
     * @return Value.
     */
    public static Enum<?> doReadEnum(PortableInputStream in, Class<?> cls) throws BinaryObjectException {
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
    public static Object[] doReadEnumArray(PortableInputStream in, PortableContext ctx, ClassLoader ldr, Class<?> cls)
        throws BinaryObjectException {
        int len = in.readInt();

        Object[] arr = (Object[]) Array.newInstance(cls, len);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == NULL)
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
    public static Object doReadOptimized(PortableInputStream in, PortableContext ctx, @Nullable ClassLoader clsLdr) {
        int len = in.readInt();

        ByteArrayInputStream input = new ByteArrayInputStream(in.array(), in.position(), len);

        try {
            return ctx.optimizedMarsh().unmarshal(input, clsLdr);
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
    @Nullable public static Object doReadObject(PortableInputStream in, PortableContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles) throws BinaryObjectException {
        return new BinaryReaderExImpl(ctx, in, ldr, handles.handles()).deserialize();
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public static Object unmarshal(PortableInputStream in, PortableContext ctx, ClassLoader ldr)
        throws BinaryObjectException {
        return unmarshal(in, ctx, ldr, new BinaryReaderHandlesHolderImpl());
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public static Object unmarshal(PortableInputStream in, PortableContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles) throws BinaryObjectException {
        return unmarshal(in, ctx, ldr, handles, false);
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public static Object unmarshal(PortableInputStream in, PortableContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean detach) throws BinaryObjectException {
        int start = in.position();

        byte flag = in.readByte();

        switch (flag) {
            case NULL:
                return null;

            case HANDLE: {
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

            case OBJ: {
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

            case BYTE:
                return in.readByte();

            case SHORT:
                return in.readShort();

            case INT:
                return in.readInt();

            case LONG:
                return in.readLong();

            case FLOAT:
                return in.readFloat();

            case DOUBLE:
                return in.readDouble();

            case CHAR:
                return in.readChar();

            case BOOLEAN:
                return in.readBoolean();

            case DECIMAL:
                return doReadDecimal(in);

            case STRING:
                return doReadString(in);

            case UUID:
                return doReadUuid(in);

            case DATE:
                return doReadDate(in);

            case TIMESTAMP:
                return doReadTimestamp(in);

            case BYTE_ARR:
                return doReadByteArray(in);

            case SHORT_ARR:
                return doReadShortArray(in);

            case INT_ARR:
                return doReadIntArray(in);

            case LONG_ARR:
                return doReadLongArray(in);

            case FLOAT_ARR:
                return doReadFloatArray(in);

            case DOUBLE_ARR:
                return doReadDoubleArray(in);

            case CHAR_ARR:
                return doReadCharArray(in);

            case BOOLEAN_ARR:
                return doReadBooleanArray(in);

            case DECIMAL_ARR:
                return doReadDecimalArray(in);

            case STRING_ARR:
                return doReadStringArray(in);

            case UUID_ARR:
                return doReadUuidArray(in);

            case DATE_ARR:
                return doReadDateArray(in);

            case TIMESTAMP_ARR:
                return doReadTimestampArray(in);

            case OBJ_ARR:
                return doReadObjectArray(in, ctx, ldr, handles, false);

            case COL:
                return doReadCollection(in, ctx, ldr, handles, false, null);

            case MAP:
                return doReadMap(in, ctx, ldr, handles, false, null);

            case PORTABLE_OBJ:
                return doReadPortableObject(in, ctx);

            case ENUM:
                return doReadPortableEnum(in, ctx, doReadEnumType(in));

            case ENUM_ARR:
                doReadEnumType(in); // Simply skip this part as we do not need it.

                return doReadPortableEnumArray(in, ctx);

            case CLASS:
                return doReadClass(in, ctx, ldr);

            case OPTM_MARSH:
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
    public static Object[] doReadObjectArray(PortableInputStream in, PortableContext ctx, ClassLoader ldr,
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
    public static Collection<?> doReadCollection(PortableInputStream in, PortableContext ctx, ClassLoader ldr,
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
                case ARR_LIST:
                    col = new ArrayList<>(size);

                    break;

                case LINKED_LIST:
                    col = new LinkedList<>();

                    break;

                case HASH_SET:
                    col = U.newHashSet(size);

                    break;

                case LINKED_HASH_SET:
                    col = U.newLinkedHashSet(size);

                    break;

                case USER_SET:
                    col = U.newHashSet(size);

                    break;

                case USER_COL:
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
    public static Map<?, ?> doReadMap(PortableInputStream in, PortableContext ctx, ClassLoader ldr,
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
                case HASH_MAP:
                    map = U.newHashMap(size);

                    break;

                case LINKED_HASH_MAP:
                    map = U.newLinkedHashMap(size);

                    break;

                case USER_COL:
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
    private static Object deserializeOrUnmarshal(PortableInputStream in, PortableContext ctx, ClassLoader ldr,
        BinaryReaderHandlesHolder handles, boolean deserialize) {
        return deserialize ? doReadObject(in, ctx, ldr, handles) : unmarshal(in, ctx, ldr, handles);
    }

    /**
     * Get position to be used for handle. We assume here that the hdr byte was read, hence subtract -1.
     *
     * @return Position for handle.
     */
    public static int positionForHandle(PortableInputStream in) {
        return in.position() - 1;
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
            assert typeId != UNREGISTERED_TYPE_ID && clsName == null ||
                typeId == UNREGISTERED_TYPE_ID && clsName != null;

            this.typeId = typeId;
            this.clsName = clsName;
        }
    }
}