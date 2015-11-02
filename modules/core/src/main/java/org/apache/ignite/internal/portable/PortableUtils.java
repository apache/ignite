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

import org.apache.ignite.internal.portable.builder.PortableLazyValue;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableObject;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR_ARR;
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
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.MAP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.NULL;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.OBJ_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.PROTO_VER;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.STRING_ARR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.TIMESTAMP_ARR;
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
    public static final short FLAG_USR_TYP = 0x1;

    /** Flag: only raw data exists. */
    public static final short FLAG_RAW_ONLY = 0x2;

    /** Flag: offsets take 1 byte. */
    public static final short FLAG_OFFSET_ONE_BYTE = 0x4;

    /** Flag: offsets take 2 bytes. */
    public static final short FLAG_OFFSET_TWO_BYTES = 0x8;

    /** Offset which fits into 1 byte. */
    public static final int OFFSET_1 = 1;

    /** Offset which fits into 2 bytes. */
    public static final int OFFSET_2 = 2;

    /** Offset which fits into 4 bytes. */
    public static final int OFFSET_4 = 4;

    /**
     * Write flags.
     *
     * @param writer Writer.
     * @param userType User type flag.
     */
    public static void writeFlags(PortableWriterExImpl writer, boolean userType) {
        short val = 0;

        if (userType)
            val |= FLAG_USR_TYP;

        writer.doWriteShort(val);
    }

    /**
     * Check if user type flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    public static boolean isUserType(short flags) {
        return (flags & FLAG_USR_TYP) == FLAG_USR_TYP;
    }

    /**
     * Check if raw-only flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    public static boolean isRawOnly(short flags) {
        return (flags & FLAG_RAW_ONLY) == FLAG_RAW_ONLY;
    }

    /**
     *
     */
    static {
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
    }

    /**
     *
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
    }

    /**
     * Write value with flag. e.g. writePlainObject(writer, (byte)77) will write two byte: {BYTE, 77}.
     *
     * @param writer W
     * @param val Value.
     */
    public static void writePlainObject(PortableWriterExImpl writer, Object val) {
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

        if (Collection.class.isAssignableFrom(cls))
            return COL;

        if (Map.class.isAssignableFrom(cls))
            return MAP;

        if (Map.Entry.class.isAssignableFrom(cls))
            return MAP;

        return OBJ;
    }

    /**
     * Tells whether provided type is portable or a collection.
     *
     * @param cls Class to check.
     * @return Whether type is portable or a collection.
     */
    public static boolean isPortableOrCollectionType(Class<?> cls) {
        assert cls != null;

        return isPortableType(cls) ||
            cls == Object[].class ||
            Collection.class.isAssignableFrom(cls) ||
            Map.class.isAssignableFrom(cls) ||
            Map.Entry.class.isAssignableFrom(cls);
    }

    /**
     * Tells whether provided type is portable.
     *
     * @param cls Class to check.
     * @return Whether type is portable.
     */
    public static boolean isPortableType(Class<?> cls) {
        assert cls != null;

        return PortableObject.class.isAssignableFrom(cls) ||
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
            throw new PortableException("Unsupported protocol version: " + protoVer);
    }

    /**
     * Write portable header.
     *
     * @param writer Writer.
     * @param usrTyp User type flag.
     * @param typeId Type ID.
     * @param hashCode Hash code.
     * @param clsName Class name (optional).
     * @return Position where length should be written.
     */
    public static int writeHeader(PortableWriterExImpl writer, boolean usrTyp, int typeId, int hashCode,
        @Nullable String clsName) {
        writer.doWriteByte(GridPortableMarshaller.OBJ);
        writer.doWriteByte(GridPortableMarshaller.PROTO_VER);

        PortableUtils.writeFlags(writer, usrTyp);

        writer.doWriteInt(typeId);
        writer.doWriteInt(hashCode);

        int reserved = writer.reserve(12);

        if (clsName != null)
            writer.doWriteString(clsName);

        return reserved;
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

        if (PortableUtils.isRawOnly(flags))
            // No schema, footer start equals to object end.
            return length(in, start);
        else
            // Schema exists, use offset.
            return in.readIntPositioned(start + GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS);
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
        int footerStart = footerStartRelative(in, start);
        int footerEnd = length(in, start);

        // Take in count possible raw offset.
        if ((((footerEnd - footerStart) >> 2) & 0x1) == 0x1)
            footerEnd -= 4;

        return F.t(start + footerStart, start + footerEnd);
    }

    /**
     * Get raw offset of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Raw offset.
     */
    public static int rawOffsetAbsolute(PortablePositionReadable in, int start) {
        int len = length(in, start);

        short flags = in.readShortPositioned(start + GridPortableMarshaller.FLAGS_POS);

        if (PortableUtils.isRawOnly(flags))
            // No schema, raw offset is located on schema offset position.
            return start + in.readIntPositioned(start + GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS);
        else {
            // Schema exists.
            int schemaOff = in.readIntPositioned(start + GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS);

            if ((((len - schemaOff) >> 2) & 0x1) == 0x0)
                // Even amount of records in schema => no raw offset.
                return start + schemaOff;
            else
                // Odd amount of records in schema => raw offset is the very last 4 bytes in object.
                return start + in.readIntPositioned(start + len - 4);
        }
    }

    /**
     * Get offset size for the given flags.
     * @param flags Flags.
     * @return Offset size.
     */
    public static int fieldOffsetSize(short flags) {
        if ((flags & FLAG_OFFSET_ONE_BYTE) == FLAG_OFFSET_ONE_BYTE)
            return OFFSET_1;
        else if ((flags & FLAG_OFFSET_TWO_BYTES) == FLAG_OFFSET_TWO_BYTES)
            return OFFSET_2;
        else
            return OFFSET_4;
    }
}