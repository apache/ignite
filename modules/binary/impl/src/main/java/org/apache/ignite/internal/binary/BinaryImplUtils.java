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
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryCollectionFactory;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryMapFactory;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.marshaller.ClassLoaderUtils;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.Marshallers;
import org.jetbrains.annotations.Nullable;

/**
 * Binary utils used only in implementation.
 */
public class BinaryImplUtils {
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

    /** {@code true} if serialized value of this type cannot contain references to objects. */
    private static final boolean[] PLAIN_TYPE_FLAG = new boolean[102];

    static {
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
     * @param val Value to check.
     * @return {@code True} if {@code val} instance of {@link BinaryEnumArray}.
     */
    public static boolean isBinaryEnumArray(Object val) {
        return val instanceof BinaryEnumArray;
    }

    /** */
    public static int hashCode(byte[] data, int startPos, int endPos) {
        int hash = 1;

        for (int i = startPos; i < endPos; i++)
            hash = 31 * hash + data[i];

        return hash;
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
        byte[] mag = BinaryUtils.doReadByteArray(in);

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

            return BinaryUtils.binariesFactory.binaryOffheapObject(ctx, in.offheapPointer() + pos, start, len);
        }
        else {
            byte[] arr = BinaryUtils.doReadByteArray(in);
            int start = in.readInt();

            BinaryObject binO = BinaryUtils.binariesFactory.binaryObject(ctx, arr, start);

            if (detach)
                return BinaryUtils.detach(binO);

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

        return Proxy.newProxyInstance(ldr != null ? ldr : CommonUtils.gridClassLoader(), intfs, ih);
    }

    /**
     * Read plain type.
     *
     * @param in Input stream.
     * @return Plain type.
     */
    static EnumType doReadEnumType(BinaryInputStream in) {
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

        return BinaryUtils.doReadString(in);
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

            try {
                cls = ClassLoaderUtils.forName(clsName, ldr);
            }
            catch (ClassNotFoundException e) {
                throw new BinaryInvalidTypeException("Failed to load the class: " + clsName, e);
            }

            // forces registering of class by type id, at least locally
            if (Marshallers.USE_CACHE.get())
                ctx.registerType(cls, false, false);
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
    static BinaryObjectEx doReadBinaryEnum(BinaryInputStream in, BinaryContext ctx,
        EnumType type) {
        return BinaryUtils.binariesFactory.binaryEnum(ctx, type.typeId, type.clsName, in.readInt());
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

        Object[] arr = (Object[])Array.newInstance(BinaryUtils.binariesFactory.binaryEnumClass(), len);

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
     * Read object serialized using optimized marshaller.
     *
     * @return Result.
     */
    public static Object doReadOptimized(BinaryInputStream in, BinaryContext ctx, @Nullable ClassLoader clsLdr) {
        int len = in.readInt();

        ByteArrayInputStream input = new ByteArrayInputStream(in.array(), in.position(), len);

        try {
            return ctx.optimizedMarsh().unmarshal(input, CommonUtils.resolveClassLoader(clsLdr, ctx.classLoader()));
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
        return new BinaryReaderExImpl(ctx, in, ldr, handles.handles(), false, true).deserialize();
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

                BinaryObjectEx po;

                if (detach) {
                    BinaryObjectEx binObj = BinaryUtils.binariesFactory.binaryObject(ctx, in.array(), start);

                    binObj.detachAllowed(true);

                    po = binObj.detach(!handles.isEmpty());
                }
                else {
                    if (in.offheapPointer() == 0)
                        po = BinaryUtils.binariesFactory.binaryObject(ctx, in.array(), start);
                    else
                        po = BinaryUtils.binariesFactory.binaryOffheapObject(ctx, in.offheapPointer(), start,
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
                return BinaryUtils.doReadString(in);

            case GridBinaryMarshaller.UUID:
                return doReadUuid(in);

            case GridBinaryMarshaller.DATE:
                return doReadDate(in);

            case GridBinaryMarshaller.TIMESTAMP:
                return doReadTimestamp(in);

            case GridBinaryMarshaller.TIME:
                return doReadTime(in);

            case GridBinaryMarshaller.BYTE_ARR:
                return BinaryUtils.doReadByteArray(in);

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
                return BinaryUtils.doReadStringArray(in);

            case GridBinaryMarshaller.UUID_ARR:
                return doReadUuidArray(in);

            case GridBinaryMarshaller.DATE_ARR:
                return doReadDateArray(in);

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                return doReadTimestampArray(in);

            case GridBinaryMarshaller.TIME_ARR:
                return doReadTimeArray(in);

            case GridBinaryMarshaller.OBJ_ARR:
                if (BinaryUtils.useBinaryArrays() && !deserialize)
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
                if (BinaryUtils.useBinaryArrays() && !deserialize)
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

            if (deserialize && BinaryUtils.useBinaryArrays() && res instanceof BinaryObject)
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
                    col = CommonUtils.newHashSet(size);

                    break;

                case GridBinaryMarshaller.LINKED_HASH_SET:
                    col = CommonUtils.newLinkedHashSet(size);

                    break;

                case GridBinaryMarshaller.USER_SET:
                    col = CommonUtils.newHashSet(size);

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

        return colType == GridBinaryMarshaller.SINGLETON_LIST ? CommonUtils.convertToSingletonList(col) : col;
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
                    map = CommonUtils.newHashMap(size);

                    break;

                case GridBinaryMarshaller.LINKED_HASH_MAP:
                    map = CommonUtils.newLinkedHashMap(size);

                    break;

                case GridBinaryMarshaller.USER_COL:
                    map = CommonUtils.newHashMap(size);

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
