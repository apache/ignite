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
import org.apache.ignite.binary.BinaryCollectionFactory;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryMapFactory;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.marshaller.ClassLoaderUtils;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.marshaller.Marshallers;
import org.jetbrains.annotations.Nullable;

/**
 * Reader that unmarshals a binary stream without parsing an object header.
 * Holds only the state the {@link #unmarshal()} family needs, so it can be allocated
 * cheaply on the paths that merely wrap bytes into a {@link org.apache.ignite.binary.BinaryObject}.
 * {@link BinaryReaderExImpl} extends it with full header, schema and field access.
 */
class SimpleBinaryReader {
    /** Binary context. */
    final BinaryContext ctx;

    /** Input stream. */
    final BinaryInputStream in;

    /** Class loader. */
    final ClassLoader ldr;

    /** Reader context which is constantly passed between objects. */
    BinaryReaderHandles hnds;

    /**
     * @param ctx Context.
     * @param in Input stream.
     * @param ldr Class loader.
     */
    SimpleBinaryReader(BinaryContext ctx, BinaryInputStream in, @Nullable ClassLoader ldr) {
        this.ctx = ctx;
        this.in = in;
        this.ldr = ldr;
    }

    /**
     * Set handle.
     *
     * @param obj Object.
     * @param pos Position.
     */
    final void setHandle(Object obj, int pos) {
        handles().put(pos, obj);
    }

    /**
     * Get handle.
     *
     * @param pos Position.
     * @return Handle.
     */
    final Object getHandle(int pos) {
        return hnds != null ? hnds.get(pos) : null;
    }

    /**
     * Get all handles.
     *
     * @return Handles.
     */
    public final BinaryReaderHandles handles() {
        if (hnds == null)
            hnds = new BinaryReaderHandles();

        return hnds;
    }

    /**
     * @return Value.
     */
    final boolean[] doReadBooleanArray() {
        int len = in.readInt();

        return in.readBooleanArray(len);
    }

    /**
     * @return Value.
     */
    final short[] doReadShortArray() {
        int len = in.readInt();

        return in.readShortArray(len);
    }

    /**
     * @return Value.
     */
    final char[] doReadCharArray() {
        int len = in.readInt();

        return in.readCharArray(len);
    }

    /**
     * @return Value.
     */
    final int[] doReadIntArray() {
        int len = in.readInt();

        return in.readIntArray(len);
    }

    /**
     * @return Value.
     */
    final long[] doReadLongArray() {
        int len = in.readInt();

        return in.readLongArray(len);
    }

    /**
     * @return Value.
     */
    final float[] doReadFloatArray() {
        int len = in.readInt();

        return in.readFloatArray(len);
    }

    /**
     * @return Value.
     */
    final double[] doReadDoubleArray() {
        int len = in.readInt();

        return in.readDoubleArray(len);
    }

    /**
     * @return Value.
     */
    final BigDecimal doReadDecimal() {
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
    final UUID doReadUuid() {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * @return Value.
     */
    final Date doReadDate() {
        long time = in.readLong();

        return new Date(time);
    }

    /**
     * @return Value.
     */
    final Timestamp doReadTimestamp() {
        long time = in.readLong();
        int nanos = in.readInt();

        Timestamp ts = new Timestamp(time);

        ts.setNanos(ts.getNanos() + nanos);

        return ts;
    }

    /**
     * @return Value.
     */
    final Time doReadTime() {
        long time = in.readLong();

        return new Time(time);
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    final BigDecimal[] doReadDecimalArray() throws BinaryObjectException {
        int len = in.readInt();

        BigDecimal[] arr = new BigDecimal[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.DECIMAL)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadDecimal();
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    final UUID[] doReadUuidArray() throws BinaryObjectException {
        int len = in.readInt();

        UUID[] arr = new UUID[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.UUID)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadUuid();
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    final Date[] doReadDateArray() throws BinaryObjectException {
        int len = in.readInt();

        Date[] arr = new Date[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.DATE)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadDate();
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    final Timestamp[] doReadTimestampArray() throws BinaryObjectException {
        int len = in.readInt();

        Timestamp[] arr = new Timestamp[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.TIMESTAMP)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadTimestamp();
            }
        }

        return arr;
    }

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    final Time[] doReadTimeArray() throws BinaryObjectException {
        int len = in.readInt();

        Time[] arr = new Time[len];

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else {
                if (flag != GridBinaryMarshaller.TIME)
                    throw new BinaryObjectException("Invalid flag value: " + flag);

                arr[i] = doReadTime();
            }
        }

        return arr;
    }

    /**
     * @return Value.
     */
    final BinaryObject doReadBinaryObject(boolean detach) {
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
     * @return Class object specified at the input stream.
     * @throws BinaryObjectException If failed.
     */
    final Class doReadClass()
        throws BinaryObjectException {
        return doReadClass(true);
    }

    /**
     * @param deserialize Doesn't load the class when the flag is {@code false}. Class information is skipped.
     * @return Class object specified at the input stream if {@code deserialize == true}. Otherwise returns {@code null}
     * @throws BinaryObjectException If failed.
     */
    final Class doReadClass(boolean deserialize)
        throws BinaryObjectException {
        int typeId = in.readInt();

        if (!deserialize) {
            // Skip class name at the stream.
            if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
                BinaryImplUtils.doReadClassName(in);

            return null;
        }

        return doReadClass(typeId);
    }

    /**
     * @return Value.
     */
    @SuppressWarnings("ConstantConditions")
    final Object doReadProxy() {
        Class<?>[] intfs = new Class<?>[in.readInt()];

        for (int i = 0; i < intfs.length; i++)
            intfs[i] = doReadClass();

        InvocationHandler ih = (InvocationHandler)doReadObject();

        return Proxy.newProxyInstance(ldr != null ? ldr : CommonUtils.gridClassLoader(), intfs, ih);
    }

    /**
     * Read plain type.
     *
     * @return Plain type.
     */
    final EnumType doReadEnumType() {
        int typeId = in.readInt();

        if (typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            return new EnumType(typeId, null);
        else {
            String clsName = BinaryImplUtils.doReadClassName(in);

            return new EnumType(GridBinaryMarshaller.UNREGISTERED_TYPE_ID, clsName);
        }
    }

    /**
     * @param typeId Type id.
     * @return Class object specified at the input stream.
     * @throws BinaryObjectException If failed.
     */
    final Class doReadClass(int typeId)
        throws BinaryObjectException {
        Class cls;

        if (typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            cls = ctx.descriptorForTypeId(true, typeId, ldr, false).describedClass();
        else {
            String clsName = BinaryImplUtils.doReadClassName(in);

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
     * @param type Plain type.
     * @return Enum.
     */
    final BinaryObjectEx doReadBinaryEnum(EnumType type) {
        return BinaryUtils.binariesFactory.binaryEnum(ctx, type.typeId, type.clsName, in.readInt());
    }

    /**
     * Read binary enum array.
     *
     * @return Enum array.
     */
    private Object[] doReadBinaryEnumArray() {
        int len = in.readInt();

        Object[] arr = (Object[])Array.newInstance(BinaryUtils.binariesFactory.binaryEnumClass(), len);

        for (int i = 0; i < len; i++) {
            byte flag = in.readByte();

            if (flag == GridBinaryMarshaller.NULL)
                arr[i] = null;
            else
                arr[i] = doReadBinaryEnum(doReadEnumType());
        }

        return arr;
    }

    /**
     * @return Object.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable final Object doReadObject() throws BinaryObjectException {
        return new BinaryReaderExImpl(ctx, in, ldr, handles(), false, true).deserialize();
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable final Object unmarshal() throws BinaryObjectException {
        return unmarshal(false, false);
    }

    /**
     * @return Unmarshalled value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable final Object unmarshal(boolean detach, boolean deserialize) throws BinaryObjectException {
        int start = in.position();

        byte flag = in.readByte();

        switch (flag) {
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.HANDLE: {
                int handlePos = start - in.readInt();

                Object obj = getHandle(handlePos);

                if (obj == null) {
                    int retPos = in.position();

                    in.position(handlePos);

                    obj = unmarshal(detach, deserialize);

                    in.position(retPos);
                }

                return obj;
            }

            case GridBinaryMarshaller.OBJ: {
                BinaryImplUtils.checkProtocolVersion(in.readByte());

                int len = BinaryImplUtils.length(in, start);

                BinaryObjectEx po;

                if (detach) {
                    BinaryObjectEx binObj = BinaryUtils.binariesFactory.binaryObject(ctx, in.array(), start);

                    binObj.detachAllowed(true);

                    po = binObj.detach(hnds != null && !hnds.isEmpty());
                }
                else {
                    if (in.offheapPointer() == 0)
                        po = BinaryUtils.binariesFactory.binaryObject(ctx, in.array(), start);
                    else
                        po = BinaryUtils.binariesFactory.binaryOffheapObject(ctx, in.offheapPointer(), start,
                            in.remaining() + in.position());
                }

                in.position(start + len);

                setHandle(po, start);

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
                return doReadDecimal();

            case GridBinaryMarshaller.STRING:
                return BinaryUtils.doReadString(in);

            case GridBinaryMarshaller.UUID:
                return doReadUuid();

            case GridBinaryMarshaller.DATE:
                return doReadDate();

            case GridBinaryMarshaller.TIMESTAMP:
                return doReadTimestamp();

            case GridBinaryMarshaller.TIME:
                return doReadTime();

            case GridBinaryMarshaller.BYTE_ARR:
                return BinaryUtils.doReadByteArray(in);

            case GridBinaryMarshaller.SHORT_ARR:
                return doReadShortArray();

            case GridBinaryMarshaller.INT_ARR:
                return doReadIntArray();

            case GridBinaryMarshaller.LONG_ARR:
                return doReadLongArray();

            case GridBinaryMarshaller.FLOAT_ARR:
                return doReadFloatArray();

            case GridBinaryMarshaller.DOUBLE_ARR:
                return doReadDoubleArray();

            case GridBinaryMarshaller.CHAR_ARR:
                return doReadCharArray();

            case GridBinaryMarshaller.BOOLEAN_ARR:
                return doReadBooleanArray();

            case GridBinaryMarshaller.DECIMAL_ARR:
                return doReadDecimalArray();

            case GridBinaryMarshaller.STRING_ARR:
                return BinaryUtils.doReadStringArray(in);

            case GridBinaryMarshaller.UUID_ARR:
                return doReadUuidArray();

            case GridBinaryMarshaller.DATE_ARR:
                return doReadDateArray();

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                return doReadTimestampArray();

            case GridBinaryMarshaller.TIME_ARR:
                return doReadTimeArray();

            case GridBinaryMarshaller.OBJ_ARR:
                if (BinaryUtils.useBinaryArrays() && !deserialize)
                    return doReadBinaryArray(detach, deserialize, false);
                else
                    return doReadObjectArray(detach, deserialize);

            case GridBinaryMarshaller.COL:
                return doReadCollection(detach, deserialize, null);

            case GridBinaryMarshaller.MAP:
                return doReadMap(detach, deserialize, null);

            case GridBinaryMarshaller.BINARY_OBJ:
                return doReadBinaryObject(detach);

            case GridBinaryMarshaller.ENUM:
            case GridBinaryMarshaller.BINARY_ENUM:
                return doReadBinaryEnum(doReadEnumType());

            case GridBinaryMarshaller.ENUM_ARR:
                if (BinaryUtils.useBinaryArrays() && !deserialize)
                    return doReadBinaryArray(detach, deserialize, true);
                else {
                    doReadEnumType(); // Simply skip this part as we do not need it.

                    return doReadBinaryEnumArray();
                }

            case GridBinaryMarshaller.CLASS:
                return doReadClass();

            case GridBinaryMarshaller.PROXY:
                return doReadProxy();

            case GridBinaryMarshaller.OPTM_MARSH:
                return BinaryImplUtils.doReadOptimized(in, ctx, ldr);

            default:
                throw new BinaryObjectException("Invalid flag value: " + flag);
        }
    }

    /**
     * @param detach Detach flag.
     * @param deserialize Deep flag.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    final Object[] doReadObjectArray(boolean detach, boolean deserialize) throws BinaryObjectException {
        int hPos = positionForHandle();

        Class compType = doReadClass(deserialize);

        int len = in.readInt();

        Object[] arr = (deserialize && !BinaryObject.class.isAssignableFrom(compType))
            ? (Object[])Array.newInstance(compType, len)
            : new Object[len];

        setHandle(arr, hPos);

        for (int i = 0; i < len; i++) {
            Object res = deserializeOrUnmarshal(detach, deserialize);

            if (deserialize && BinaryUtils.useBinaryArrays() && res instanceof BinaryObject)
                arr[i] = ((BinaryObject)res).deserialize(ldr);
            else
                arr[i] = res;
        }

        return arr;
    }

    /**
     * @param detach Detach flag.
     * @param deserialize Deep flag.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    private BinaryArray doReadBinaryArray(boolean detach, boolean deserialize, boolean isEnumArray) {
        int hPos = positionForHandle();

        int compTypeId = in.readInt();
        String compClsName = null;

        if (compTypeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            compClsName = BinaryImplUtils.doReadClassName(in);

        int len = in.readInt();

        Object[] arr = new Object[len];

        BinaryArray res = isEnumArray
            ? new BinaryEnumArray(ctx, compTypeId, compClsName, arr)
            : new BinaryArray(ctx, compTypeId, compClsName, arr);

        setHandle(res, hPos);

        for (int i = 0; i < len; i++)
            arr[i] = deserializeOrUnmarshal(detach, deserialize);

        return res;
    }

    /**
     * @param deserialize Deep flag.
     * @param factory Collection factory.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    final Collection<?> doReadCollection(boolean detach, boolean deserialize, BinaryCollectionFactory factory)
        throws BinaryObjectException {
        int hPos = positionForHandle();

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

        setHandle(col, hPos);

        for (int i = 0; i < size; i++)
            col.add(deserializeOrUnmarshal(detach, deserialize));

        return colType == GridBinaryMarshaller.SINGLETON_LIST ? CommonUtils.convertToSingletonList(col) : col;
    }

    /**
     * @param deserialize Deep flag.
     * @param factory Map factory.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @SuppressWarnings("unchecked")
    final Map<?, ?> doReadMap(boolean detach, boolean deserialize, BinaryMapFactory factory)
        throws BinaryObjectException {
        int hPos = positionForHandle();

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

        setHandle(map, hPos);

        for (int i = 0; i < size; i++) {
            Object key = deserializeOrUnmarshal(detach, deserialize);
            Object val = deserializeOrUnmarshal(detach, deserialize);

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
    private Object deserializeOrUnmarshal(boolean detach, boolean deserialize) {
        return deserialize ? doReadObject() : unmarshal(detach, deserialize);
    }

    /**
     * Get position to be used for handle. We assume here that the hdr byte was read, hence subtract -1.
     *
     * @return Position for handle.
     */
    final int positionForHandle() {
        return in.position() - 1;
    }

    /**
     * Enum type.
     */
    static class EnumType {
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
