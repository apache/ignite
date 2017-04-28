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

import java.io.IOException;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Binary writer implementation.
 */
public class BinaryWriterExImpl implements BinaryWriter, BinaryRawWriterEx, ObjectOutput {
    /** Length: integer. */
    private static final int LEN_INT = 4;

    /** Initial capacity. */
    private static final int INIT_CAP = 1024;

    /** */
    private final BinaryContext ctx;

    /** Output stream. */
    private final BinaryOutputStream out;

    /** Schema. */
    private final BinaryWriterSchemaHolder schema;

    /** */
    private int typeId;

    /** */
    private final int start;

    /** Raw offset position. */
    private int rawOffPos;

    /** Handles. */
    private BinaryWriterHandles handles;

    /** Schema ID. */
    private int schemaId = BinaryUtils.schemaInitialId();

    /** Amount of written fields. */
    private int fieldCnt;

    /** */
    private BinaryInternalMapper mapper;

    /**
     * @param ctx Context.
     */
    public BinaryWriterExImpl(BinaryContext ctx) {
        this(ctx, BinaryThreadLocalContext.get());
    }

    /**
     * @param ctx Context.
     * @param tlsCtx TLS context.
     */
    public BinaryWriterExImpl(BinaryContext ctx, BinaryThreadLocalContext tlsCtx) {
        this(ctx, new BinaryHeapOutputStream(INIT_CAP, tlsCtx.chunk()), tlsCtx.schemaHolder(), null);
    }

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @param handles Handles.
     */
    public BinaryWriterExImpl(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema,
        BinaryWriterHandles handles) {
        this.ctx = ctx;
        this.out = out;
        this.schema = schema;
        this.handles = handles;

        start = out.position();
    }

    /**
     * @param typeId Type ID.
     */
    public void typeId(int typeId) {
        this.typeId = typeId;
    }

    /**
     * Close the writer releasing resources if necessary.
     */
    @Override public void close() {
        out.close();
    }

    /**
     * @param obj Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void marshal(Object obj) throws BinaryObjectException {
        marshal(obj, true);
    }

    /**
     * @param obj Object.
     * @param enableReplace Object replacing enabled flag.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void marshal(Object obj, boolean enableReplace) throws BinaryObjectException {
        String newName = ctx.configuration().getIgniteInstanceName();
        String oldName = IgniteUtils.setCurrentIgniteName(newName);

        try {
            marshal0(obj, enableReplace);
        }
        finally {
            IgniteUtils.restoreOldIgniteName(oldName, newName);
        }
    }

    /**
     * @param obj Object.
     * @param enableReplace Object replacing enabled flag.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private void marshal0(Object obj, boolean enableReplace) throws BinaryObjectException {
        assert obj != null;

        Class<?> cls = obj.getClass();

        BinaryClassDescriptor desc = ctx.descriptorForClass(cls, false);

        if (desc == null)
            throw new BinaryObjectException("Object is not binary: [class=" + cls + ']');

        if (desc.excluded()) {
            out.writeByte(GridBinaryMarshaller.NULL);

            return;
        }

        if (desc.useOptimizedMarshaller()) {
            out.writeByte(GridBinaryMarshaller.OPTM_MARSH);

            try {
                byte[] arr = U.marshal(ctx.optimizedMarsh(), obj);

                writeInt(arr.length);

                write(arr);
            }
            catch (IgniteCheckedException e) {
                throw new BinaryObjectException("Failed to marshal object with optimized marshaller: " + obj, e);
            }

            return;
        }

        if (enableReplace && desc.isWriteReplace()) {
            Object replacedObj = desc.writeReplace(obj);

            if (replacedObj == null) {
                out.writeByte(GridBinaryMarshaller.NULL);

                return;
            }

            marshal(replacedObj, false);

            return;
        }

        desc.write(obj, this);
    }

    /**
     * @return Array.
     */
    public byte[] array() {
        return out.arrayCopy();
    }

    /**
     * @return Stream current position.
     */
    int position() {
        return out.position();
    }

    /**
     * Sets new position.
     *
     * @param pos Position.
     */
    void position(int pos) {
        out.position(pos);
    }

    /**
     * Perform pre-write. Reserves space for header and writes class name if needed.
     *
     * @param clsName Class name (optional).
     */
    public void preWrite(@Nullable String clsName) {
        out.position(out.position() + GridBinaryMarshaller.DFLT_HDR_LEN);

        if (clsName != null)
            doWriteString(clsName);
    }

    /**
     * Perform post-write. Fills object header.
     *
     * @param userType User type flag.
     * @param registered Whether type is registered.
     */
    public void postWrite(boolean userType, boolean registered) {
        short flags;
        boolean useCompactFooter;

        if (userType) {
            if (ctx.isCompactFooter()) {
                flags = BinaryUtils.FLAG_USR_TYP | BinaryUtils.FLAG_COMPACT_FOOTER;
                useCompactFooter = true;
            }
            else {
                flags = BinaryUtils.FLAG_USR_TYP;
                useCompactFooter = false;
            }
        }
        else {
            flags = 0;
            useCompactFooter = false;
        }

        int finalSchemaId;
        int offset;

        if (fieldCnt != 0) {
            finalSchemaId = schemaId;
            offset = out.position() - start;

            // Write the schema.
            flags |= BinaryUtils.FLAG_HAS_SCHEMA;

            int offsetByteCnt = schema.write(out, fieldCnt, useCompactFooter);

            if (offsetByteCnt == BinaryUtils.OFFSET_1)
                flags |= BinaryUtils.FLAG_OFFSET_ONE_BYTE;
            else if (offsetByteCnt == BinaryUtils.OFFSET_2)
                flags |= BinaryUtils.FLAG_OFFSET_TWO_BYTES;

            // Write raw offset if needed.
            if (rawOffPos != 0) {
                flags |= BinaryUtils.FLAG_HAS_RAW;

                out.writeInt(rawOffPos - start);
            }
        }
        else {
            if (rawOffPos != 0) {
                finalSchemaId = 0;
                offset = rawOffPos - start;

                // If there is no schema, we are free to write raw offset to schema offset.
                flags |= BinaryUtils.FLAG_HAS_RAW;
            }
            else {
                finalSchemaId = 0;
                offset = 0;
            }
        }

        // Actual write.
        int retPos = out.position();

        out.unsafePosition(start);

        out.unsafeWriteByte(GridBinaryMarshaller.OBJ);
        out.unsafeWriteByte(GridBinaryMarshaller.PROTO_VER);
        out.unsafeWriteShort(flags);
        out.unsafeWriteInt(registered ? typeId : GridBinaryMarshaller.UNREGISTERED_TYPE_ID);
        out.unsafePosition(start + GridBinaryMarshaller.TOTAL_LEN_POS);
        out.unsafeWriteInt(retPos - start);
        out.unsafeWriteInt(finalSchemaId);
        out.unsafeWriteInt(offset);

        out.unsafePosition(retPos);
    }

    /**
     * Perform post-write hash code update if necessary.
     *
     * @param clsName Class name. Always null if class is registered.
     */
    public void postWriteHashCode(@Nullable String clsName) {
        int typeId = clsName == null ? this.typeId : ctx.typeId(clsName);

        BinaryIdentityResolver identity = ctx.identity(typeId);

        if (out.hasArray()) {
            // Heap.
            byte[] data = out.array();

            BinaryObjectImpl obj = new BinaryObjectImpl(ctx, data, start);

            BinaryPrimitives.writeInt(data, start + GridBinaryMarshaller.HASH_CODE_POS, identity.hashCode(obj));
        }
        else {
            // Offheap.
            long ptr = out.rawOffheapPointer();

            assert ptr != 0;

            BinaryObjectOffheapImpl obj = new BinaryObjectOffheapImpl(ctx, ptr, start, out.capacity());

            BinaryPrimitives.writeInt(ptr, start + GridBinaryMarshaller.HASH_CODE_POS, identity.hashCode(obj));
        }
    }

    /**
     * Pop schema.
     */
    public void popSchema() {
        if (fieldCnt > 0)
            schema.pop(fieldCnt);
    }

    /**
     * @param val Byte array.
     */
    public void write(byte[] val) {
        out.writeByteArray(val);
    }

    /**
     * @param val Byte array.
     * @param off Offset.
     * @param len Length.
     */
    public void write(byte[] val, int off, int len) {
        out.write(val, off, len);
    }

    /**
     * @param val String value.
     */
    public void doWriteDecimal(@Nullable BigDecimal val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.DECIMAL);

            out.unsafeWriteInt(val.scale());

            BigInteger intVal = val.unscaledValue();

            boolean negative = intVal.signum() == -1;

            if (negative)
                intVal = intVal.negate();

            byte[] vals = intVal.toByteArray();

            if (negative)
                vals[0] |= -0x80;

            out.unsafeWriteInt(vals.length);
            out.writeByteArray(vals);
        }
    }

    /**
     * @param val String value.
     */
    public void doWriteString(@Nullable String val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            byte[] strArr;

            if (BinaryUtils.USE_STR_SERIALIZATION_VER_2)
                strArr = BinaryUtils.strToUtf8Bytes(val);
            else
                strArr = val.getBytes(UTF_8);

            out.unsafeEnsure(1);
            out.unsafeWriteByte(GridBinaryMarshaller.STRING);

            doWriteUnsignedVarint(strArr.length);

            out.write(strArr, 0, strArr.length);
        }
    }

    /**
     * @param uuid UUID.
     */
    public void doWriteUuid(@Nullable UUID uuid) {
        if (uuid == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 8 + 8);
            out.unsafeWriteByte(GridBinaryMarshaller.UUID);
            out.unsafeWriteLong(uuid.getMostSignificantBits());
            out.unsafeWriteLong(uuid.getLeastSignificantBits());
        }
    }

    /**
     * @param date Date.
     */
    public void doWriteDate(@Nullable Date date) {
        if (date == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 8);
            out.unsafeWriteByte(GridBinaryMarshaller.DATE);
            out.unsafeWriteLong(date.getTime());
        }
    }

    /**
     * @param ts Timestamp.
     */
    public void doWriteTimestamp(@Nullable Timestamp ts) {
        if (ts== null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 8 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.TIMESTAMP);
            out.unsafeWriteLong(ts.getTime());
            out.unsafeWriteInt(ts.getNanos() % 1000000);
        }
    }

    /**
     * @param time Time.
     */
    public void doWriteTime(@Nullable Time time) {
        if (time== null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 8);
            out.unsafeWriteByte(GridBinaryMarshaller.TIME);
            out.unsafeWriteLong(time.getTime());
        }
    }

    /**
     * Write object.
     *
     * @param obj Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public void doWriteObject(@Nullable Object obj) throws BinaryObjectException {
        if (obj == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, out, schema, handles());

            writer.marshal(obj);
        }
    }

    /**
     * @param val Byte array.
     */
    void doWriteByteArray(@Nullable byte[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.BYTE_ARR);
            out.unsafeWriteInt(val.length);

            out.writeByteArray(val);
        }
    }

    /**
     * @param val Short array.
     */
    void doWriteShortArray(@Nullable short[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.SHORT_ARR);
            out.unsafeWriteInt(val.length);

            out.writeShortArray(val);
        }
    }

    /**
     * @param val Integer array.
     */
    void doWriteIntArray(@Nullable int[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.INT_ARR);
            out.unsafeWriteInt(val.length);

            out.writeIntArray(val);
        }
    }

    /**
     * Writes integer value in varint encoding.
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">More information about varint.</a>
     *
     * @param val Value to write. Must be greater than zero.
     */
    void doWriteUnsignedVarint(int val) {
        assert val >= 0;

        while (val > 0x7f) {
            out.writeByte((byte)((val & 0x7f) | 0x80));
            val >>>= 7;
        }

        out.writeByte((byte)(val & 0x7F));
    }

    /**
     * @param val Long array.
     */
    void doWriteLongArray(@Nullable long[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.LONG_ARR);
            out.unsafeWriteInt(val.length);

            out.writeLongArray(val);
        }
    }

    /**
     * @param val Float array.
     */
    void doWriteFloatArray(@Nullable float[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.FLOAT_ARR);
            out.unsafeWriteInt(val.length);

            out.writeFloatArray(val);
        }
    }

    /**
     * @param val Double array.
     */
    void doWriteDoubleArray(@Nullable double[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.DOUBLE_ARR);
            out.unsafeWriteInt(val.length);

            out.writeDoubleArray(val);
        }
    }

    /**
     * @param val Char array.
     */
    void doWriteCharArray(@Nullable char[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.CHAR_ARR);
            out.unsafeWriteInt(val.length);

            out.writeCharArray(val);
        }
    }

    /**
     * @param val Boolean array.
     */
    void doWriteBooleanArray(@Nullable boolean[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.BOOLEAN_ARR);
            out.unsafeWriteInt(val.length);

            out.writeBooleanArray(val);
        }
    }

    /**
     * @param val Array of strings.
     */
    void doWriteDecimalArray(@Nullable BigDecimal[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.DECIMAL_ARR);
            out.unsafeWriteInt(val.length);

            for (BigDecimal str : val)
                doWriteDecimal(str);
        }
    }

    /**
     * @param val Array of strings.
     */
    void doWriteStringArray(@Nullable String[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.STRING_ARR);
            out.unsafeWriteInt(val.length);

            for (String str : val)
                doWriteString(str);
        }
    }

    /**
     * @param val Array of UUIDs.
     */
    void doWriteUuidArray(@Nullable UUID[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.UUID_ARR);
            out.unsafeWriteInt(val.length);

            for (UUID uuid : val)
                doWriteUuid(uuid);
        }
    }

    /**
     * @param val Array of dates.
     */
    void doWriteDateArray(@Nullable Date[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.DATE_ARR);
            out.unsafeWriteInt(val.length);

            for (Date date : val)
                doWriteDate(date);
        }
    }

     /**
      * @param val Array of timestamps.
      */
     void doWriteTimestampArray(@Nullable Timestamp[] val) {
         if (val == null)
             out.writeByte(GridBinaryMarshaller.NULL);
         else {
             out.unsafeEnsure(1 + 4);
             out.unsafeWriteByte(GridBinaryMarshaller.TIMESTAMP_ARR);
             out.unsafeWriteInt(val.length);

             for (Timestamp ts : val)
                 doWriteTimestamp(ts);
         }
     }

    /**
     * @param val Array of time.
     */
    void doWriteTimeArray(@Nullable Time[] val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.TIME_ARR);
            out.unsafeWriteInt(val.length);

            for (Time time : val)
                doWriteTime(time);
        }
    }

    /**
     * @param val Array of objects.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void doWriteObjectArray(@Nullable Object[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            BinaryClassDescriptor desc = ctx.descriptorForClass(val.getClass().getComponentType(), false);

            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.OBJ_ARR);

            if (desc.registered())
                out.unsafeWriteInt(desc.typeId());
            else {
                out.unsafeWriteInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);

                doWriteString(val.getClass().getComponentType().getName());
            }

            out.writeInt(val.length);

            for (Object obj : val)
                doWriteObject(obj);
        }
    }

    /**
     * @param col Collection.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void doWriteCollection(@Nullable Collection<?> col) throws BinaryObjectException {
        if (col == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            if (tryWriteAsHandle(col))
                return;

            out.unsafeEnsure(1 + 4 + 1);
            out.unsafeWriteByte(GridBinaryMarshaller.COL);
            out.unsafeWriteInt(col.size());
            out.unsafeWriteByte(ctx.collectionType(col.getClass()));

            for (Object obj : col)
                doWriteObject(obj);
        }
    }

    /**
     * @param map Map.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void doWriteMap(@Nullable Map<?, ?> map) throws BinaryObjectException {
        if (map == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            if (tryWriteAsHandle(map))
                return;

            out.unsafeEnsure(1 + 4 + 1);
            out.unsafeWriteByte(GridBinaryMarshaller.MAP);
            out.unsafeWriteInt(map.size());
            out.unsafeWriteByte(ctx.mapType(map.getClass()));

            for (Map.Entry<?, ?> e : map.entrySet()) {
                doWriteObject(e.getKey());
                doWriteObject(e.getValue());
            }
        }
    }

    /**
     * @param val Value.
     */
    void doWriteEnum(@Nullable Enum<?> val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryClassDescriptor desc = ctx.descriptorForClass(val.getClass(), false);

            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.ENUM);

            if (desc.registered())
                out.unsafeWriteInt(desc.typeId());
            else {
                out.unsafeWriteInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);
                doWriteString(val.getClass().getName());
            }

            out.writeInt(val.ordinal());
        }
    }

    /**
     * @param val Value.
     */
    void doWriteBinaryEnum(BinaryEnumObjectImpl val) {
        assert val != null;

        int typeId = val.typeId();

        out.unsafeEnsure(1 + 4);

        out.unsafeWriteByte(GridBinaryMarshaller.ENUM);
        out.unsafeWriteInt(typeId);

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            doWriteString(val.className());

        out.writeInt(val.enumOrdinal());
    }

    /**
     * @param val Array.
     */
    void doWriteEnumArray(@Nullable Object[] val) {
        assert val == null || val.getClass().getComponentType().isEnum();

        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryClassDescriptor desc = ctx.descriptorForClass(val.getClass().getComponentType(), false);

            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.ENUM_ARR);

            if (desc.registered())
                out.unsafeWriteInt(desc.typeId());
            else {
                out.unsafeWriteInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);

                doWriteString(val.getClass().getComponentType().getName());
            }

            out.writeInt(val.length);

            // TODO: Denis: Redundant data for each element of the array.
            for (Object o : val)
                doWriteEnum((Enum<?>)o);
        }
    }

    /**
     * @param val Class.
     */
    void doWriteClass(@Nullable Class val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryClassDescriptor desc = ctx.descriptorForClass(val, false);

            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.CLASS);

            if (desc.registered())
                out.unsafeWriteInt(desc.typeId());
            else {
                out.unsafeWriteInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);

                doWriteString(val.getName());
            }
        }
    }

    /**
     * @param proxy Proxy.
     */
    public void doWriteProxy(Proxy proxy, Class<?>[] intfs) {
        if (proxy == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.PROXY);
            out.unsafeWriteInt(intfs.length);

            for (Class<?> intf : intfs) {
                BinaryClassDescriptor desc = ctx.descriptorForClass(intf, false);

                if (desc.registered())
                    out.writeInt(desc.typeId());
                else {
                    out.writeInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);

                    doWriteString(intf.getName());
                }
            }

            InvocationHandler ih = Proxy.getInvocationHandler(proxy);

            assert ih != null;

            doWriteObject(ih);
        }
    }

    /**
     * @param po Binary object.
     */
    public void doWriteBinaryObject(@Nullable BinaryObjectImpl po) {
        if (po == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            byte[] poArr = po.array();

            out.unsafeEnsure(1 + 4 + poArr.length + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.BINARY_OBJ);
            out.unsafeWriteInt(poArr.length);
            out.writeByteArray(poArr);
            out.unsafeWriteInt(po.start());
        }
    }

    /**
     * @param val Value.
     */
    void writeByteFieldPrimitive(byte val) {
        out.unsafeEnsure(1 + 1);

        out.unsafeWriteByte(GridBinaryMarshaller.BYTE);
        out.unsafeWriteByte(val);
    }

    /**
     * @param val Value.
     */
    void writeByteField(@Nullable Byte val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else
            writeByteFieldPrimitive(val);
    }

    /**
     * @param val Class.
     */
    void writeClassField(@Nullable Class val) {
        doWriteClass(val);
    }

    /**
     * @param val Value.
     */
    void writeShortFieldPrimitive(short val) {
        out.unsafeEnsure(1 + 2);

        out.unsafeWriteByte(GridBinaryMarshaller.SHORT);
        out.unsafeWriteShort(val);
    }

    /**
     * @param val Value.
     */
    void writeShortField(@Nullable Short val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else
            writeShortFieldPrimitive(val);
    }

    /**
     * @param val Value.
     */
    void writeIntFieldPrimitive(int val) {
        out.unsafeEnsure(1 + 4);

        out.unsafeWriteByte(GridBinaryMarshaller.INT);
        out.unsafeWriteInt(val);
    }

    /**
     * @param val Value.
     */
    void writeIntField(@Nullable Integer val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else
            writeIntFieldPrimitive(val);
    }

    /**
     * @param val Value.
     */
    void writeLongFieldPrimitive(long val) {
        out.unsafeEnsure(1 + 8);

        out.unsafeWriteByte(GridBinaryMarshaller.LONG);
        out.unsafeWriteLong(val);
    }

    /**
     * @param val Value.
     */
    void writeLongField(@Nullable Long val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else
            writeLongFieldPrimitive(val);
    }

    /**
     * @param val Value.
     */
    void writeFloatFieldPrimitive(float val) {
        out.unsafeEnsure(1 + 4);

        out.unsafeWriteByte(GridBinaryMarshaller.FLOAT);
        out.unsafeWriteFloat(val);
    }

    /**
     * @param val Value.
     */
    void writeFloatField(@Nullable Float val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else
            writeFloatFieldPrimitive(val);
    }

    /**
     * @param val Value.
     */
    void writeDoubleFieldPrimitive(double val) {
        out.unsafeEnsure(1 + 8);

        out.unsafeWriteByte(GridBinaryMarshaller.DOUBLE);
        out.unsafeWriteDouble(val);
    }

    /**
     * @param val Value.
     */
    void writeDoubleField(@Nullable Double val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else
            writeDoubleFieldPrimitive(val);
    }

    /**
     * @param val Value.
     */
    void writeCharFieldPrimitive(char val) {
        out.unsafeEnsure(1 + 2);

        out.unsafeWriteByte(GridBinaryMarshaller.CHAR);
        out.unsafeWriteChar(val);
    }

    /**
     * @param val Value.
     */
    void writeCharField(@Nullable Character val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else
            writeCharFieldPrimitive(val);
    }

    /**
     * @param val Value.
     */
    void writeBooleanFieldPrimitive(boolean val) {
        out.unsafeEnsure(1 + 1);

        out.unsafeWriteByte(GridBinaryMarshaller.BOOLEAN);
        out.unsafeWriteBoolean(val);
    }

    /**
     * @param val Value.
     */
    void writeBooleanField(@Nullable Boolean val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else
            writeBooleanFieldPrimitive(val);
    }

    /**
     * @param val Value.
     */
    void writeDecimalField(@Nullable BigDecimal val) {
        doWriteDecimal(val);
    }

    /**
     * @param val Value.
     */
    void writeStringField(@Nullable String val) {
        doWriteString(val);
    }

    /**
     * @param val Value.
     */
    void writeUuidField(@Nullable UUID val) {
        doWriteUuid(val);
    }

    /**
     * @param val Value.
     */
    void writeDateField(@Nullable Date val) {
        doWriteDate(val);
    }

    /**
     * @param val Value.
     */
    void writeTimestampField(@Nullable Timestamp val) {
        doWriteTimestamp(val);
    }

    /**
     * @param val Value.
     */
    void writeTimeField(@Nullable Time val) {
        doWriteTime(val);
    }

    /**
     * @param obj Object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeObjectField(@Nullable Object obj) throws BinaryObjectException {
        doWriteObject(obj);
    }

    /**
     * @param val Value.
     */
    void writeByteArrayField(@Nullable byte[] val) {
        doWriteByteArray(val);
    }

    /**
     * @param val Value.
     */
    void writeShortArrayField(@Nullable short[] val) {
        doWriteShortArray(val);
    }

    /**
     * @param val Value.
     */
    void writeIntArrayField(@Nullable int[] val) {
        doWriteIntArray(val);
    }

    /**
     * @param val Value.
     */
    void writeLongArrayField(@Nullable long[] val) {
        doWriteLongArray(val);
    }

    /**
     * @param val Value.
     */
    void writeFloatArrayField(@Nullable float[] val) {
        doWriteFloatArray(val);
    }

    /**
     * @param val Value.
     */
    void writeDoubleArrayField(@Nullable double[] val) {
        doWriteDoubleArray(val);
    }

    /**
     * @param val Value.
     */
    void writeCharArrayField(@Nullable char[] val) {
        doWriteCharArray(val);
    }

    /**
     * @param val Value.
     */
    void writeBooleanArrayField(@Nullable boolean[] val) {
        doWriteBooleanArray(val);
    }

    /**
     * @param val Value.
     */
    void writeDecimalArrayField(@Nullable BigDecimal[] val) {
        doWriteDecimalArray(val);
    }

    /**
     * @param val Value.
     */
    void writeStringArrayField(@Nullable String[] val) {
        doWriteStringArray(val);
    }

    /**
     * @param val Value.
     */
    void writeUuidArrayField(@Nullable UUID[] val) {
        doWriteUuidArray(val);
    }

    /**
     * @param val Value.
     */
    void writeDateArrayField(@Nullable Date[] val) {
        doWriteDateArray(val);
    }

    /**
     * @param val Value.
     */
    void writeTimestampArrayField(@Nullable Timestamp[] val) {
        doWriteTimestampArray(val);
    }

    /**
     * @param val Value.
     */
    void writeTimeArrayField(@Nullable Time[] val) {
        doWriteTimeArray(val);
    }

    /**
     * @param val Value.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeObjectArrayField(@Nullable Object[] val) throws BinaryObjectException {
        doWriteObjectArray(val);
    }

    /**
     * @param col Collection.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeCollectionField(@Nullable Collection<?> col) throws BinaryObjectException {
        doWriteCollection(col);
    }

    /**
     * @param map Map.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeMapField(@Nullable Map<?, ?> map) throws BinaryObjectException {
        doWriteMap(map);
    }

    /**
     * @param val Value.
     */
    void writeEnumField(@Nullable Enum<?> val) {
        doWriteEnum(val);
    }

    /**
     * @param val Value.
     */
    void writeEnumArrayField(@Nullable Object[] val) {
        doWriteEnumArray(val);
    }

    /**
     * @param po Binary object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void writeBinaryObjectField(@Nullable BinaryObjectImpl po) throws BinaryObjectException {
        doWriteBinaryObject(po);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeByteField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) throws BinaryObjectException {
        out.writeByte(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeShortField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) throws BinaryObjectException {
        out.writeShort(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeIntField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) throws BinaryObjectException {
        out.writeInt(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeLongField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) throws BinaryObjectException {
        out.writeLong(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeFloatField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) throws BinaryObjectException {
        out.writeFloat(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDoubleField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) throws BinaryObjectException {
        out.writeDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeCharField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) throws BinaryObjectException {
        out.writeChar(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeBooleanField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) throws BinaryObjectException {
        out.writeBoolean(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(String fieldName, @Nullable BigDecimal val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDecimalField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(@Nullable BigDecimal val) throws BinaryObjectException {
        doWriteDecimal(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeStringField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(@Nullable String val) throws BinaryObjectException {
        doWriteString(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeUuidField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(@Nullable UUID val) throws BinaryObjectException {
        doWriteUuid(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(String fieldName, @Nullable Date val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDateField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(@Nullable Date val) throws BinaryObjectException {
        doWriteDate(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeTimestampField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(@Nullable Timestamp val) throws BinaryObjectException {
        doWriteTimestamp(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTime(String fieldName, @Nullable Time val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeTimeField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTime(@Nullable Time val) throws BinaryObjectException {
        doWriteTime(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeObjectField(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(@Nullable Object obj) throws BinaryObjectException {
        doWriteObject(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectDetached(@Nullable Object obj) throws BinaryObjectException {
        if (obj == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, out, schema, null);

            writer.marshal(obj);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeByteArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(@Nullable byte[] val) throws BinaryObjectException {
        doWriteByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeShortArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(@Nullable short[] val) throws BinaryObjectException {
        doWriteShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeIntArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(@Nullable int[] val) throws BinaryObjectException {
        doWriteIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeLongArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(@Nullable long[] val) throws BinaryObjectException {
        doWriteLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeFloatArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(@Nullable float[] val) throws BinaryObjectException {
        doWriteFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDoubleArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(@Nullable double[] val) throws BinaryObjectException {
        doWriteDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeCharArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(@Nullable char[] val) throws BinaryObjectException {
        doWriteCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeBooleanArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(@Nullable boolean[] val) throws BinaryObjectException {
        doWriteBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(String fieldName, @Nullable BigDecimal[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDecimalArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(@Nullable BigDecimal[] val) throws BinaryObjectException {
        doWriteDecimalArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeStringArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(@Nullable String[] val) throws BinaryObjectException {
        doWriteStringArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeUuidArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(@Nullable UUID[] val) throws BinaryObjectException {
        doWriteUuidArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(String fieldName, @Nullable Date[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDateArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(@Nullable Date[] val) throws BinaryObjectException {
        doWriteDateArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestampArray(String fieldName, @Nullable Timestamp[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeTimestampArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestampArray(@Nullable Timestamp[] val) throws BinaryObjectException {
        doWriteTimestampArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimeArray(String fieldName, @Nullable Time[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeTimeArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimeArray(@Nullable Time[] val) throws BinaryObjectException {
        doWriteTimeArray(val);
    }

     /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeObjectArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(@Nullable Object[] val) throws BinaryObjectException {
        doWriteObjectArray(val);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeCollectionField(col);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(@Nullable Collection<T> col) throws BinaryObjectException {
        doWriteCollection(col);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeMapField(map);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(@Nullable Map<K, V> map) throws BinaryObjectException {
        doWriteMap(map);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeEnumField(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(T val) throws BinaryObjectException {
        doWriteEnum(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeEnumArrayField(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(T[] val) throws BinaryObjectException {
        doWriteEnumArray(val);
    }

    /** {@inheritDoc} */
    @Override public BinaryRawWriter rawWriter() {
        if (rawOffPos == 0)
            rawOffPos = out.position();

        return this;
    }

    /** {@inheritDoc} */
    @Override public BinaryOutputStream out() {
        return out;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public void writeBytes(String s) throws IOException {
        int len = s.length();

        writeInt(len);

        for (int i = 0; i < len; i++)
            writeByte(s.charAt(i));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public void writeChars(String s) throws IOException {
        int len = s.length();

        writeInt(len);

        for (int i = 0; i < len; i++)
            writeChar(s.charAt(i));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NullableProblems")
    @Override public void writeUTF(String s) throws IOException {
        writeString(s);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int v) throws IOException {
        out.writeByte((byte) v);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int v) throws IOException {
        out.writeShort((short) v);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int v) throws IOException {
        out.writeChar((char) v);
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        out.writeByte((byte) b);
    }

    /** {@inheritDoc} */
    @Override public void flush() throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int reserveInt() {
        int pos = out.position();

        out.position(pos + LEN_INT);

        return pos;
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int pos, int val) throws BinaryObjectException {
        out.writeInt(pos, val);
    }

    /**
     * @param fieldName Field name.
     * @throws org.apache.ignite.binary.BinaryObjectException If fields are not allowed.
     */
    private void writeFieldId(String fieldName) throws BinaryObjectException {
        A.notNull(fieldName, "fieldName");

        if (rawOffPos != 0)
            throw new BinaryObjectException("Individual field can't be written after raw writer is acquired.");

        if (mapper == null)
            mapper = ctx.userTypeMapper(typeId);

        assert mapper != null;

        int id = mapper.fieldId(typeId, fieldName);

        writeFieldId(id);
    }

    /**
     * Write field ID.
     * @param fieldId Field ID.
     */
    public void writeFieldId(int fieldId) {
        int fieldOff = out.position() - start;

        // Advance schema hash.
        schemaId = BinaryUtils.updateSchemaId(schemaId, fieldId);

        schema.push(fieldId, fieldOff);

        fieldCnt++;
    }

    /**
     * Write field ID without schema ID update. This method should be used when schema ID is stable because class
     * is seializable.
     *
     * @param fieldId Field ID.
     */
    public void writeFieldIdNoSchemaUpdate(int fieldId) {
        int fieldOff = out.position() - start;

        schema.push(fieldId, fieldOff);

        fieldCnt++;
    }

    /**
     * @param schemaId Schema ID.
     */
    public void schemaId(int schemaId) {
        this.schemaId = schemaId;
    }

    /**
     * @return Schema ID.
     */
    public int schemaId() {
        return schemaId;
    }

    /**
     * @return Current writer's schema.
     */
    public BinarySchema currentSchema() {
        BinarySchema.Builder builder = BinarySchema.Builder.newBuilder();

        if (schema != null)
            schema.build(builder, fieldCnt);

        return builder.build();
    }

    /**
     * Get current handles. If they are {@code null}, then we should create them. Otherwise we will not see updates
     * performed by child writers.
     *
     * @return Handles.
     */
    private BinaryWriterHandles handles() {
        if (handles == null)
            handles = new BinaryWriterHandles();

        return handles;
    }

    /**
     * Attempts to write the object as a handle.
     *
     * @param obj Object to write.
     * @return {@code true} if the object has been written as a handle.
     */
    boolean tryWriteAsHandle(Object obj) {
        assert obj != null;

        int pos = out.position();

        BinaryWriterHandles handles0 = handles();

        int old = handles0.put(obj, pos);

        if (old == BinaryWriterHandles.POS_NULL)
            return false;
        else {
            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.HANDLE);
            out.unsafeWriteInt(pos - old);

            return true;
        }
    }

    /**
     * Create new writer with same context.
     *
     * @param typeId type
     * @return New writer.
     */
    public BinaryWriterExImpl newWriter(int typeId) {
        BinaryWriterExImpl res = new BinaryWriterExImpl(ctx, out, schema, handles());

        res.typeId(typeId);

        return res;
    }

    /**
     * @return Binary context.
     */
    public BinaryContext context() {
        return ctx;
    }
}
