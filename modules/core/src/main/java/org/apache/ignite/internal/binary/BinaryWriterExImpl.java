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
import java.io.InputStream;
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
import org.apache.ignite.internal.UnregisteredClassException;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.util.CommonUtils.MAX_ARRAY_SIZE;

/**
 * Binary writer implementation.
 */
class BinaryWriterExImpl implements BinaryWriterEx {
    /** Length: integer. */
    private static final int LEN_INT = 4;

    /** Default buffer size for reading from streams. */
    public static final int DEFAULT_BUFFER_SIZE = 8 * 1024;

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

    /** */
    private boolean failIfUnregistered;

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @param handles Handles.
     */
    public BinaryWriterExImpl(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema, BinaryWriterHandles handles) {
        this.ctx = ctx;
        this.out = out;
        this.schema = schema;
        this.handles = handles;

        start = out.position();
    }

    /** {@inheritDoc} */
    @Override public boolean failIfUnregistered() {
        return failIfUnregistered;
    }

    /** {@inheritDoc} */
    @Override public void failIfUnregistered(boolean failIfUnregistered) {
        this.failIfUnregistered = failIfUnregistered;
    }

    /** {@inheritDoc} */
    @Override public void typeId(int typeId) {
        this.typeId = typeId;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        out.close();
    }

    /** {@inheritDoc} */
    @Override public void marshal(Object obj) throws BinaryObjectException {
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

        BinaryClassDescriptor desc = ctx.descriptorForClass(cls);

        if (!desc.registered()) {
            if (failIfUnregistered)
                throw new UnregisteredClassException(cls);
            else {
                // Metadata is registered for OBJECT and BINARY during actual writing.
                boolean registerMeta = !(desc.isObject() || desc.isBinary());

                desc = ctx.registerDescriptor(desc, registerMeta, false);
            }
        }

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

    /** {@inheritDoc} */
    @Override public byte[] array() {
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

    /** {@inheritDoc} */
    @Override public void preWrite(@Nullable String clsName) {
        out.position(out.position() + GridBinaryMarshaller.DFLT_HDR_LEN);

        if (clsName != null)
            writeString(clsName);
    }

    /** {@inheritDoc} */
    @Override public void postWrite(boolean userType, boolean registered) {
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

        int offset;

        if (fieldCnt != 0) {
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
                offset = rawOffPos - start;

                // If there is no schema, we are free to write raw offset to schema offset.
                flags |= BinaryUtils.FLAG_HAS_RAW;
            }
            else
                offset = GridBinaryMarshaller.DFLT_HDR_LEN;
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
        out.unsafeWriteInt(schemaId);
        out.unsafeWriteInt(offset);

        out.unsafePosition(retPos);
    }

    /** {@inheritDoc} */
    @Override public void postWriteHashCode(@Nullable String clsName) {
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

    /** {@inheritDoc} */
    @Override public void popSchema() {
        if (fieldCnt > 0)
            schema.pop(fieldCnt);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] val) {
        out.writeByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] val, int off, int len) {
        out.write(val, off, len);
    }

    /**
     * @param val Array wrapper.
     * @throws BinaryObjectException In case of error.
     */
    void writeBinaryArray(BinaryArray val) throws BinaryObjectException {
        if (val.array() == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(val instanceof BinaryEnumArray
                ? GridBinaryMarshaller.ENUM_ARR
                : GridBinaryMarshaller.OBJ_ARR
            );
            out.unsafeWriteInt(val.componentTypeId());

            if (val.componentTypeId() == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
                writeString(val.componentClassName());

            out.writeInt(val.array().length);

            for (Object obj : val.array())
                writeObject(obj);
        }
    }

    /**
     * @param val Value.
     */
    void writeBinaryEnum(BinaryEnumObjectImpl val) {
        assert val != null;

        int typeId = val.typeId();

        if (typeId != GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            out.unsafeEnsure(1 + 4 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.BINARY_ENUM);
            out.unsafeWriteInt(typeId);
            out.unsafeWriteInt(val.enumOrdinal());
        }
        else {
            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.BINARY_ENUM);
            out.unsafeWriteInt(typeId);

            writeString(val.enumClassName());

            out.writeInt(val.enumOrdinal());
        }
    }

    /**
     * @param proxy Proxy.
     */
    public void writeProxy(Proxy proxy, Class<?>[] intfs) {
        if (proxy == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.PROXY);
            out.unsafeWriteInt(intfs.length);

            for (Class<?> intf : intfs) {
                BinaryClassDescriptor desc = ctx.registerClass(intf, true, failIfUnregistered);

                if (desc.registered())
                    out.writeInt(desc.typeId());
                else {
                    out.writeInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);

                    writeString(intf.getName());
                }
            }

            InvocationHandler ih = Proxy.getInvocationHandler(proxy);

            assert ih != null;

            writeObject(ih);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeByteFieldPrimitive(byte val) {
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
    void writeClass(@Nullable Class val) {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryClassDescriptor desc = ctx.registerClass(val, false, failIfUnregistered);

            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.CLASS);

            if (desc.registered())
                out.unsafeWriteInt(desc.typeId());
            else {
                out.unsafeWriteInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);

                writeString(val.getName());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void writeShortFieldPrimitive(short val) {
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

    /** {@inheritDoc} */
    @Override public void writeIntFieldPrimitive(int val) {
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

    /** {@inheritDoc} */
    @Override public void writeLongFieldPrimitive(long val) {
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

    /** {@inheritDoc} */
    @Override public void writeFloatFieldPrimitive(float val) {
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

    /** {@inheritDoc} */
    @Override public void writeDoubleFieldPrimitive(double val) {
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

    /** {@inheritDoc} */
    @Override public void writeCharFieldPrimitive(char val) {
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

    /** {@inheritDoc} */
    @Override public void writeBooleanFieldPrimitive(boolean val) {
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

    /** {@inheritDoc} */
    @Override public void writeBinaryObject(@Nullable BinaryObjectImpl po) throws BinaryObjectException {
        if (po == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            byte[] poArr = po.bytes();

            out.unsafeEnsure(1 + 4 + poArr.length + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.BINARY_OBJ);
            out.unsafeWriteInt(poArr.length);
            out.writeByteArray(poArr);
            out.unsafeWriteInt(po.start());
        }
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
        writeDecimal(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(@Nullable BigDecimal val) throws BinaryObjectException {
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

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeString(val);
    }

    /** {@inheritDoc} */
    @Override public void writeString(@Nullable String val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            byte[] strArr;

            if (BinaryUtils.USE_STR_SERIALIZATION_VER_2)
                strArr = BinaryUtils.strToUtf8Bytes(val);
            else
                strArr = val.getBytes(UTF_8);

            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.STRING);
            out.unsafeWriteInt(strArr.length);

            out.writeByteArray(strArr);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeUuid(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(@Nullable UUID val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 8 + 8);
            out.unsafeWriteByte(GridBinaryMarshaller.UUID);
            out.unsafeWriteLong(val.getMostSignificantBits());
            out.unsafeWriteLong(val.getLeastSignificantBits());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeDate(String fieldName, @Nullable Date val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDate(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(@Nullable Date val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 8);
            out.unsafeWriteByte(GridBinaryMarshaller.DATE);
            out.unsafeWriteLong(val.getTime());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeTimestamp(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(@Nullable Timestamp val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 8 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.TIMESTAMP);
            out.unsafeWriteLong(val.getTime());
            out.unsafeWriteInt(val.getNanos() % 1000000);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeTime(String fieldName, @Nullable Time val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeTime(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTime(@Nullable Time val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 8);
            out.unsafeWriteByte(GridBinaryMarshaller.TIME);
            out.unsafeWriteLong(val.getTime());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeObject(obj);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(@Nullable Object obj) throws BinaryObjectException {
        if (obj == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, out, schema, handles());

            writer.failIfUnregistered(failIfUnregistered);

            writer.marshal(obj);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeObjectDetached(@Nullable Object obj) throws BinaryObjectException {
        if (obj == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryWriterExImpl writer = new BinaryWriterExImpl(ctx, out, schema, null);

            writer.failIfUnregistered(failIfUnregistered);

            writer.marshal(obj);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeByteArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(@Nullable byte[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.BYTE_ARR);
            out.unsafeWriteInt(val.length);

            out.writeByteArray(val);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeShortArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(@Nullable short[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.SHORT_ARR);
            out.unsafeWriteInt(val.length);

            out.writeShortArray(val);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeIntArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(@Nullable int[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.INT_ARR);
            out.unsafeWriteInt(val.length);

            out.writeIntArray(val);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeLongArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(@Nullable long[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.LONG_ARR);
            out.unsafeWriteInt(val.length);

            out.writeLongArray(val);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeFloatArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(@Nullable float[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.FLOAT_ARR);
            out.unsafeWriteInt(val.length);

            out.writeFloatArray(val);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDoubleArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(@Nullable double[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.DOUBLE_ARR);
            out.unsafeWriteInt(val.length);

            out.writeDoubleArray(val);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeCharArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(@Nullable char[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.CHAR_ARR);
            out.unsafeWriteInt(val.length);

            out.writeCharArray(val);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeBooleanArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(@Nullable boolean[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.BOOLEAN_ARR);
            out.unsafeWriteInt(val.length);

            out.writeBooleanArray(val);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(String fieldName, @Nullable BigDecimal[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDecimalArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(@Nullable BigDecimal[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.DECIMAL_ARR);
            out.unsafeWriteInt(val.length);

            for (BigDecimal str : val)
                writeDecimal(str);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeStringArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(@Nullable String[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.STRING_ARR);
            out.unsafeWriteInt(val.length);

            for (String str : val)
                writeString(str);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeUuidArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(@Nullable UUID[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.UUID_ARR);
            out.unsafeWriteInt(val.length);

            for (UUID uuid : val)
                writeUuid(uuid);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(String fieldName, @Nullable Date[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeDateArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(@Nullable Date[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.DATE_ARR);
            out.unsafeWriteInt(val.length);

            for (Date date : val)
                writeDate(date);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeTimestampArray(String fieldName, @Nullable Timestamp[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeTimestampArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestampArray(@Nullable Timestamp[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.TIMESTAMP_ARR);
            out.unsafeWriteInt(val.length);

            for (Timestamp ts : val)
                writeTimestamp(ts);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeTimeArray(String fieldName, @Nullable Time[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeTimeArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeTimeArray(@Nullable Time[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.TIME_ARR);
            out.unsafeWriteInt(val.length);

            for (Time time : val)
                writeTime(time);
        }
    }

     /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeObjectArray(val);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(@Nullable Object[] val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            if (tryWriteAsHandle(val))
                return;

            BinaryClassDescriptor desc = ctx.registerClass(
                val.getClass().getComponentType(),
                true,
                failIfUnregistered);

            out.unsafeEnsure(1 + 4);
            out.unsafeWriteByte(GridBinaryMarshaller.OBJ_ARR);

            if (desc.registered())
                out.unsafeWriteInt(desc.typeId());
            else {
                out.unsafeWriteInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);

                writeString(val.getClass().getComponentType().getName());
            }

            out.writeInt(val.length);

            for (Object obj : val)
                writeObject(obj);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeCollection(col);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(@Nullable Collection<T> col) throws BinaryObjectException {
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
                writeObject(obj);
        }
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map)
        throws BinaryObjectException {
        writeFieldId(fieldName);
        writeMap(map);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(@Nullable Map<K, V> map) throws BinaryObjectException {
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
                writeObject(e.getKey());
                writeObject(e.getValue());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws BinaryObjectException {
        writeFieldId(fieldName);
        writeEnum(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(T val) throws BinaryObjectException {
        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryClassDescriptor desc = ctx.registerClass(val.getDeclaringClass(), true, failIfUnregistered);

            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.ENUM);

            if (desc.registered())
                out.unsafeWriteInt(desc.typeId());
            else {
                out.unsafeWriteInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);
                writeString(val.getDeclaringClass().getName());
            }

            out.writeInt(val.ordinal());
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws BinaryObjectException {
        writeFieldId(fieldName);
        doWriteEnumArray(val);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(T[] val) throws BinaryObjectException {
        doWriteEnumArray(val);
    }

    /**
     * @param val Array.
     */
    void doWriteEnumArray(@Nullable Object[] val) {
        assert val == null || val.getClass().getComponentType().isEnum();

        if (val == null)
            out.writeByte(GridBinaryMarshaller.NULL);
        else {
            BinaryClassDescriptor desc = ctx.registerClass(
                val.getClass().getComponentType(),
                true,
                failIfUnregistered);

            out.unsafeEnsure(1 + 4);

            out.unsafeWriteByte(GridBinaryMarshaller.ENUM_ARR);

            if (desc.registered())
                out.unsafeWriteInt(desc.typeId());
            else {
                out.unsafeWriteInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);

                writeString(val.getClass().getComponentType().getName());
            }

            out.writeInt(val.length);

            // TODO: Denis: Redundant data for each element of the array.
            for (Object o : val)
                writeEnum((Enum<?>)o);
        }
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
    @Override public void writeBytes(String s) throws IOException {
        int len = s.length();

        writeInt(len);

        for (int i = 0; i < len; i++)
            writeByte(s.charAt(i));
    }

    /** {@inheritDoc} */
    @Override public void writeChars(String s) throws IOException {
        int len = s.length();

        writeInt(len);

        for (int i = 0; i < len; i++)
            writeChar(s.charAt(i));
    }

    /** {@inheritDoc} */
    @Override public void writeUTF(String s) throws IOException {
        writeString(s);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(int v) throws IOException {
        out.writeByte((byte)v);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(int v) throws IOException {
        out.writeShort((short)v);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(int v) throws IOException {
        out.writeChar((char)v);
    }

    /** {@inheritDoc} */
    @Override public void write(int b) throws IOException {
        out.writeByte((byte)b);
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

    /** {@inheritDoc} */
    @Override public void writeFieldId(int fieldId) {
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

    /** {@inheritDoc} */
    @Override public int writeByteArray(InputStream in, int limit) throws BinaryObjectException {
        if (limit != -1)
            out.unsafeEnsure(1 + 4 + limit);
        else
            out.unsafeEnsure(1 + 4);

        out.unsafeWriteByte(GridBinaryMarshaller.BYTE_ARR);

        int lengthPos = out.position();

        out.position(lengthPos + 4);

        int written = 0;

        byte[] buf = new byte[limit > 0 ? Math.min(limit, DEFAULT_BUFFER_SIZE) : DEFAULT_BUFFER_SIZE];

        while (limit == -1 || written < limit) {
            int read;
            try {
                read = limit > 0
                        ? in.read(buf, 0, Math.min(buf.length, limit - written))
                        : in.read(buf, 0, buf.length);
            }
            catch (IOException e) {
                throw new BinaryObjectException(e);
            }

            if (read == -1)
                break;

            if (read + written > MAX_ARRAY_SIZE)
                throw new BinaryObjectException("Too much data. Can't write more then " + MAX_ARRAY_SIZE + " bytes from stream.");

            out.writeByteArray(buf, 0, read);

            written += read;
        }

        out.position(lengthPos);
        out.unsafeWriteInt(written);
        out.position(out.position() + written);

        return written;
    }

    /**
     * @param schemaId Schema ID.
     */
    public void schemaId(int schemaId) {
        this.schemaId = schemaId;
    }

    /** {@inheritDoc} */
    @Override public int schemaId() {
        return schemaId;
    }

    /** {@inheritDoc} */
    @Override public BinarySchema currentSchema() {
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

    /** {@inheritDoc} */
    @Override public BinaryWriterEx newWriter(int typeId) {
        BinaryWriterExImpl res = new BinaryWriterExImpl(ctx, out, schema, handles());

        res.failIfUnregistered(failIfUnregistered);

        res.typeId(typeId);

        return res;
    }

    /** {@inheritDoc} */
    @Override public BinaryContext context() {
        return ctx;
    }
}
