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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.binary.streams.BinaryOffheapInputStream;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 *  Binary object implementation over offheap memory
 */
public class BinaryObjectOffheapImpl extends BinaryObjectExImpl implements Externalizable, CacheObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final BinaryContext ctx;

    /** */
    private final long ptr;

    /** */
    private final int start;

    /** */
    private final int size;

    /**
     * For {@link Externalizable} (not supported).
     */
    public BinaryObjectOffheapImpl() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param ctx Context.
     * @param ptr Memory address.
     * @param start Object start.
     * @param size Memory size.
     */
    public BinaryObjectOffheapImpl(BinaryContext ctx, long ptr, int start, int size) {
        this.ctx = ctx;
        this.ptr = ptr;
        this.start = start;
        this.size = size;
    }

    /**
     * @return Heap-based copy.
     */
    public BinaryObject heapCopy() {
        return new BinaryObjectImpl(ctx, U.copyMemory(ptr, size), start);
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        int typeId = BinaryPrimitives.readInt(ptr, start + GridBinaryMarshaller.TYPE_ID_POS);

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            int off = start + GridBinaryMarshaller.DFLT_HDR_LEN;

            String clsName = BinaryUtils.doReadClassName(new BinaryOffheapInputStream(ptr + off, size));

            typeId = ctx.typeId(clsName);
        }

        return typeId;
    }

    /** {@inheritDoc} */
    @Override public int length() {
        return BinaryPrimitives.readInt(ptr, start + GridBinaryMarshaller.TOTAL_LEN_POS);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return BinaryPrimitives.readInt(ptr, start + GridBinaryMarshaller.HASH_CODE_POS);
    }

    /** {@inheritDoc} */
    @Override public boolean hasSchema() {
        short flags = BinaryPrimitives.readShort(ptr, start + GridBinaryMarshaller.FLAGS_POS);

        return BinaryUtils.hasSchema(flags);
    }

    /** {@inheritDoc} */
    @Override public int schemaId() {
        return BinaryPrimitives.readInt(ptr, start + GridBinaryMarshaller.SCHEMA_ID_POS);
    }

    /** {@inheritDoc} */
    @Override public BinarySchema createSchema() {
        return reader(null, false).getOrCreateSchema();
    }

    /** {@inheritDoc} */
    @Override public BinaryContext context() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public int start() {
        return start;
    }

    /** {@inheritDoc} */
    @Override public byte[] array() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean putValue(ByteBuffer buf) throws IgniteCheckedException {
        throw new UnsupportedOperationException("TODO implement");
    }

    /** {@inheritDoc} */
    @Override public int putValue(long addr) throws IgniteCheckedException {
        throw new UnsupportedOperationException("TODO implement");
    }

    /** {@inheritDoc} */
    @Override public boolean putValue(final ByteBuffer buf, final int off, final int len)
        throws IgniteCheckedException {
        throw new UnsupportedOperationException("TODO implement");
    }

    /** {@inheritDoc} */
    @Override public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException {
        throw new UnsupportedOperationException("TODO implement");
    }

    /** {@inheritDoc} */
    @Override public long offheapAddress() {
        return ptr;
    }

    /** {@inheritDoc} */
    @Override public boolean hasArray() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isFlagSet(short flag) {
        short flags = BinaryPrimitives.readShort(ptr, start + GridBinaryMarshaller.FLAGS_POS);

        return BinaryUtils.isFlagSet(flags, flag);
    }

    /** {@inheritDoc} */
    @Nullable @Override public BinaryType type() throws BinaryObjectException {
        return BinaryUtils.typeProxy(ctx, this);
    }

    /** {@inheritDoc} */
    @Nullable @Override public BinaryType rawType() throws BinaryObjectException {
        return BinaryUtils.type(ctx, this);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <F> F field(String fieldName) throws BinaryObjectException {
        return (F) reader(null, false).unmarshalField(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <F> F field(int fieldId) throws BinaryObjectException {
        return (F) reader(null, false).unmarshalField(fieldId);
    }

    /** {@inheritDoc} */
    @Override public BinarySerializedFieldComparator createFieldComparator() {
        int schemaOff = BinaryPrimitives.readInt(ptr, start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);

        short flags = BinaryPrimitives.readShort(ptr, start + GridBinaryMarshaller.FLAGS_POS);

        int fieldIdLen = BinaryUtils.isCompactFooter(flags) ? 0 : BinaryUtils.FIELD_ID_LEN;
        int fieldOffLen = BinaryUtils.fieldOffsetLength(flags);

        int orderBase = start + schemaOff + fieldIdLen;
        int orderMultiplier = fieldIdLen + fieldOffLen;

        return new BinarySerializedFieldComparator(this, null, ptr, start, orderBase, orderMultiplier, fieldOffLen);
    }

    /** {@inheritDoc} */
    @Override public int dataStartOffset() {
        int typeId = BinaryPrimitives.readInt(ptr, start + GridBinaryMarshaller.TYPE_ID_POS);

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            int len = BinaryPrimitives.readInt(ptr, start + GridBinaryMarshaller.DFLT_HDR_LEN + 1);

            return start + GridBinaryMarshaller.DFLT_HDR_LEN + len + 5;
        } else
            return start + GridBinaryMarshaller.DFLT_HDR_LEN;
    }

    /** {@inheritDoc} */
    @Override public int footerStartOffset() {
        short flags = BinaryPrimitives.readShort(ptr, start + GridBinaryMarshaller.FLAGS_POS);

        if (!BinaryUtils.hasSchema(flags))
            return start + length();

        return start + BinaryPrimitives.readInt(ptr, start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <F> F fieldByOrder(int order) {
        if (order == BinarySchema.ORDER_NOT_FOUND)
            return null;

        Object val;

        // Calculate field position.
        int schemaOff = BinaryPrimitives.readInt(ptr, start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);

        short flags = BinaryPrimitives.readShort(ptr, start + GridBinaryMarshaller.FLAGS_POS);

        int fieldIdLen = BinaryUtils.isCompactFooter(flags) ? 0 : BinaryUtils.FIELD_ID_LEN;
        int fieldOffLen = BinaryUtils.fieldOffsetLength(flags);

        int fieldOffsetPos = start + schemaOff + order * (fieldIdLen + fieldOffLen) + fieldIdLen;

        int fieldPos;

        if (fieldOffLen == BinaryUtils.OFFSET_1)
            fieldPos = start + ((int)BinaryPrimitives.readByte(ptr, fieldOffsetPos) & 0xFF);
        else if (fieldOffLen == BinaryUtils.OFFSET_2)
            fieldPos = start + ((int)BinaryPrimitives.readShort(ptr, fieldOffsetPos) & 0xFFFF);
        else
            fieldPos = start + BinaryPrimitives.readInt(ptr, fieldOffsetPos);

        // Read header and try performing fast lookup for well-known types (the most common types go first).
        byte hdr = BinaryPrimitives.readByte(ptr, fieldPos);

        switch (hdr) {
            case GridBinaryMarshaller.INT:
                val = BinaryPrimitives.readInt(ptr, fieldPos + 1);

                break;

            case GridBinaryMarshaller.LONG:
                val = BinaryPrimitives.readLong(ptr, fieldPos + 1);

                break;

            case GridBinaryMarshaller.BOOLEAN:
                val = BinaryPrimitives.readBoolean(ptr, fieldPos + 1);

                break;

            case GridBinaryMarshaller.SHORT:
                val = BinaryPrimitives.readShort(ptr, fieldPos + 1);

                break;

            case GridBinaryMarshaller.BYTE:
                val = BinaryPrimitives.readByte(ptr, fieldPos + 1);

                break;

            case GridBinaryMarshaller.CHAR:
                val = BinaryPrimitives.readChar(ptr, fieldPos + 1);

                break;

            case GridBinaryMarshaller.FLOAT:
                val = BinaryPrimitives.readFloat(ptr, fieldPos + 1);

                break;

            case GridBinaryMarshaller.DOUBLE:
                val = BinaryPrimitives.readDouble(ptr, fieldPos + 1);

                break;

            case GridBinaryMarshaller.STRING: {
                int dataLen = BinaryPrimitives.readInt(ptr, fieldPos + 1);
                byte[] data = BinaryPrimitives.readByteArray(ptr, fieldPos + 5, dataLen);

                val = new String(data, UTF_8);

                break;
            }

            case GridBinaryMarshaller.DATE: {
                long time = BinaryPrimitives.readLong(ptr, fieldPos + 1);

                val = new Date(time);

                break;
            }

            case GridBinaryMarshaller.TIMESTAMP: {
                long time = BinaryPrimitives.readLong(ptr, fieldPos + 1);
                int nanos = BinaryPrimitives.readInt(ptr, fieldPos + 1 + 8);

                Timestamp ts = new Timestamp(time);

                ts.setNanos(ts.getNanos() + nanos);

                val = ts;

                break;
            }

            case GridBinaryMarshaller.TIME: {
                long time = BinaryPrimitives.readLong(ptr, fieldPos + 1);

                val = new Time(time);

                break;
            }

            case GridBinaryMarshaller.UUID: {
                long most = BinaryPrimitives.readLong(ptr, fieldPos + 1);
                long least = BinaryPrimitives.readLong(ptr, fieldPos + 1 + 8);

                val = new UUID(most, least);

                break;
            }

            case GridBinaryMarshaller.DECIMAL: {
                int scale = BinaryPrimitives.readInt(ptr, fieldPos + 1);

                int dataLen = BinaryPrimitives.readInt(ptr, fieldPos + 5);
                byte[] data = BinaryPrimitives.readByteArray(ptr, fieldPos + 9, dataLen);

                boolean negative = data[0] < 0;

                if (negative)
                    data[0] &= 0x7F;

                BigInteger intVal = new BigInteger(data);

                if (negative)
                    intVal = intVal.negate();

                val = new BigDecimal(intVal, scale);

                break;
            }

            case GridBinaryMarshaller.NULL:
                val = null;

                break;

            default:
                BinaryOffheapInputStream stream = new BinaryOffheapInputStream(ptr, size, false);

                stream.position(fieldPos);

                val = BinaryUtils.unmarshal(stream, ctx, null);

                break;
        }

        return (F)val;
    }

    /** {@inheritDoc} */
    @Override protected boolean writeFieldByOrder(int fieldOffset, ByteBuffer buf) {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected <F> F field(BinaryReaderHandles rCtx, String fieldName) {
        return (F)reader(rCtx, false).unmarshalField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(String fieldName) {
        return reader(null, false).findFieldByName(fieldName);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T deserialize() throws BinaryObjectException {
        return (T)deserializeValue();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return heapCopy();
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder toBuilder() throws BinaryObjectException {
        return BinaryObjectBuilderImpl.wrap(heapCopy());
    }

    /** {@inheritDoc} */
    @Override public byte cacheObjectType() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isPlatformType() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T value(CacheObjectValueContext ctx, boolean cpy) {
        return (T)deserializeValue();
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException(); // To make sure it is not marshalled.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException(); // To make sure it is not marshalled.
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     * @return Deserialized value.
     */
    private Object deserializeValue() {
        return reader(null, true).deserialize();
    }

    /**
     * Create new reader for this object.
     *
     * @param rCtx Reader context.
     * @param forUnmarshal {@code True} if reader is needed to unmarshal object.
     * @return Reader.
     */
    private BinaryReaderExImpl reader(@Nullable BinaryReaderHandles rCtx, boolean forUnmarshal) {
        BinaryOffheapInputStream stream = new BinaryOffheapInputStream(ptr, size, false);

        stream.position(start);

        return new BinaryReaderExImpl(ctx,
            stream,
            ctx.configuration().getClassLoader(),
            rCtx,
            forUnmarshal);
    }
}
