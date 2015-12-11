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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.streams.PortableOffheapInputStream;
import org.apache.ignite.internal.binary.streams.PortableOffheapInputStream;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.BOOLEAN;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.BYTE;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.CHAR;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.DATE;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.DECIMAL;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.DOUBLE;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.FLOAT;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.INT;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.LONG;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.NULL;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.SHORT;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.STRING;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.TIMESTAMP;
import static org.apache.ignite.internal.binary.GridPortableMarshaller.UUID;

/**
 *  Portable object implementation over offheap memory
 */
public class BinaryObjectOffheapImpl extends BinaryObjectExImpl implements Externalizable, CacheObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private final PortableContext ctx;

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
    public BinaryObjectOffheapImpl(PortableContext ctx, long ptr, int start, int size) {
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
        return UNSAFE.getInt(ptr + start + GridPortableMarshaller.TYPE_ID_POS);
    }

    /** {@inheritDoc} */
    @Override public int length() {
        return UNSAFE.getInt(ptr + start + GridPortableMarshaller.TOTAL_LEN_POS);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return UNSAFE.getInt(ptr + start + GridPortableMarshaller.HASH_CODE_POS);
    }

    /** {@inheritDoc} */
    @Override protected int schemaId() {
        return UNSAFE.getInt(ptr + start + GridPortableMarshaller.SCHEMA_ID_POS);
    }

    /** {@inheritDoc} */
    @Override protected PortableSchema createSchema() {
        return reader(null).getOrCreateSchema();
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
    @Override public long offheapAddress() {
        return ptr;
    }

    /** {@inheritDoc} */
    @Override protected boolean hasArray() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public BinaryType type() throws BinaryObjectException {
        if (ctx == null)
            throw new BinaryObjectException("PortableContext is not set for the object.");

        return ctx.metadata(typeId());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <F> F field(String fieldName) throws BinaryObjectException {
        return (F) reader(null).unmarshalField(fieldName);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <F> F field(int fieldId) throws BinaryObjectException {
        return (F) reader(null).unmarshalField(fieldId);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override protected <F> F fieldByOrder(int order) {
        Object val;

        // Calculate field position.
        int schemaOffset = PortablePrimitives.readInt(ptr, start + GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS);

        short flags = PortablePrimitives.readShort(ptr, start + GridPortableMarshaller.FLAGS_POS);

        int fieldIdLen = PortableUtils.isCompactFooter(flags) ? 0 : PortableUtils.FIELD_ID_LEN;
        int fieldOffsetLen = PortableUtils.fieldOffsetLength(flags);

        int fieldOffsetPos = start + schemaOffset + order * (fieldIdLen + fieldOffsetLen) + fieldIdLen;

        int fieldPos;

        if (fieldOffsetLen == PortableUtils.OFFSET_1)
            fieldPos = start + ((int)PortablePrimitives.readByte(ptr, fieldOffsetPos) & 0xFF);
        else if (fieldOffsetLen == PortableUtils.OFFSET_2)
            fieldPos = start + ((int)PortablePrimitives.readShort(ptr, fieldOffsetPos) & 0xFFFF);
        else
            fieldPos = start + PortablePrimitives.readInt(ptr, fieldOffsetPos);

        // Read header and try performing fast lookup for well-known types (the most common types go first).
        byte hdr = PortablePrimitives.readByte(ptr, fieldPos);

        switch (hdr) {
            case GridPortableMarshaller.INT:
                val = PortablePrimitives.readInt(ptr, fieldPos + 1);

                break;

            case GridPortableMarshaller.LONG:
                val = PortablePrimitives.readLong(ptr, fieldPos + 1);

                break;

            case GridPortableMarshaller.BOOLEAN:
                val = PortablePrimitives.readBoolean(ptr, fieldPos + 1);

                break;

            case GridPortableMarshaller.SHORT:
                val = PortablePrimitives.readShort(ptr, fieldPos + 1);

                break;

            case GridPortableMarshaller.BYTE:
                val = PortablePrimitives.readByte(ptr, fieldPos + 1);

                break;

            case GridPortableMarshaller.CHAR:
                val = PortablePrimitives.readChar(ptr, fieldPos + 1);

                break;

            case GridPortableMarshaller.FLOAT:
                val = PortablePrimitives.readFloat(ptr, fieldPos + 1);

                break;

            case GridPortableMarshaller.DOUBLE:
                val = PortablePrimitives.readDouble(ptr, fieldPos + 1);

                break;

            case GridPortableMarshaller.STRING: {
                int dataLen = PortablePrimitives.readInt(ptr, fieldPos + 1);
                byte[] data = PortablePrimitives.readByteArray(ptr, fieldPos + 5, dataLen);

                val = new String(data, UTF_8);

                break;
            }

            case GridPortableMarshaller.DATE: {
                long time = PortablePrimitives.readLong(ptr, fieldPos + 1);

                val = new Date(time);

                break;
            }

            case GridPortableMarshaller.TIMESTAMP: {
                long time = PortablePrimitives.readLong(ptr, fieldPos + 1);
                int nanos = PortablePrimitives.readInt(ptr, fieldPos + 1 + 8);

                Timestamp ts = new Timestamp(time);

                ts.setNanos(ts.getNanos() + nanos);

                val = ts;

                break;
            }

            case GridPortableMarshaller.UUID: {
                long most = PortablePrimitives.readLong(ptr, fieldPos + 1);
                long least = PortablePrimitives.readLong(ptr, fieldPos + 1 + 8);

                val = new UUID(most, least);

                break;
            }

            case GridPortableMarshaller.DECIMAL: {
                int scale = PortablePrimitives.readInt(ptr, fieldPos + 1);

                int dataLen = PortablePrimitives.readInt(ptr, fieldPos + 5);
                byte[] data = PortablePrimitives.readByteArray(ptr, fieldPos + 9, dataLen);

                BigInteger intVal = new BigInteger(data);

                if (scale < 0) {
                    scale &= 0x7FFFFFFF;

                    intVal = intVal.negate();
                }

                val = new BigDecimal(intVal, scale);

                break;
            }

            case GridPortableMarshaller.NULL:
                val = null;

                break;

            default:
                PortableOffheapInputStream stream = new PortableOffheapInputStream(ptr, size, false);

                stream.position(fieldPos);

                val = PortableUtils.unmarshal(stream, ctx, null);

                break;
        }

        return (F)val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override protected <F> F field(BinaryReaderHandles rCtx, String fieldName) {
        return (F)reader(rCtx).unmarshalField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(String fieldName) {
        return reader(null).findFieldByName(fieldName);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T deserialize() throws BinaryObjectException {
        return (T)deserializeValue();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return heapCopy();
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
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
        return (T)deserializeValue();
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectContext ctx) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
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
    @Override public byte directType() {
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

    /**
     * @return Deserialized value.
     */
    private Object deserializeValue() {
        return reader(null).deserialize();
    }

    /**
     * Create new reader for this object.
     *
     * @param rCtx Reader context.
     * @return Reader.
     */
    private BinaryReaderExImpl reader(@Nullable BinaryReaderHandles rCtx) {
        PortableOffheapInputStream stream = new PortableOffheapInputStream(ptr, size, false);

        stream.position(start);

        return new BinaryReaderExImpl(ctx,
            stream,
            ctx.configuration().getClassLoader(),
            rCtx);
    }
}
