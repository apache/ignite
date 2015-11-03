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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.portable.streams.PortableOffheapInputStream;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryField;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

import static org.apache.ignite.internal.portable.GridPortableMarshaller.BOOLEAN;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.BYTE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.CHAR;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.DOUBLE;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.FLOAT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.INT;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.LONG;
import static org.apache.ignite.internal.portable.GridPortableMarshaller.SHORT;

/**
 *  Portable object implementation over offheap memory
 */
public class BinaryObjectOffheapImpl extends BinaryObjectEx implements Externalizable, CacheObject {
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
        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx,
            new PortableOffheapInputStream(ptr, size, false),
            start,
            null);

        return reader.createSchema();
    }

    /** {@inheritDoc} */
    @Override public BinaryField fieldDescriptor(String fieldName) throws BinaryObjectException {
        int typeId = typeId();

        PortableSchemaRegistry schemaReg = ctx.schemaRegistry(typeId);

        int fieldId = ctx.userTypeIdMapper(typeId).fieldId(typeId, fieldName);

        return new BinaryFieldImpl(schemaReg, fieldId);
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
    @Nullable @Override public BinaryType metaData() throws BinaryObjectException {
        if (ctx == null)
            throw new BinaryObjectException("PortableContext is not set for the object.");

        return ctx.metaData(typeId());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <F> F field(String fieldName) throws BinaryObjectException {
        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx,
            new PortableOffheapInputStream(ptr, size, false),
            start,
            null);

        return (F)reader.unmarshalField(fieldName);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <F> F field(int fieldId) throws BinaryObjectException {
        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx,
            new PortableOffheapInputStream(ptr, size, false),
            start,
            null);

        return (F)reader.unmarshalField(fieldId);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override protected <F> F fieldByOrder(int order) {
        Object val;

        // Calculate field position.
        int schemaOffset = PortablePrimitives.readInt(ptr, start + GridPortableMarshaller.SCHEMA_OR_RAW_OFF_POS);

        short flags = PortablePrimitives.readShort(ptr, start + GridPortableMarshaller.FLAGS_POS);
        int fieldOffsetSize = PortableUtils.fieldOffsetSize(flags);

        int fieldOffsetPos = start + schemaOffset + order * (4 + fieldOffsetSize) + 4;

        int fieldPos;

        if (fieldOffsetSize == PortableUtils.OFFSET_1)
            fieldPos = start + (int)PortablePrimitives.readByte(ptr, fieldOffsetPos) & 0xFF;
        else if (fieldOffsetSize == PortableUtils.OFFSET_2)
            fieldPos = start + (int)PortablePrimitives.readShort(ptr, fieldOffsetPos) & 0xFFFF;
        else
            fieldPos = start + PortablePrimitives.readInt(ptr, fieldOffsetPos);

        // Read header and try performing fast lookup for well-known types (the most common types go first).
        byte hdr = PortablePrimitives.readByte(ptr, fieldPos);

        switch (hdr) {
            case INT:
                val = PortablePrimitives.readInt(ptr, fieldPos + 1);

                break;

            case LONG:
                val = PortablePrimitives.readLong(ptr, fieldPos + 1);

                break;

            case BOOLEAN:
                val = PortablePrimitives.readBoolean(ptr, fieldPos + 1);

                break;

            case SHORT:
                val = PortablePrimitives.readShort(ptr, fieldPos + 1);

                break;

            case BYTE:
                val = PortablePrimitives.readByte(ptr, fieldPos + 1);

                break;

            case CHAR:
                val = PortablePrimitives.readChar(ptr, fieldPos + 1);

                break;

            case FLOAT:
                val = PortablePrimitives.readFloat(ptr, fieldPos + 1);

                break;

            case DOUBLE:
                val = PortablePrimitives.readDouble(ptr, fieldPos + 1);

                break;

            default: {
                PortableReaderExImpl reader = new PortableReaderExImpl(ctx,
                    new PortableOffheapInputStream(ptr, size, false),
                    start,
                    null);

                val = reader.unmarshalFieldByAbsolutePosition(fieldPos);
            }
        }

        return (F)val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override protected <F> F field(PortableReaderContext rCtx, String fieldName) {
        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx,
            new PortableOffheapInputStream(ptr, size, false),
            start,
            null,
            rCtx);

        return (F)reader.unmarshalField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(String fieldName) {
        BinaryReaderExImpl reader = new BinaryReaderExImpl(ctx,
            new PortableOffheapInputStream(ptr, size, false),
            start,
            null);

        return reader.hasField(fieldName);
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
    @Override public byte type() {
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
        // TODO: IGNITE-1272 - Deserialize with proper class loader.
        BinaryReaderExImpl reader = new BinaryReaderExImpl(
            ctx,
            new PortableOffheapInputStream(ptr, size, false),
            start,
            null);

        return reader.deserialize();
    }
}