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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.io.*;
import java.nio.*;

import static org.apache.ignite.marshaller.optimized.OptimizedObjectOutputStream.*;

/**
 * Cache object implementation for classes that support footer injection is their serialized form thus enabling fields
 * search and extraction without necessity to fully deserialize an object.
 */
public class CacheIndexedObjectImpl extends CacheObjectAdapter implements CacheIndexedObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    private CacheObjectContext ctx;

    /** */
    protected int start;

    /** */
    protected int len;

    /**
     * For {@link Externalizable}.
     */
    public CacheIndexedObjectImpl() {
       // No-op
    }

    /**
     * Instantiates {@code CacheIndexedObjectImpl} with object.
     * @param val Object.
     */
    public CacheIndexedObjectImpl(CacheObjectContext ctx, Object val) {
        this(ctx, val, null, 0, 0);
    }

    /**
     * Instantiates {@code CacheIndexedObjectImpl} with object's serialized form.
     * @param valBytes Object serialized to byte array.
     * @param start Object's start in the array.
     * @param len Object's len in the array.
     */
    public CacheIndexedObjectImpl(CacheObjectContext ctx, byte[] valBytes, int start, int len) {
        this(ctx, null, valBytes, start, len);
    }

    /**
     * Instantiates {@code CacheIndexedObjectImpl} with object's serialized form and value.
     * @param val Object.
     * @param valBytes Object serialized to byte array.
     */
    public CacheIndexedObjectImpl(CacheObjectContext ctx, Object val, byte[] valBytes) {
        this(ctx, val, valBytes, 0, valBytes != null ? valBytes.length : 0);
    }

    /**
     * Instantiates {@code CacheIndexedObjectImpl}.
     * @param val Object.
     * @param valBytes Object in a serialized form.
     * @param start Object's start in the array.
     * @param len Object's len in the array.
     */
    public CacheIndexedObjectImpl(CacheObjectContext ctx, Object val, byte[] valBytes, int start, int len) {
        assert val != null || (valBytes != null && start >= 0 && len > 0);

        this.ctx = ctx;
        this.val = val;
        this.valBytes = valBytes;
        this.start = start;
        this.len = len;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T value(CacheObjectContext ctx, boolean cpy) {
        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectContext ctx) throws IgniteCheckedException {
        toMarshaledFormIfNeeded();

        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert valBytes != null;

        this.ctx = ctx;

        if (val == null && keepDeserialized(ctx, true))
            val = ctx.processor().unmarshal(ctx, valBytes, start, len, ldr);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        toMarshaledFormIfNeeded();
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        // refer to GridIoMessageFactory.
        return 113;
    }

    /** {@inheritDoc} */
    @Override public byte type() {
        return TYPE_OPTIMIZED;
    }

    /**
     * Returns object's type ID.
     *
     * @return Type ID.
     */
    @Override public int typeId() {
        assert valBytes != null;

        int typeId = UNSAFE.getInt(valBytes, BYTE_ARR_OFF + start + 1);

        if (typeId == 0)
            throw new IgniteException("Object's type ID wasn't written to cache.");

        return typeId;
    }

    /**
     * Checks whether a wrapped object has field with name {@code fieldName}.
     *
     * @param fieldName Field name.
     * @return {@code true} if has.
     * @throws IgniteException In case of error.
     */
    @Override public boolean hasField(String fieldName) {
        assert valBytes != null;

        try {
            OptimizedMarshaller marsh = (OptimizedMarshaller)ctx.kernalContext().config().getMarshaller();

            return marsh.hasField(fieldName, valBytes, start, len);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Searches and returns field if it exists.
     *
     * @param fieldName Field name.
     * @return Field.
     * @throws IgniteException In case of error.
     */
    @Override public <T> T field(String fieldName) {
        assert valBytes != null;

        try {
            OptimizedMarshaller marsh = (OptimizedMarshaller)ctx.kernalContext().config().getMarshaller();

            return marsh.readField(fieldName, valBytes, start, len,
                val != null ? val.getClass().getClassLoader() : null, ctx);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T deserialize() {
        if (val != null)
            return (T)val;

        try {
            assert valBytes != null;

            Object val = ctx.processor().unmarshal(ctx, valBytes, start, len,
                ctx.kernalContext().defaultClassLoader());

            if (keepDeserialized(ctx, false))
                this.val = val;

            return (T)val;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to unmarshall object.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(start);
        out.writeInt(len);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        start = in.readInt();
        len = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 1:
                len = reader.readInt("len");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                start = reader.readInt("start");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 1:
                if (!writer.writeInt("len", len))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("start", start))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        if (val != null)
            return val.hashCode();
        else {
            assert valBytes != null;

            return UNSAFE.getInt(valBytes, BYTE_ARR_OFF + start + len -
                FOOTER_LENGTH_FIELD_SIZE - FOOTER_HANDLES_FIELD_SIZE - FOOTER_OBJECT_HASH_CODE_FIELD_SIZE);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof CacheIndexedObjectImpl))
            return false;

        CacheIndexedObjectImpl other = (CacheIndexedObjectImpl)obj;

        try {
            if (val != null && other.val != null)
                return F.eq(val, other.val);
            else {
                toMarshaledFormIfNeeded();

                other.toMarshaledFormIfNeeded();

                if (len != other.len)
                    return false;

                for (int i = 0; i < len; i++) {
                    if (valBytes[start + i] != other.valBytes[other.start + i])
                        return false;
                }

                return true;
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Marshals {@link #val} to {@link #valBytes} if needed.
     *
     * @throws IgniteCheckedException In case of error.
     */
    protected void toMarshaledFormIfNeeded() throws IgniteCheckedException {
        if (valBytes == null) {
            assert val != null;

            valBytes = ctx.processor().marshal(ctx, val);

            start = 0;
            len = valBytes.length;

            if (!keepDeserialized(ctx, false))
                val = null;
        }
    }

    /**
     * Checks whether to keep deserialized version of the object or not.
     *
     * @param ctx Cache object context.
     * @param checkCls Check class definition presence on node flag.
     * @return {@code true} if keep, {@code false} otherwise.
     */
    @SuppressWarnings("SimplifiableIfStatement")
    protected boolean keepDeserialized(CacheObjectContext ctx, boolean checkCls) {
        if (ctx.copyOnGet())
            return false;

        return !checkCls || ctx.processor().hasClass(typeId());
    }
}
