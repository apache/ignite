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
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.io.*;
import java.nio.*;
import java.util.concurrent.*;

/**
 * Cache object implementation for classes that support footer injection is their serialized form thus enabling fields
 * search and extraction without necessity to fully deserialize an object.
 */
public class CacheIndexedObjectImpl extends CacheObjectAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

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
    public CacheIndexedObjectImpl(Object val) {
        this(val, null, 0, 0);
    }

    /**
     * Instantiates {@code CacheIndexedObjectImpl} with object's serialized form.
     * @param valBytes Object serialized to byte array.
     * @param start Object's start in the array.
     * @param len Object's len in the array.
     */
    public CacheIndexedObjectImpl(byte[] valBytes, int start, int len) {
        this(null, valBytes, start, len);
    }

    /**
     * Instantiates {@code CacheIndexedObjectImpl} with object's serialized form and value.
     * @param val Object.
     * @param valBytes Object serialized to byte array.
     */
    public CacheIndexedObjectImpl(Object val, byte[] valBytes) {
        this(val, valBytes, 0, valBytes != null ? valBytes.length : 0);
    }

    /**
     * Instantiates {@code CacheIndexedObjectImpl}.
     * @param val Object.
     * @param valBytes Object in a serialized form.
     * @param start Object's start in the array.
     * @param len Object's len in the array.
     */
    public CacheIndexedObjectImpl(Object val, byte[] valBytes, int start, int len) {
        assert val != null || (valBytes != null && start >= 0 && len > 0);

        if (valBytes != null && val != null)
            val = null;

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
        toMarshaledFormIfNeeded(ctx);

        shrinkToSize();

        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        return detach();
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        toMarshaledFormIfNeeded(ctx);
        shrinkToSize();
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
    public int typeId() {
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
     * @param marsh Marshaller.
     * @return {@code true} if has.
     * @throws IgniteCheckedException In case of error.
     */
    public boolean hasField(String fieldName, OptimizedMarshallerExt marsh) throws IgniteCheckedException {
        assert valBytes != null;

        return marsh.hasField(fieldName, valBytes, start, len);
    }

    /**
     * Searches and returns field if it exists.
     *
     * @param fieldName Field name.
     * @param marsh Marshaller.
     * @return Field.
     * @throws IgniteFieldNotFoundException In case if there is no such a field.
     * @throws IgniteCheckedException In case of error.
     */
    public Object field(String fieldName, OptimizedMarshallerExt marsh) throws IgniteCheckedException {
        assert valBytes != null;

        return marsh.readField(fieldName, valBytes, start, len, val != null ? val.getClass().getClassLoader() : null);
    }

    /**
     * Deserializes wrapped object.
     *
     * @param ctx Cache context.
     * @return Deserialized object.
     */
    public Object deserialize(CacheObjectContext ctx) {
        if (val != null)
            return val;

        try {
            assert valBytes != null;

            Object val = ctx.processor().unmarshal(ctx, valBytes, start, len,
                ctx.kernalContext().config().getClassLoader());

            //if (ctx.storeValue())
            //    this.val = val;

            return val;
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
        assert false;

        return super.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        assert false;

        return super.equals(obj);
    }

    /**
     * Detaches object.
     *
     * @return Detached object wrapped by {@code CacheIndexedObjectImpl}.
     */
    protected CacheIndexedObjectImpl detach() {
        shrinkToSize();

        return this;
    }

    /**
     * Checks whether the object is already detached or not.
     *
     * @return {@code true} if detached.
     */
    protected boolean detached() {
        return true;
        //return start == 0 && len == valBytes.length;
    }

    /**
     * Marshals {@link #val} to {@link #valBytes} if needed.
     *
     * @param ctx Cache object context.
     * @throws IgniteCheckedException In case of error.
     */
    protected void toMarshaledFormIfNeeded(CacheObjectContext ctx) throws IgniteCheckedException {
        if (valBytes == null) {
            assert val != null;

            valBytes = ctx.processor().marshal(ctx, val);

            start = 0;
            len = valBytes.length;

            val = null;
        }
    }

    /**
     * Shrinks byte array to size boundaries.
     */
    private void shrinkToSize() {
        if (detached())
            return;

        byte[] arr = new byte[len];

        U.arrayCopy(valBytes, start, arr, 0, len);

        valBytes = arr;
        start = 0;
    }
}
