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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TRANSFORMED;

/**
 *
 */
public class TransformableBinaryObject extends CacheObjectAdapter implements BinaryObjectEx {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public TransformableBinaryObject() {
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public TransformableBinaryObject(BinaryObjectEx val, byte[] valBytes) {
        assert val != null || (valBytes != null && transformed(valBytes));

        assert !(val instanceof TransformableBinaryObject);

        this.val = val;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T value(CacheObjectValueContext ctx, boolean cpy) {
        return value(ctx, cpy, null);
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T value(CacheObjectValueContext ctx, boolean cpy, ClassLoader ldr) {
        cpy = cpy && needCopy(ctx);

        try {
            GridKernalContext kernalCtx = ctx.kernalContext();

            IgniteCacheObjectProcessor proc = ctx.kernalContext().cacheObjects();

            if (cpy) {
                if (valBytes == null) {
                    assert val != null;

                    valBytes = bytes(ctx);
                }

                if (ldr == null) {
                    if (val != null)
                        ldr = val.getClass().getClassLoader();
                    else if (kernalCtx.config().isPeerClassLoadingEnabled())
                        ldr = kernalCtx.cache().context().deploy().globalLoader();
                }

                return (T)proc.unmarshal(ctx, valBytes, ldr);
            }

            if (val != null)
                return (T)val;

            assert valBytes != null;

            Object val = proc.unmarshal(ctx, valBytes, kernalCtx.config().isPeerClassLoadingEnabled() ?
                kernalCtx.cache().context().deploy().globalLoader() : null);

            assert !(val instanceof TransformableBinaryObject);

            if (storeValue(ctx))
                this.val = val;

            return (T)val;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to unmarshall object.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
        if (valBytes == null)
            valBytes = bytes(ctx);

        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public boolean isPlatformType() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
        assert val != null || valBytes != null;

        if (valBytes == null)
            valBytes = bytes(ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert val != null || valBytes != null;

        if (val == null && storeValue(ctx)) {
            val = ctx.kernalContext().cacheObjects().unmarshal(ctx, valBytes, ldr);

            ((CacheObject)val).finishUnmarshal(ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public byte cacheObjectType() {
        if (!transformed(valBytes))
            return ((CacheObject)val).cacheObjectType();
        else
            return CacheObject.TYPE_BINARY_TRANSFORMER;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        if (!transformed(valBytes))
            return ((Message)val).directType();
        else
            return 190;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (!transformed(valBytes))
            return ((Message)val).writeTo(buf, writer);
        else
            return super.writeTo(buf, writer);
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        ((Message)val).onAckReceived();
    }

    /** {@inheritDoc} */
    @Override public BinaryType type() throws BinaryObjectException {
        return ((BinaryObject)val).type();
    }

    /** {@inheritDoc} */
    @Override public <F> F field(String fieldName) throws BinaryObjectException {
        return ((BinaryObject)val).field(fieldName);
    }

    /** {@inheritDoc} */
    @Override public boolean hasField(String fieldName) {
        return ((BinaryObject)val).hasField(fieldName);
    }

    /** {@inheritDoc} */
    @Override public <T> T deserialize() throws BinaryObjectException {
        return ((BinaryObject)val).deserialize();
    }

    /** {@inheritDoc} */
    @Override public <T> T deserialize(ClassLoader ldr) throws BinaryObjectException {
        return ((BinaryObject)val).deserialize(ldr);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject clone() throws CloneNotSupportedException {
        return (BinaryObject)super.clone();
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder toBuilder() throws BinaryObjectException {
        return ((BinaryObject)val).toBuilder();
    }

    /** {@inheritDoc} */
    @Override public int enumOrdinal() throws BinaryObjectException {
        return ((BinaryObject)val).enumOrdinal();
    }

    /** {@inheritDoc} */
    @Override public String enumName() throws BinaryObjectException {
        return ((BinaryObject)val).enumName();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return ((BinaryObject)val).size();
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return ((BinaryObjectEx)val).typeId();
    }

    /** {@inheritDoc} */
    @Override public @Nullable BinaryType rawType() throws BinaryObjectException {
        return ((BinaryObjectEx)val).rawType();
    }

    /** {@inheritDoc} */
    @Override public boolean isFlagSet(short flag) {
        return ((BinaryObjectEx)val).isFlagSet(flag);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        if (!transformed(valBytes))
            ((Externalizable)val).writeExternal(out);
        else
            super.writeExternal(out);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TransformableBinaryObject obj = (TransformableBinaryObject)o;

        return val.equals(obj.val);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return val.hashCode();
    }

    /***/
    private byte[] bytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
        byte[] bytes = ctx.kernalContext().cacheObjects().marshal(ctx, val);

        if (!transformed(bytes))
            return ((CacheObject)val).valueBytes(ctx);
        else
            return bytes;
    }

    /***/
    protected boolean storeValue(CacheObjectValueContext ctx) {
        return ctx.storeValue();
    }

    /***/
    protected boolean transformed(byte[] bytes) {
        return bytes[0] == TRANSFORMED;
    }
}
