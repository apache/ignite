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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_TRANSFORMED;

/**
 *
 */
public class TransformedCacheObject extends CacheObjectAdapter implements BinaryObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache object type. */
    @GridDirectTransient
    private byte cacheObjType = CacheObject.TYPE_TRANSFORMER;

    /** Direct type. */
    @GridDirectTransient
    private short directType = 120;

    /** Transformed. */
    @GridDirectTransient
    private boolean transformed = true;

    /**
     * Default constructor.
     */
    public TransformedCacheObject() {
    }

    /**
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public TransformedCacheObject(CacheObject val, byte[] valBytes) {
        assert val != null || valBytes != null;

        assert !(val instanceof TransformedCacheObject);

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

//            if (cpy) {
//                if (valBytes == null) {
//                    assert val != null;
//
//                    valBytes = proc.marshal(ctx, val);
//                }
//
//                if (ldr == null) {
//                    if (val != null)
//                        ldr = val.getClass().getClassLoader();
//                    else if (kernalCtx.config().isPeerClassLoadingEnabled())
//                        ldr = kernalCtx.cache().context().deploy().globalLoader();
//                }
//
//                return (T)proc.unmarshal(ctx, valBytes, ldr);
//            }

            if (val != null)
                return (T)val;

            assert valBytes != null;

            Object val = proc.unmarshal(ctx, restore(ctx), kernalCtx.config().isPeerClassLoadingEnabled() ?
                kernalCtx.cache().context().deploy().globalLoader() : null);

            assert !(val instanceof TransformedCacheObject);

            if (ctx.storeValue())
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
            valBytes = transform(((CacheObject)val).valueBytes(ctx), ctx);

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
            valBytes = valueBytes(ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert val != null || valBytes != null;

        if (val == null && ctx.storeValue())
            val = ctx.kernalContext().cacheObjects().unmarshal(ctx, restore(ctx), ldr);
    }

    /** {@inheritDoc} */
    @Override public byte cacheObjectType() {
        return cacheObjType;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return directType;
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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        if (transformed)
            return super.writeTo(buf, writer);
        else
            return ((Message)val).writeTo(buf, writer); // Writing the original message.
    }

    /**
     *
     */
    public byte[] transform(byte[] bytes, CacheObjectValueContext ctx) throws IgniteCheckedException {
        try {
            byte[] res = ctx.cacheObjectsTransformationConfiguration().getActiveTransformer().transform(bytes);

            if (ctx.kernalContext().event().isRecordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
                ctx.kernalContext().event().record(
                    new CacheObjectTransformedEvent(ctx.kernalContext().discovery().localNode(),
                        "Object transformed",
                        EVT_CACHE_OBJECT_TRANSFORMED,
                        bytes,
                        res,
                        false));
            }

            return res;
        }
        catch (IgniteCheckedException ex) { // Can not be transformed.
            cacheObjType = ((CacheObject)val).cacheObjectType();
            directType = ((Message)val).directType();
            transformed = false;

            if (ctx.kernalContext().event().isRecordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
                ctx.kernalContext().event().record(
                    new CacheObjectTransformedEvent(ctx.kernalContext().discovery().localNode(),
                        "Object transformation was cancelled",
                        EVT_CACHE_OBJECT_TRANSFORMED,
                        bytes,
                        null,
                        false));
            }

            return bytes;
        }
    }

    /**
     *
     */
    public byte[] restore(CacheObjectValueContext ctx) throws IgniteCheckedException {
        byte[] res = ctx.cacheObjectsTransformationConfiguration().getActiveTransformer().restore(valBytes);

        if (ctx.kernalContext().event().isRecordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
            ctx.kernalContext().event().record(
                new CacheObjectTransformedEvent(ctx.kernalContext().discovery().localNode(),
                    "Object restored",
                    EVT_CACHE_OBJECT_TRANSFORMED,
                    res,
                    valBytes,
                    true));
        }

        return res;
    }
}
