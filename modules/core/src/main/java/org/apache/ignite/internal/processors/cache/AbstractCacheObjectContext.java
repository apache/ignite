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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.cache.transform.CacheObjectTransformerProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_TRANSFORMED;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TRANSFORMED;

/**
 * Abstract implementation of {@link CacheObjectValueContext}.
 */
public abstract class AbstractCacheObjectContext implements CacheObjectValueContext {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     */
    protected AbstractCacheObjectContext(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public BinaryContext binaryContext() {
        return ctx.cacheObjects().binaryContext();
    }

    /** {@inheritDoc} */
    @Override public void waitMetadataWriteIfNeeded(final int typeId) {
        ctx.cacheObjects().waitMetadataWriteIfNeeded(typeId);
    }

    /** {@inheritDoc} */
    @Override public @Nullable ClassLoader classLoader() {
        return ctx.config().getClassLoader();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader globalLoader() {
        return ctx.cache().context().deploy().globalLoader();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(Class<?> cls) {
        return ctx.log(cls);
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(Object val) throws IgniteCheckedException {
        return ctx.cacheObjects().marshal(this, val);
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException {
        return ctx.cacheObjects().unmarshal(this, bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public boolean isPeerClassLoadingEnabled() {
        return ctx.config().isPeerClassLoadingEnabled();
    }

    /** {@inheritDoc} */
    @Override public byte[] transformIfNecessary(byte[] bytes, int offset, int length) {
        assert bytes[offset] != TRANSFORMED;

        CacheObjectTransformerProcessor transformer = ctx.transformer();

        if (transformer == null)
            return bytes;

        ByteBuffer src = ByteBuffer.wrap(bytes, offset, length);
        ByteBuffer transformed = transformer.transform(src);

        if (transformed != null) {
            assert transformed.remaining() > 0 : transformed.remaining();

            byte[] res = toArray(transformed);

            if (recordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
                ctx.event().record(
                    new CacheObjectTransformedEvent(ctx.discovery().localNode(),
                        "Object transformed",
                        EVT_CACHE_OBJECT_TRANSFORMED,
                        detachIfNecessary(bytes, offset, length),
                        res,
                        false));
            }

            return res;
        }
        else {
            byte[] res = detachIfNecessary(bytes, offset, length);

            if (recordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
                ctx.event().record(
                    new CacheObjectTransformedEvent(ctx.discovery().localNode(),
                        "Object transformation was cancelled.",
                        EVT_CACHE_OBJECT_TRANSFORMED,
                        res,
                        res,
                        false));
            }

            return res;
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] restoreIfNecessary(byte[] bytes) {
        if (bytes[0] != TRANSFORMED)
            return bytes;

        CacheObjectTransformerProcessor transformer = ctx.transformer();

        ByteBuffer src = ByteBuffer.wrap(bytes, 1, bytes.length - 1); // Skipping TRANSFORMED.
        ByteBuffer restored = transformer.restore(src);

        byte[] res = toArray(restored);

        if (recordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
            ctx.event().record(
                new CacheObjectTransformedEvent(ctx.discovery().localNode(),
                    "Object restored",
                    EVT_CACHE_OBJECT_TRANSFORMED,
                    res,
                    bytes,
                    true));
        }

        return res;
    }

    /** */
    private static byte[] detachIfNecessary(byte[] bytes, int offset, int length) {
        if (offset == 0 && length == bytes.length)
            return bytes;

        byte[] res = new byte[length];

        GridUnsafe.arrayCopy(bytes, offset, res, 0, length);

        return res;
    }

    /**
     * @param buf Buffer.
     */
    private static byte[] toArray(ByteBuffer buf) {
        if (buf.isDirect()) {
            byte[] res = new byte[buf.remaining()];

            buf.get(res);

            return res;
        }
        else {
            if (buf.remaining() != buf.capacity())
                throw new IllegalStateException("Unexpected Heap Byte Buffer state. " +
                    "Wrapped array must contain the data without any offsets to avoid unnecessary copying. " +
                    "Position must be 0, limit must be equal to the capacity." +
                    " [buf=" + buf + "]");

            return buf.array();
        }
    }

    /**
     * @param type Type.
     */
    private boolean recordable(int type) {
        return ctx.event() != null // Can be null at external usage (via StandaloneGridKernalContext)
            && ctx.event().isRecordable(type);
    }
}
