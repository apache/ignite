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
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.spi.transform.CacheObjectsTransformer;
import org.apache.ignite.spi.transform.CacheObjectsTransformerSpi;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_TRANSFORMED;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TRANSFORMED;

/** */
public class CacheObjectsTransformerUtils {
    /** Header buffer. */
    private static final ThreadLocalByteBuffer hdrBuf = new ThreadLocalByteBuffer(CacheObjectsTransformer.OVERHEAD);

    /** Destination buffer. */
    private static final ThreadLocalByteBuffer dstBuf = new ThreadLocalByteBuffer(1 << 10);

    /** Source buffer. */
    private static final ThreadLocalByteBuffer srcBuf = new ThreadLocalByteBuffer(1 << 10);

    /** Version. */
    private static final byte VER = 0;

    /***/
    private static CacheObjectsTransformer transformer(CacheObjectValueContext ctx) {
        CacheObjectsTransformerSpi spi = ctx.kernalContext().config().getCacheObjectsTransformSpi();

        return (spi == null) ? null : spi.transformer(ctx.cacheConfiguration());
    }

    /**
     * Transforms bytes according to {@link CacheObjectsTransformerSpi} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, CacheObjectValueContext ctx) {
        return transformIfNecessary(bytes, 0, bytes.length, ctx);
    }

    /**
     * Transforms bytes according to {@link CacheObjectsTransformerSpi} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, int offset, int length, CacheObjectValueContext ctx) {
        assert bytes[offset] != TRANSFORMED;

        try {
            CacheObjectsTransformer trans = transformer(ctx);

            if (trans == null)
                return bytes;

            ByteBuffer src = sourceByteBuffer(bytes, offset, length, trans.direct());
            ByteBuffer transformed = dstBuf.get();

            while (true) {
                int capacity = trans.transform(src, transformed);

                if (capacity <= 0)
                    break;

                transformed = dstBuf.get(capacity);
            }

            ByteBuffer hdr = hdrBuf.get();

            hdr.put(TRANSFORMED);
            hdr.put(VER);
            hdr.putInt(bytes.length);
            hdr.flip();

            byte[] res = new byte[hdr.remaining() + transformed.remaining()];

            hdr.get(res, 0, hdr.remaining());
            transformed.get(res, CacheObjectsTransformer.OVERHEAD, transformed.remaining());

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
            if (ctx.kernalContext().event().isRecordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
                ctx.kernalContext().event().record(
                    new CacheObjectTransformedEvent(ctx.kernalContext().discovery().localNode(),
                        "Object transformation was cancelled. " + ex.getMessage(),
                        EVT_CACHE_OBJECT_TRANSFORMED,
                        bytes,
                        null,
                        false));
            }

            return bytes;
        }
    }

    /**
     * Restores transformed bytes if necessary.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Restored bytes.
     */
    public static byte[] restoreIfNecessary(byte[] bytes, CacheObjectValueContext ctx) {
        if (bytes[0] != TRANSFORMED)
            return bytes;

        CacheObjectsTransformer trans = transformer(ctx);

        ByteBuffer src = sourceByteBuffer(bytes, 0, bytes.length, trans.direct());

        byte transformed = src.get();
        byte ver = src.get();

        assert transformed == TRANSFORMED;
        assert ver == VER; // Correct while VER == 0;

        int length = src.getInt();

        ByteBuffer restored = dstBuf.get(length);

        restored.limit(length);

        trans.restore(src, restored);

        byte[] res = new byte[length];

        restored.get(res);

        if (ctx.kernalContext().event().isRecordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
            ctx.kernalContext().event().record(
                new CacheObjectTransformedEvent(ctx.kernalContext().discovery().localNode(),
                    "Object restored",
                    EVT_CACHE_OBJECT_TRANSFORMED,
                    res,
                    bytes,
                    true));
        }

        return res;
    }

    /***/
    private static ByteBuffer sourceByteBuffer(byte[] bytes, int offset, int length, boolean direct) {
        ByteBuffer src;

        if (direct) {
            src = srcBuf.get(bytes.length);

            src.put(bytes, offset, length);
            src.flip();
        }
        else
            src = ByteBuffer.wrap(bytes, offset, length);

        return src;
    }

    /***/
    private static final class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
        /***/
        final int size;

        /***/
        ThreadLocalByteBuffer(int size) {
            this.size = size;
        }

        /** {@inheritDoc} */
        @Override protected ByteBuffer initialValue() {
            return allocateDirectBuffer(size);
        }

        /***/
        public ByteBuffer get(int capacity) {
            ByteBuffer buf = super.get();

            if (buf.capacity() < capacity) {
                buf = allocateDirectBuffer(capacity);

                set(buf);
            }
            else
                buf.clear();

            return buf;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer get() {
            ByteBuffer buf = super.get();

            buf.clear();

            return buf;
        }
    }

    /***/
    private static ByteBuffer allocateDirectBuffer(int cap) {
        return ByteBuffer.allocateDirect(cap);
    }
}
