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
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.internal.cache.transform.CacheObjectTransformerProcessor;
import org.apache.ignite.internal.util.GridUnsafe;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_TRANSFORMED;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TRANSFORMED;

/** */
public class CacheObjectTransformerUtils {
    /** */
    private static CacheObjectTransformerProcessor transformer(CacheObjectValueContext ctx) {
        return ctx.kernalContext().transformer();
    }

    /**
     * Transforms bytes according to {@link CacheObjectTransformerProcessor} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, CacheObjectValueContext ctx) {
        return transformIfNecessary(bytes, 0, bytes.length, ctx);
    }

    /**
     * Transforms bytes according to {@link CacheObjectTransformerProcessor} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, int offset, int length, CacheObjectValueContext ctx) {
        assert bytes[offset] != TRANSFORMED;

        CacheObjectTransformerProcessor transformer = transformer(ctx);

        if (transformer == null)
            return bytes;

        ByteBuffer src = ByteBuffer.wrap(bytes, offset, length);
        ByteBuffer transformed = transformer.transform(src);

        if (transformed != null) {
            assert transformed.remaining() > 0 : transformed.remaining();

            byte[] res = toArray(transformed);

            if (recordable(ctx, EVT_CACHE_OBJECT_TRANSFORMED)) {
                ctx.kernalContext().event().record(
                    new CacheObjectTransformedEvent(ctx.kernalContext().discovery().localNode(),
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

            if (recordable(ctx, EVT_CACHE_OBJECT_TRANSFORMED)) {
                ctx.kernalContext().event().record(
                    new CacheObjectTransformedEvent(ctx.kernalContext().discovery().localNode(),
                        "Object transformation was cancelled.",
                        EVT_CACHE_OBJECT_TRANSFORMED,
                        res,
                        res,
                        false));
            }

            return res;
        }
    }

    /**
     *
     */
    private static byte[] detachIfNecessary(byte[] bytes, int offset, int length) {
        if (offset == 0 && length == bytes.length)
            return bytes;

        byte[] res = new byte[length];

        GridUnsafe.arrayCopy(bytes, offset, res, 0, length);

        return res;
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

        CacheObjectTransformerProcessor transformer = transformer(ctx);

        ByteBuffer src = ByteBuffer.wrap(bytes, 1, bytes.length - 1); // Skipping TRANSFORMED.
        ByteBuffer restored = transformer.restore(src);

        byte[] res = toArray(restored);

        if (recordable(ctx, EVT_CACHE_OBJECT_TRANSFORMED)) {
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
     * @param ctx Context.
     * @param type Type.
     */
    private static boolean recordable(CacheObjectValueContext ctx, int type) {
        return ctx.kernalContext().event() != null // Can be null at external usage (via StandaloneGridKernalContext)
            && ctx.kernalContext().event().isRecordable(type);
    }
}
