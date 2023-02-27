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
import org.apache.ignite.internal.cache.transform.CacheObjectTransformerManager;
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.internal.ThreadLocalDirectByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_TRANSFORMED;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TRANSFORMED;

/** */
public class CacheObjectTransformerUtils {
    /** Additional space required to store the transformed data. */
    public static final int OVERHEAD = 2;

    /** Source byte buffer. */
    private static final ThreadLocalDirectByteBuffer srcBuf = new ThreadLocalDirectByteBuffer();

    /** Version. */
    private static final byte VER = 0;

    /***/
    private static CacheObjectTransformerManager transformer(CacheObjectValueContext ctx) {
        return ctx.kernalContext().cache().context().transformer();
    }

    /**
     * Transforms bytes according to {@link CacheObjectTransformerManager} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, CacheObjectValueContext ctx) {
        return transformIfNecessary(bytes, 0, bytes.length, ctx);
    }

    /**
     * Transforms bytes according to {@link CacheObjectTransformerManager} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, int offset, int length, CacheObjectValueContext ctx) {
        assert bytes[offset] != TRANSFORMED;

        CacheObjectTransformerManager transformer = transformer(ctx);

        if (transformer == null)
            return bytes;

        ByteBuffer src = sourceByteBuffer(bytes, offset, length, transformer.direct());
        ByteBuffer transformed = transformer.transform(src);

        if (transformed != null) {
            assert transformed.remaining() > 0 : transformed.remaining();

            byte[] res = new byte[OVERHEAD + transformed.remaining()];

            if (transformed.isDirect())
                transformed.get(res, OVERHEAD, transformed.remaining());
            else {
                byte[] arr = transformed.array();

                U.arrayCopy(arr, 0, res, OVERHEAD, arr.length);
            }

            res[0] = TRANSFORMED;
            res[1] = VER;

            if (ctx.kernalContext().event().isRecordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
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

            if (ctx.kernalContext().event().isRecordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
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

        U.arrayCopy(bytes, offset, res, 0, length);

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

        byte transformed = bytes[0];
        byte ver = bytes[1];

        assert transformed == TRANSFORMED;

        int offset;

        if (ver == 0) {
            offset = 1 /*transformed*/ + 1 /*ver*/;

            assert offset == OVERHEAD : offset; // Correct while VER == 0;
        }
        else
            throw new IllegalStateException("Unknown version " + ver);

        CacheObjectTransformerManager transformer = transformer(ctx);

        ByteBuffer src = sourceByteBuffer(bytes, offset, bytes.length - offset, transformer.direct());
        ByteBuffer restored = transformer.restore(src);

        byte[] res;

        if (restored.isDirect()) {
            res = new byte[restored.remaining()];

            restored.get(res);
        }
        else
            res = restored.array();

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

    /** */
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
}
