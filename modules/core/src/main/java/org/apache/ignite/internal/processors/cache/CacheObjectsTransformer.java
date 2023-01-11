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
import org.apache.ignite.internal.ThreadLocalDirectByteBuffer;
import org.apache.ignite.spi.transform.CacheObjectTransformerSpi;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_TRANSFORMED;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TRANSFORMED;
import static org.apache.ignite.spi.transform.CacheObjectTransformerSpi.OVERHEAD;

/** */
public class CacheObjectsTransformer {
    /** Header buffer. */
    private static final ThreadLocalDirectByteBuffer hdrBuf = new ThreadLocalDirectByteBuffer();

    /** Version. */
    private static final byte VER = 0;

    /***/
    private static CacheObjectTransformerSpi spi(CacheObjectValueContext ctx) {
        return ctx.kernalContext().config().getCacheObjectTransformSpi();
    }

    /**
     * Transforms bytes according to {@link CacheObjectTransformerSpi} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, CacheObjectValueContext ctx) {
        return transformIfNecessary(bytes, 0, bytes.length, ctx);
    }

    /**
     * Transforms bytes according to {@link CacheObjectTransformerSpi} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, int offset, int length, CacheObjectValueContext ctx) {
        assert bytes[offset] != TRANSFORMED;

        try {
            CacheObjectTransformerSpi spi = spi(ctx);

            if (spi == null)
                return bytes;

            byte[] transformed = spi.transform(bytes, offset, length);

            ByteBuffer hdr = hdrBuf.get(OVERHEAD);

            hdr.put(TRANSFORMED);
            hdr.put(VER);
            hdr.putInt(bytes.length);
            hdr.flip();

            hdr.get(transformed, 0, hdr.remaining());

            if (ctx.kernalContext().event().isRecordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
                ctx.kernalContext().event().record(
                    new CacheObjectTransformedEvent(ctx.kernalContext().discovery().localNode(),
                        "Object transformed",
                        EVT_CACHE_OBJECT_TRANSFORMED,
                        bytes,
                        transformed,
                        false));
            }

            return transformed;
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

        CacheObjectTransformerSpi spi = spi(ctx);

        ByteBuffer hdr = ByteBuffer.wrap(bytes);

        byte transformed = hdr.get();
        byte ver = hdr.get();

        assert transformed == TRANSFORMED;

        byte[] restored;

        if (ver == 0) {
            int length = hdr.getInt();
            int offset = 1 /*transformed*/ + 1 /*ver*/ + 4 /*length*/;

            assert offset == OVERHEAD : offset; // Correct while VER == 0;

            restored = spi.restore(bytes, offset, length);
        }
        else
            throw new IllegalStateException("Unknown version " + ver);

        if (ctx.kernalContext().event().isRecordable(EVT_CACHE_OBJECT_TRANSFORMED)) {
            ctx.kernalContext().event().record(
                new CacheObjectTransformedEvent(ctx.kernalContext().discovery().localNode(),
                    "Object restored",
                    EVT_CACHE_OBJECT_TRANSFORMED,
                    restored,
                    bytes,
                    true));
        }

        return restored;
    }
}
