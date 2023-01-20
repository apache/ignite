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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.spi.transform.CacheObjectTransformer;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_TRANSFORMED;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TRANSFORMED;
import static org.apache.ignite.spi.transform.CacheObjectTransformer.OVERHEAD;

/** */
public class CacheObjectTransformerUtils {
    /** Version. */
    private static final byte VER = 0;

    /***/
    private static CacheObjectTransformer transformer(CacheObjectValueContext ctx) {
        return ctx.kernalContext().config().getCacheObjectTransformer();
    }

    /**
     * Transforms bytes according to {@link CacheObjectTransformer} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, CacheObjectValueContext ctx) {
        return transformIfNecessary(bytes, 0, bytes.length, ctx);
    }

    /**
     * Transforms bytes according to {@link CacheObjectTransformer} when specified.
     * @param bytes Given bytes.
     * @param ctx Context.
     * @return Transformed bytes.
     */
    public static byte[] transformIfNecessary(byte[] bytes, int offset, int length, CacheObjectValueContext ctx) {
        assert bytes[offset] != TRANSFORMED;

        try {
            CacheObjectTransformer transformer = transformer(ctx);

            if (transformer == null)
                return bytes;

            byte[] transformed = transformer.transform(bytes, offset, length);

            transformed[0] = TRANSFORMED;
            transformed[1] = VER;

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

        CacheObjectTransformer transformer = transformer(ctx);

        byte transformed = bytes[0];
        byte ver = bytes[1];

        assert transformed == TRANSFORMED;

        byte[] restored;

        if (ver == 0) {
            int offset = 1 /*transformed*/ + 1 /*ver*/;

            assert offset == OVERHEAD : offset; // Correct while VER == 0;

            restored = transformer.restore(bytes, offset, bytes.length - offset);
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
