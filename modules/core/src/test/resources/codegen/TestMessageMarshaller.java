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

package org.apache.ignite.internal;

import java.lang.String;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.TestMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestMessageMarshaller implements MessageMarshaller<TestMessage> {
    /** */
    public TestMessageMarshaller() {

    }

    /** */
    @Override public void prepareMarshal(TestMessage msg, GridKernalContext kctx, GridCacheContext<?, ?> nested) throws IgniteCheckedException {
        GridCacheContext<?, ?> ctx = nested;

        if (msg.ver != null)
            MessageMarshaller.prepareMarshal(kctx.messageFactory(), msg.ver, kctx, ctx);

        if (msg.verArr != null) {
            for (GridCacheVersion e3 : msg.verArr) {
                if (e3 != null)
                    MessageMarshaller.prepareMarshal(kctx.messageFactory(), e3, kctx, ctx);
            }
        }

        if (msg.keyCacheObject != null && ctx != null)
            msg.keyCacheObject.prepareMarshal(ctx.cacheObjectContext());

        if (msg.cacheObject != null && ctx != null)
            msg.cacheObject.prepareMarshal(ctx.cacheObjectContext());
    }

    /** */
    @Override public void finishUnmarshal(TestMessage msg, GridKernalContext kctx, GridCacheContext<?, ?> nested, ClassLoader clsLdr) throws IgniteCheckedException {
        GridCacheContext<?, ?> ctx = nested;

        if (msg.ver != null)
            MessageMarshaller.finishUnmarshal(kctx.messageFactory(), msg.ver, kctx, ctx, clsLdr);

        if (msg.verArr != null) {
            for (GridCacheVersion e3 : msg.verArr) {
                if (e3 != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e3, kctx, ctx, clsLdr);
            }
        }

        if (msg.keyCacheObject != null && ctx != null)
            msg.keyCacheObject.finishUnmarshal(ctx.cacheObjectContext(), clsLdr);

        if (msg.cacheObject != null && ctx != null)
            msg.cacheObject.finishUnmarshal(ctx.cacheObjectContext(), clsLdr);
    }

    /** */
    @Override public void finishUnmarshal(TestMessage msg, GridKernalContext kctx) throws IgniteCheckedException {
        if (msg.ver != null)
            MessageMarshaller.finishUnmarshal(kctx.messageFactory(), msg.ver, kctx);

        if (msg.verArr != null) {
            for (GridCacheVersion e3 : msg.verArr) {
                if (e3 != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e3, kctx);
            }
        }
    }
}