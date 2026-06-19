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

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.TestCollectionsMessage;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestCollectionsMessageMarshaller implements MessageMarshaller<TestCollectionsMessage> {
    /** */
    public TestCollectionsMessageMarshaller() {

    }

    /** */
    @Override public void prepareMarshal(TestCollectionsMessage msg, GridKernalContext kctx, GridCacheContext<?, ?> nested) throws IgniteCheckedException {
        GridCacheContext<?, ?> ctx = nested;

        if (msg.messageList != null) {
            for (GridCacheVersion e2 : (Collection<? extends GridCacheVersion>)msg.messageList) {
                if (e2 != null)
                    MessageMarshaller.prepareMarshal(kctx.messageFactory(), e2, kctx, ctx);
            }
        }

        if (msg.cacheObjectSet != null) {
            for (CacheObject e2 : (Collection<? extends CacheObject>)msg.cacheObjectSet) {
                if (e2 != null && ctx != null)
                    e2.prepareMarshal(ctx.cacheObjectContext());
            }
        }
    }

    /** */
    @Override public void finishUnmarshal(TestCollectionsMessage msg, GridKernalContext kctx, GridCacheContext<?, ?> nested, ClassLoader clsLdr) throws IgniteCheckedException {
        GridCacheContext<?, ?> ctx = nested;

        if (msg.messageList != null) {
            for (GridCacheVersion e2 : (Collection<? extends GridCacheVersion>)msg.messageList) {
                if (e2 != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e2, kctx, ctx, clsLdr);
            }
        }

        if (msg.cacheObjectSet != null) {
            for (CacheObject e2 : (Collection<? extends CacheObject>)msg.cacheObjectSet) {
                if (e2 != null && ctx != null)
                    e2.finishUnmarshal(ctx.cacheObjectContext(), clsLdr);
            }
        }
    }

    /** */
    @Override public void finishUnmarshal(TestCollectionsMessage msg, GridKernalContext kctx) throws IgniteCheckedException {
        if (msg.messageList != null) {
            for (GridCacheVersion e2 : (Collection<? extends GridCacheVersion>)msg.messageList) {
                if (e2 != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e2, kctx);
            }
        }
    }
}
