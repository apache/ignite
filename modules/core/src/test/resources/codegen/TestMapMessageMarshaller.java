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

import java.lang.Double;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.TestMapMessage;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestMapMessageMarshaller implements MessageMarshaller<TestMapMessage> {
    /** */
    public TestMapMessageMarshaller() {

    }

    /** */
    @Override public void prepareMarshal(TestMapMessage msg, GridKernalContext kctx, GridCacheContext<?, ?> nested) throws IgniteCheckedException {
        GridCacheContext<?, ?> ctx = nested;

        if (msg.messageBoxedDoubleMap != null) {
            for (GridCacheVersion e3 : ((Collection<? extends GridCacheVersion>)msg.messageBoxedDoubleMap.keySet())) {
                if (e3 != null)
                    MessageMarshaller.prepareMarshal(kctx.messageFactory(), e3, kctx, ctx);
            }
        }

        if (msg.gridCacheObjectMap != null) {
            for (KeyCacheObject e3 : ((Collection<? extends KeyCacheObject>)msg.gridCacheObjectMap.keySet())) {
                if (e3 != null && ctx != null)
                    e3.prepareMarshal(ctx.cacheObjectContext());
            }
            for (Map e3 : ((Collection<? extends Map>)msg.gridCacheObjectMap.values())) {
                if (e3 != null) {
                    for (List e5 : ((Collection<? extends List>)e3.values())) {
                        if (e5 != null) {
                            for (CacheObject e6 : (Collection<? extends CacheObject>)e5) {
                                if (e6 != null && ctx != null)
                                    e6.prepareMarshal(ctx.cacheObjectContext());
                            }
                        }
                    }
                }
            }
        }
    }

    /** */
    @Override public void finishUnmarshal(TestMapMessage msg, GridKernalContext kctx, GridCacheContext<?, ?> nested, ClassLoader clsLdr) throws IgniteCheckedException {
        GridCacheContext<?, ?> ctx = nested;

        if (msg.messageBoxedDoubleMap != null) {
            for (GridCacheVersion e3 : ((Collection<? extends GridCacheVersion>)msg.messageBoxedDoubleMap.keySet())) {
                if (e3 != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e3, kctx, ctx, clsLdr);
            }
        }

        if (msg.gridCacheObjectMap != null) {
            for (KeyCacheObject e3 : ((Collection<? extends KeyCacheObject>)msg.gridCacheObjectMap.keySet())) {
                if (e3 != null && ctx != null)
                    e3.finishUnmarshal(ctx.cacheObjectContext(), clsLdr);
            }
            for (Map e3 : ((Collection<? extends Map>)msg.gridCacheObjectMap.values())) {
                if (e3 != null) {
                    for (List e5 : ((Collection<? extends List>)e3.values())) {
                        if (e5 != null) {
                            for (CacheObject e6 : (Collection<? extends CacheObject>)e5) {
                                if (e6 != null && ctx != null)
                                    e6.finishUnmarshal(ctx.cacheObjectContext(), clsLdr);
                            }
                        }
                    }
                }
            }
        }
    }

    /** */
    @Override public void finishUnmarshal(TestMapMessage msg, GridKernalContext kctx) throws IgniteCheckedException {
        if (msg.messageBoxedDoubleMap != null) {
            for (GridCacheVersion e3 : ((Collection<? extends GridCacheVersion>)msg.messageBoxedDoubleMap.keySet())) {
                if (e3 != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e3, kctx);
            }
        }
    }
}