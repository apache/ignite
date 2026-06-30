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

import java.lang.Boolean;
import java.lang.Byte;
import java.lang.Character;
import java.lang.Double;
import java.lang.Float;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Short;
import java.lang.String;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.TestMapMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
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
    @Override public void prepareMarshal(TestMapMessage msg, GridKernalContext kctx, CacheObjectContext nested) throws IgniteCheckedException {
        CacheObjectContext ctx = nested;

        if (msg.messageBoxedDoubleMap != null) {
            for (GridCacheVersion e : ((Collection<? extends GridCacheVersion>)msg.messageBoxedDoubleMap.keySet())) {
                if (e != null)
                    MessageMarshaller.prepareMarshal(kctx.messageFactory(), e, kctx, ctx);
            }
        }

        if (msg.gridCacheObjectMap != null) {
            for (KeyCacheObject e : ((Collection<? extends KeyCacheObject>)msg.gridCacheObjectMap.keySet())) {
                if (e != null && ctx != null)
                    e.prepareMarshal(ctx);
            }
            for (Map e : ((Collection<? extends Map>)msg.gridCacheObjectMap.values())) {
                if (e != null) {
                    for (List e1 : ((Collection<? extends List>)e.values())) {
                        if (e1 != null) {
                            for (CacheObject e2 : (Collection<? extends CacheObject>)e1) {
                                if (e2 != null && ctx != null)
                                    e2.prepareMarshal(ctx);
                            }
                        }
                    }
                }
            }
        }
    }

    /** */
    @Override public void finishUnmarshal(TestMapMessage msg, GridKernalContext kctx, CacheObjectContext nested, ClassLoader clsLdr) throws IgniteCheckedException {
        CacheObjectContext ctx = nested;

        if (msg.messageBoxedDoubleMap != null) {
            for (GridCacheVersion e : ((Collection<? extends GridCacheVersion>)msg.messageBoxedDoubleMap.keySet())) {
                if (e != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e, kctx, ctx, clsLdr);
            }
        }

        if (msg.gridCacheObjectMap != null) {
            for (KeyCacheObject e : ((Collection<? extends KeyCacheObject>)msg.gridCacheObjectMap.keySet())) {
                if (e != null && ctx != null)
                    e.finishUnmarshal(ctx, clsLdr);
            }
            for (Map e : ((Collection<? extends Map>)msg.gridCacheObjectMap.values())) {
                if (e != null) {
                    for (List e1 : ((Collection<? extends List>)e.values())) {
                        if (e1 != null) {
                            for (CacheObject e2 : (Collection<? extends CacheObject>)e1) {
                                if (e2 != null && ctx != null)
                                    e2.finishUnmarshal(ctx, clsLdr);
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
            for (GridCacheVersion e : ((Collection<? extends GridCacheVersion>)msg.messageBoxedDoubleMap.keySet())) {
                if (e != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e, kctx);
            }
        }
    }
}