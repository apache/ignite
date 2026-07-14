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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.TestCollectionsMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestCollectionsMessageMarshaller implements MessageMarshaller<TestCollectionsMessage> {
    /** */
    @Override public void marshal(TestCollectionsMessage msg, GridKernalContext kctx, CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        CacheObjectContext ctx = cacheObjCtx;

        if (msg.cacheObjectSet != null) {
            for (CacheObject e : (Collection<? extends CacheObject>)msg.cacheObjectSet) {
                if (e != null && ctx != null)
                    e.marshal(ctx);
            }
        }
    }

    /** */
    @Override public void unmarshal(TestCollectionsMessage msg, GridKernalContext kctx, CacheObjectContext cacheObjCtx, ClassLoader clsLdr) throws IgniteCheckedException {
        CacheObjectContext ctx = cacheObjCtx;

        if (msg.cacheObjectSet != null) {
            for (CacheObject e : (Collection<? extends CacheObject>)msg.cacheObjectSet) {
                if (e != null && ctx != null)
                    e.unmarshal(ctx, clsLdr);
            }
        }
    }
}