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
import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.TestMarshalledMapMessage;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestMarshalledMapMessageMarshaller implements MessageMarshaller<TestMarshalledMapMessage> {
    /** */
    public TestMarshalledMapMessageMarshaller() {
    }

    /** */
    @Override public void prepareMarshal(TestMarshalledMapMessage msg, GridKernalContext kctx, CacheObjectContext nested) throws IgniteCheckedException {
        CacheObjectContext ctx = nested;

        if (msg.theMap != null && msg.mapKeys == null) {
            msg.mapKeys = msg.theMap.keySet();
            msg.mapVals = msg.theMap.values();
        }

        if (msg.mapKeys != null) {
            for (GridCacheVersion e : (Collection<? extends GridCacheVersion>)msg.mapKeys) {
                if (e != null)
                    MessageMarshaller.prepareMarshal(kctx.messageFactory(), e, kctx, ctx);
            }
        }

        if (msg.mapVals != null) {
            for (GridCacheVersion e : (Collection<? extends GridCacheVersion>)msg.mapVals) {
                if (e != null)
                    MessageMarshaller.prepareMarshal(kctx.messageFactory(), e, kctx, ctx);
            }
        }
    }

    /** */
    @Override public void finishUnmarshal(TestMarshalledMapMessage msg, GridKernalContext kctx, CacheObjectContext nested, ClassLoader clsLdr) throws IgniteCheckedException {
        CacheObjectContext ctx = nested;

        if (msg.mapKeys != null) {
            msg.theMap = U.newHashMap(msg.mapKeys.size());

            Iterator<GridCacheVersion> keyIter = msg.mapKeys.iterator();
            Iterator<GridCacheVersion> valIter = msg.mapVals.iterator();

            while (keyIter.hasNext()) {
                GridCacheVersion k = keyIter.next();
                GridCacheVersion v = valIter.next();

                if (k != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), k, kctx, ctx, clsLdr);

                if (v != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), v, kctx, ctx, clsLdr);

                msg.theMap.put(k, v);
            }

            msg.mapKeys = null;
            msg.mapVals = null;
        }
    }

    /** */
    @Override public void finishUnmarshal(TestMarshalledMapMessage msg, GridKernalContext kctx) throws IgniteCheckedException {
        if (msg.mapKeys != null) {
            for (GridCacheVersion e : (Collection<? extends GridCacheVersion>)msg.mapKeys) {
                if (e != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e, kctx);
            }
        }

        if (msg.mapVals != null) {
            for (GridCacheVersion e : (Collection<? extends GridCacheVersion>)msg.mapVals) {
                if (e != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e, kctx);
            }
        }
    }
}