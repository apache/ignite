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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.TestMarshalledCollectionMessage;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;

/**
 * This class is generated automatically.
 *
 * @see org.apache.ignite.internal.MessageProcessor
 */
public class TestMarshalledCollectionMessageMarshaller implements MessageMarshaller<TestMarshalledCollectionMessage> {
    /** */
    public TestMarshalledCollectionMessageMarshaller() {
    }

    /** */
    @Override public void prepareMarshal(TestMarshalledCollectionMessage msg, GridKernalContext kctx, CacheObjectContext nested) throws IgniteCheckedException {
        CacheObjectContext ctx = nested;

        if (msg.keys != null && msg.keysArr == null)
            msg.keysArr = msg.keys.toArray(new GridCacheVersion[0]);

        if (msg.keysArr != null) {
            for (GridCacheVersion e : msg.keysArr) {
                if (e != null)
                    MessageMarshaller.prepareMarshal(kctx.messageFactory(), e, kctx, ctx);
            }
        }
    }

    /** */
    @Override public void finishUnmarshal(TestMarshalledCollectionMessage msg, GridKernalContext kctx, CacheObjectContext nested, ClassLoader clsLdr) throws IgniteCheckedException {
        CacheObjectContext ctx = nested;

        if (msg.keysArr != null) {
            msg.keys = U.newHashSet(msg.keysArr.length);

            for (GridCacheVersion e : msg.keysArr) {
                if (e != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e, kctx, ctx, clsLdr);

                msg.keys.add(e);
            }

            msg.keysArr = null;
        }
    }

    /** */
    @Override public void finishUnmarshal(TestMarshalledCollectionMessage msg, GridKernalContext kctx) throws IgniteCheckedException {
        if (msg.keysArr != null) {
            for (GridCacheVersion e : msg.keysArr) {
                if (e != null)
                    MessageMarshaller.finishUnmarshal(kctx.messageFactory(), e, kctx);
            }
        }
    }
}