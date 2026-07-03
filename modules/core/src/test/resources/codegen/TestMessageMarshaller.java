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
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
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
    @Override public void marshal(TestMessage msg, GridKernalContext kctx, CacheObjectContext nested) throws IgniteCheckedException {
        CacheObjectContext ctx = nested;

        if (msg.keyCacheObject != null && ctx != null)
            msg.keyCacheObject.marshal(ctx);

        if (msg.cacheObject != null && ctx != null)
            msg.cacheObject.marshal(ctx);
    }

    /** */
    @Override public void unmarshal(TestMessage msg, GridKernalContext kctx, CacheObjectContext nested, ClassLoader clsLdr) throws IgniteCheckedException {
        CacheObjectContext ctx = nested;

        if (msg.keyCacheObject != null && ctx != null)
            msg.keyCacheObject.unmarshal(ctx, clsLdr);

        if (msg.cacheObject != null && ctx != null)
            msg.cacheObject.unmarshal(ctx, clsLdr);
    }

    /** */
    @Override public void unmarshalNio(TestMessage msg, GridKernalContext kctx) throws IgniteCheckedException {
        if (msg.ver != null)
            MessageMarshaller.unmarshal(kctx.messageFactory(), msg.ver, kctx);

        if (msg.ver2 != null)
            MessageMarshaller.unmarshal(kctx.messageFactory(), msg.ver2, kctx);
    }
}