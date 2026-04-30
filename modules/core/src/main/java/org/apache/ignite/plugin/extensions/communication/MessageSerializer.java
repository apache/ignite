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

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;

/** Message serialization logic. */
public interface MessageSerializer<M extends Message> {
    /**
     * Writes this message to provided byte buffer.
     *
     * @param msg Message instance.
     * @param writer Writer.
     * @return Whether message was fully written.
     */
    public boolean writeTo(M msg, MessageWriter writer);

    /**
     * Reads this message from provided byte buffer.
     *
     * @param msg Message instance.
     * @param reader Reader.
     * @return Whether message was fully read.
     */
    public boolean readFrom(M msg, MessageReader reader);

    /**
     * Runs {@code CacheObject.prepareMarshal} for {@code @Order} cache-object fields on the user thread, so the NIO
     * worker never does it. Default is a no-op. The caller is responsible for guaranteeing that {@code ctx} is
     * non-null when invoking this method; resolution-with-null-skip happens at call sites.
     *
     * @param msg Message instance.
     * @param ctx Cache object value context for {@code msg}'s direct {@code CacheObject} fields and non-cacheId-aware
     *     nested messages. Always non-null.
     * @param sharedCtx Shared cache context for resolving per-cache contexts of nested cacheId-aware messages.
     * @throws IgniteCheckedException If marshalling fails.
     */
    public default void prepareMarshalCacheObjects(M msg, CacheObjectValueContext ctx, GridCacheSharedContext sharedCtx)
        throws IgniteCheckedException {
        // No-op by default.
    }
}
