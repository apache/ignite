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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Puts the fields of a {@link Message} into the shape they go on the wire in, and back. Three kinds of work end up
 * here, and codegen writes only the ones the message actually needs: the step the message defines itself, the
 * {@code @Marshalled} fields that become bytes, and the walk into nested messages and cache objects.
 * Generated per message class and called by {@code MessageMarshalling}.
 *
 * @param <M> Message type.
 */
public interface MessageMarshaller<M extends Message> {
    /**
     * Takes the fields to their wire shape. Called on the user thread before sending.
     *
     * @param msg Message to take to the wire.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     */
    public void marshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx)
        throws IgniteCheckedException;

    /**
     * Brings the fields back, with full cache context and class loader.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     * @param clsLdr Class loader to resolve the classes with.
     */
    public void unmarshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx, ClassLoader clsLdr)
        throws IgniteCheckedException;

    /**
     * Brings the fields back without a cache context, using the configuration class loader — the cache-free receive path.
     * Delegates to the cache-aware overload with a {@code null} context, so a generated marshaller implements the
     * cache-aware method only.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     */
    default void unmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        unmarshal(msg, kctx, null, U.resolveClassLoader(kctx.config()));
    }

    /**
     * Brings back only the {@code @NioField} fields (routing headers) on the NIO thread — unlike the {@code restore}
     * overloads, which take the full payload later on a worker thread. No-op unless the message has {@code @NioField}s.
     *
     * @param msg Message to bring back.
     * @param kctx Kernal context.
     */
    default void unmarshalNio(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        // No-op.
    }
}
