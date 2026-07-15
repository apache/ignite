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
 * Handles {@code marshal}/{@code unmarshal} for a {@link Message} that requires custom serialization. Resolve-and-dispatch
 * entry points that look the marshaller up from the message factory live in {@code MessageMarshalling}.
 *
 * @param <M> Message type.
 */
public interface MessageMarshaller<M extends Message> {
    /**
     * Marshals the message on the user thread before sending.
     *
     * @param msg Message to marshal.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     */
    public void marshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx)
        throws IgniteCheckedException;

    /**
     * Unmarshals the message with full cache context and class loader.
     *
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     * @param cacheObjCtx Cache object context of the enclosing message, or {@code null} at the top level.
     * @param clsLdr Class loader for unmarshalling.
     */
    public void unmarshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext cacheObjCtx, ClassLoader clsLdr)
        throws IgniteCheckedException;

    /**
     * Unmarshals the message without a cache context, using the configuration class loader — the cache-free receive
     * path (e.g. the generic {@code GridIoManager} pass). Delegates to the cache-aware overload with a {@code null}
     * context, so per-message marshallers need only implement the cache-aware method.
     *
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     */
    default void unmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException {
        unmarshal(msg, kctx, null, U.resolveClassLoader(kctx.config()));
    }

    /**
     * Unmarshals only the {@code @NioField}-annotated fields (routing headers such as the topic) on the NIO thread,
     * where no cache context is available — unlike the {@code unmarshal} overloads, which restore the full field set
     * later on a worker thread. No-op by default; overridden only for messages that carry {@code @NioField}s.
     *
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     */
    default void unmarshalNio(M msg, GridKernalContext kctx) throws IgniteCheckedException {
    }
}
