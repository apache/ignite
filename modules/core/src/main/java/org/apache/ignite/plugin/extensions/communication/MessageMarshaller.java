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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.jetbrains.annotations.Nullable;

/** Marshalling logic for cache-object fields in a {@link Message}. */
public interface MessageMarshaller<M extends Message> {
    /**
     * Marshals cache-object fields on the user thread.
     *
     * @param msg     Message instance.
     * @param kctx    Kernal context.
     * @param nested  Nested cache context, or {@code null}.
     */
    public void prepareMarshal(M msg, GridKernalContext kctx, @Nullable GridCacheContext<?, ?> nested)
        throws IgniteCheckedException;

    /**
     * Unmarshals cache-object fields including nested-context resolution.
     *
     * @param msg     Message instance.
     * @param kctx    Kernal context.
     * @param nested  Nested cache context, or {@code null}.
     * @param clsLdr  Class loader.
     */
    public void finishUnmarshal(M msg, GridKernalContext kctx, @Nullable GridCacheContext<?, ?> nested, ClassLoader clsLdr)
        throws IgniteCheckedException;

    /**
     * Unmarshals message fields without a nested cache context (cache-free path).
     *
     * @param msg  Message instance.
     * @param kctx Kernal context.
     */
    public void finishUnmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException;

    /** Null-safe {@code prepareMarshal} — skips when no marshaller is registered (e.g. NonMarshallableMessage). 
     * 
     * @param factory Message factory.
     * @param msg Message.
     * @param kctx Kernal context.
     * @param nested Nested context.
     * @param <M> Message type.
     * */
    static <M extends Message> void prepareMarshal(
        MessageFactory factory, M msg, GridKernalContext kctx, @Nullable GridCacheContext<?, ?> nested)
        throws IgniteCheckedException {
        MessageMarshaller<M> m = (MessageMarshaller<M>)factory.marshaller(msg.directType());

        if (m != null)
            m.prepareMarshal(msg, kctx, nested);
    }

    /** Null-safe {@code finishUnmarshal} (with nested context) — skips when no marshaller is registered. 
     * 
     * @param factory Message Factory.
     * @param msg Message.
     * @param kctx Kernal context.
     * @param nested Nested context.
     * @param clsLdr Class loader.
     * @param <M> Message type.
     * */
    static <M extends Message> void finishUnmarshal(
        MessageFactory factory, M msg, GridKernalContext kctx, @Nullable GridCacheContext<?, ?> nested, ClassLoader clsLdr)
        throws IgniteCheckedException {
        MessageMarshaller<M> m = (MessageMarshaller<M>)factory.marshaller(msg.directType());

        if (m != null)
            m.finishUnmarshal(msg, kctx, nested, clsLdr);
    }

    /** Null-safe {@code finishUnmarshal} (cache-free) — skips when no marshaller is registered. 
     * 
     * @param factory Message factory.
     * @param kctx Kernal context.
     * @param msg Message.
     * @param <M> Message type.
     * */
    static <M extends Message> void finishUnmarshal(
        MessageFactory factory, M msg, GridKernalContext kctx)
        throws IgniteCheckedException {
        MessageMarshaller<M> m = (MessageMarshaller<M>)factory.marshaller(msg.directType());

        if (m != null)
            m.finishUnmarshal(msg, kctx);
    }
}
