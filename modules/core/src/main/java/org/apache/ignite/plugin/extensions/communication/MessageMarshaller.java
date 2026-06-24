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

/** Handles {@code prepareMarshal}/{@code finishUnmarshal} for a {@link Message} type that requires custom serialization. */
public interface MessageMarshaller<M extends Message> {
    /** Pre-marshals the message on the user thread before sending. */
    public void prepareMarshal(M msg, GridKernalContext kctx, @Nullable GridCacheContext<?, ?> nested)
        throws IgniteCheckedException;

    /** Post-unmarshals the message with full cache context and class loader. */
    public void finishUnmarshal(M msg, GridKernalContext kctx, @Nullable GridCacheContext<?, ?> nested, ClassLoader clsLdr)
        throws IgniteCheckedException;

    /** Post-unmarshals message fields that do not require a cache context. */
    public void finishUnmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException;

    /** Unmarshals only {@code @NioField}-annotated fields in the NIO/IO thread. No-op by default. */
    default void finishUnmarshalNio(M msg, GridKernalContext kctx) throws IgniteCheckedException {
    }

    /** Null-safe {@code finishUnmarshalNio} — skips when no marshaller is registered. */
    static <M extends Message> void finishUnmarshalNio(
        MessageFactory factory, M msg, GridKernalContext kctx)
        throws IgniteCheckedException {
        MessageMarshaller<M> m = (MessageMarshaller<M>)factory.marshaller(msg.directType());

        if (m != null)
            m.finishUnmarshalNio(msg, kctx);
    }

    /** Null-safe {@code prepareMarshal} — skips when no marshaller is registered. */
    static <M extends Message> void prepareMarshal(
        MessageFactory factory, M msg, GridKernalContext kctx, @Nullable GridCacheContext<?, ?> nested)
        throws IgniteCheckedException {
        MessageMarshaller<M> m = (MessageMarshaller<M>)factory.marshaller(msg.directType());

        if (m != null)
            m.prepareMarshal(msg, kctx, nested);
    }

    /** Null-safe {@code finishUnmarshal} — skips when no marshaller is registered. */
    static <M extends Message> void finishUnmarshal(
        MessageFactory factory, M msg, GridKernalContext kctx, @Nullable GridCacheContext<?, ?> nested, ClassLoader clsLdr)
        throws IgniteCheckedException {
        MessageMarshaller<M> m = (MessageMarshaller<M>)factory.marshaller(msg.directType());

        if (m != null)
            m.finishUnmarshal(msg, kctx, nested, clsLdr);
    }

    /** Null-safe {@code finishUnmarshal} (cache-free) — skips when no marshaller is registered. */
    static <M extends Message> void finishUnmarshal(
        MessageFactory factory, M msg, GridKernalContext kctx)
        throws IgniteCheckedException {
        MessageMarshaller<M> m = (MessageMarshaller<M>)factory.marshaller(msg.directType());

        if (m != null)
            m.finishUnmarshal(msg, kctx);
    }
}
