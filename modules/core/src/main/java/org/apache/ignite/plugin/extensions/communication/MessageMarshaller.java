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
import org.apache.ignite.internal.managers.communication.MessageUnmarshalDedup;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Handles {@code marshal}/{@code unmarshal} for a {@link Message} type that requires custom serialization.
 *
 * @param <M> Message type.
 */
public interface MessageMarshaller<M extends Message> {
    /**
     * Marshals the message on the user thread before sending.
     *
     * @param msg Message to marshal.
     * @param kctx Kernal context.
     * @param nested Cache object context, or {@code null} if not applicable.
     */
    public void marshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext nested)
        throws IgniteCheckedException;

    /**
     * Unmarshals the message with full cache context and class loader.
     *
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     * @param nested Cache object context, or {@code null} if not applicable.
     * @param clsLdr Class loader for unmarshalling.
     */
    public void unmarshal(M msg, GridKernalContext kctx, @Nullable CacheObjectContext nested, ClassLoader clsLdr)
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
     * Unmarshals only {@code @NioField}-annotated fields in the NIO/IO thread. No-op by default.
     *
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     */
    default void unmarshalNio(M msg, GridKernalContext kctx) throws IgniteCheckedException {
    }

    /**
     * Null-safe {@code unmarshalNio} — skips when no marshaller is registered.
     *
     * @param <M> Message type.
     * @param factory Message factory.
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     */
    static <M extends Message> void unmarshalNio(MessageFactory factory, M msg, GridKernalContext kctx)
        throws IgniteCheckedException {
        MessageMarshaller<M> m = resolve(factory, msg);

        if (m != null)
            m.unmarshalNio(msg, kctx);
    }

    /**
     * Null-safe {@code marshal} — skips when no marshaller is registered.
     *
     * @param <M> Message type.
     * @param factory Message factory.
     * @param msg Message to marshal.
     * @param kctx Kernal context.
     * @param nested Cache object context, or {@code null} if not applicable.
     */
    static <M extends Message> void marshal(MessageFactory factory, M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext nested) throws IgniteCheckedException {
        MessageMarshaller<M> m = resolve(factory, msg);

        if (m != null)
            m.marshal(msg, kctx, nested);
    }

    /**
     * Null-safe {@code unmarshal} — skips when no marshaller is registered.
     *
     * @param <M> Message type.
     * @param factory Message factory.
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     * @param nested Cache object context, or {@code null} if not applicable.
     * @param clsLdr Class loader for unmarshalling.
     */
    static <M extends Message> void unmarshal(MessageFactory factory, M msg, GridKernalContext kctx,
        @Nullable CacheObjectContext nested, ClassLoader clsLdr) throws IgniteCheckedException {
        assert !MessageUnmarshalDedup.ENABLED || MessageUnmarshalDedup.firstUnmarshal(msg, true)
            : "Finish-unmarshalled more than once: " + msg.getClass().getName();

        MessageMarshaller<M> m = resolve(factory, msg);

        if (m != null)
            m.unmarshal(msg, kctx, nested, clsLdr);
    }

    /**
     * Null-safe {@code unmarshal} (cache-free) — skips when no marshaller is registered.
     *
     * @param <M> Message type.
     * @param factory Message factory.
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     */
    static <M extends Message> void unmarshal(MessageFactory factory, M msg, GridKernalContext kctx)
        throws IgniteCheckedException {
        assert !MessageUnmarshalDedup.ENABLED || MessageUnmarshalDedup.firstUnmarshal(msg, false)
            : "Finish-unmarshalled more than once: " + msg.getClass().getName();

        MessageMarshaller<M> m = resolve(factory, msg);

        if (m != null)
            m.unmarshal(msg, kctx);
    }

    /** @return the marshaller registered for {@code msg}'s direct type, or {@code null} if none. */
    @SuppressWarnings("unchecked")
    private static <M extends Message> MessageMarshaller<M> resolve(MessageFactory factory, M msg) {
        return (MessageMarshaller<M>)factory.marshaller(msg.directType());
    }
}
