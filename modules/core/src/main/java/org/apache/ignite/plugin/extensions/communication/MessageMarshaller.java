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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
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
     * Unmarshals message fields that do not require a cache context.
     *
     * @param msg Message to unmarshal.
     * @param kctx Kernal context.
     */
    public void unmarshal(M msg, GridKernalContext kctx) throws IgniteCheckedException;

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
        assert !Dedup.ENABLED || Dedup.firstUnmarshal(msg, true) : "Finish-unmarshalled more than once: " + msg.getClass().getName();

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
        assert !Dedup.ENABLED || Dedup.firstUnmarshal(msg, false) : "Finish-unmarshalled more than once: " + msg.getClass().getName();

        MessageMarshaller<M> m = resolve(factory, msg);

        if (m != null)
            m.unmarshal(msg, kctx);
    }

    /** @return the marshaller registered for {@code msg}'s direct type, or {@code null} if none. */
    @SuppressWarnings("unchecked")
    private static <M extends Message> MessageMarshaller<M> resolve(MessageFactory factory, M msg) {
        return (MessageMarshaller<M>)factory.marshaller(msg.directType());
    }

    /**
     * Detects a {@link MarshallableMessage} instance being finish-unmarshalled more than once within the same pass
     * (cache-aware or cache-free) — a class-loader or receive-path bug. The two passes over one message (e.g. the
     * generic {@code GridIoManager} pass plus a subsystem's cache-aware pass) are legitimate and tracked separately.
     * Gated by {@link #ENABLED}, so it runs only under tests and is folded away in production.
     */
    class Dedup {
        /**
         * When {@code true}, the no-double-unmarshal check runs. {@code static final} so the JIT folds the guard away
         * in production (even with assertions on); enabled only by tests via {@code IGNITE_MESSAGE_UNMARSHAL_ONCE_CHECK}.
         */
        static final boolean ENABLED = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_MESSAGE_UNMARSHAL_ONCE_CHECK);

        /** Queue of collected referents, drained on each call to evict stale {@link IdRef}s from {@link #SEEN}. */
        private static final ReferenceQueue<Message> Q = new ReferenceQueue<>();

        /** Finish-unmarshalled instances, held weakly and keyed by identity so they vanish with the message. */
        private static final Set<IdRef> SEEN = ConcurrentHashMap.newKeySet();

        /** */
        private Dedup() {
            // No-op.
        }

        /**
         * @param msg Message about to be finish-unmarshalled.
         * @param cacheMode {@code true} for the cache-aware pass, {@code false} for the cache-free pass; the two passes
         * over one message are legitimate and tracked separately, so only a repeat of the same pass is reported.
         * @return {@code true} if {@code msg} is not a {@link MarshallableMessage} or is finish-unmarshalled the first
         * time in this pass.
         */
        static boolean firstUnmarshal(Message msg, boolean cacheMode) {
            if (!(msg instanceof MarshallableMessage))
                return true;

            for (Reference<? extends Message> r; (r = Q.poll()) != null; )
                SEEN.remove(r);

            return SEEN.add(new IdRef(msg, cacheMode));
        }

        /** Weak reference to a message keyed by (identity, pass), so distinct messages and the two passes stay distinct. */
        private static final class IdRef extends WeakReference<Message> {
            /** Referent identity hash folded with the pass, captured up front since the referent may be cleared later. */
            private final int hash;

            /** Unmarshal pass: cache-aware vs cache-free. Keeps the two legitimate passes over one message distinct. */
            private final boolean cacheMode;

            /**
             * @param msg Tracked message.
             * @param cacheMode Unmarshal pass.
             */
            IdRef(Message msg, boolean cacheMode) {
                super(msg, Q);

                this.cacheMode = cacheMode;
                hash = 31 * System.identityHashCode(msg) + (cacheMode ? 1 : 0);
            }

            /** {@inheritDoc} */
            @Override public int hashCode() {
                return hash;
            }

            /** {@inheritDoc} */
            @Override public boolean equals(Object o) {
                if (this == o)
                    return true;

                if (!(o instanceof IdRef))
                    return false;

                IdRef ref = (IdRef)o;

                Message m = get();

                return m != null && m == ref.get() && cacheMode == ref.cacheMode;
            }
        }
    }
}
