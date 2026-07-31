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

package org.apache.ignite.internal.managers.communication;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Detects a {@link MarshallableMessage} instance being finish-unmarshalled twice within the same pass — a class-loader
 * or receive-path bug. The cache-free and cache-aware passes over one message are both legitimate and tracked apart.
 * Gated by {@link #ENABLED}, so it runs only under tests and is folded away in production.
 */
public class MessageUnmarshalOnceCheck {
    /**
     * When {@code true}, the no-double-unmarshal check runs. {@code static final} so the JIT folds the guard away
     * in production (even with assertions on); enabled only by tests via {@code IGNITE_MESSAGE_UNMARSHAL_ONCE_CHECK}.
     */
    public static final boolean ENABLED = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_MESSAGE_UNMARSHAL_ONCE_CHECK);

    /** Cleared referents, drained on each call to evict their stale {@link IdRef}s from {@link #seen}. */
    private static final ReferenceQueue<Message> refQueue = new ReferenceQueue<>();

    /** Finish-unmarshalled instances, held weakly and keyed by identity so they vanish with the message. */
    private static final Set<IdRef> seen = ConcurrentHashMap.newKeySet();

    /** */
    private MessageUnmarshalOnceCheck() {
        // No-op.
    }

    /**
     * @param msg Message about to be finish-unmarshalled.
     * @param cacheMode {@code true} for the cache-aware pass, {@code false} for the cache-free pass; the two passes
     * over one message are legitimate and tracked separately, so only a repeat of the same pass is reported.
     * @return {@code true} if {@code msg} is not a {@link MarshallableMessage} or is finish-unmarshalled the first
     * time in this pass.
     */
    public static boolean firstUnmarshal(Message msg, boolean cacheMode) {
        if (!(msg instanceof MarshallableMessage))
            return true;

        // Static set: evict entries whose message was already collected, so it doesn't grow across the suite.
        for (Reference<? extends Message> r; (r = refQueue.poll()) != null; )
            seen.remove(r);

        return seen.add(new IdRef(msg, cacheMode));
    }

    /** Weak reference to a message keyed by identity and pass, so distinct messages and the two passes stay distinct. */
    private static final class IdRef extends WeakReference<Message> {
        /** Referent {@code identityHashCode} folded with {@code cacheMode}, captured up front as the referent may be cleared later. */
        private final int hash;

        /** Unmarshal pass: {@code true} = cache-aware, {@code false} = cache-free. */
        private final boolean cacheMode;

        /**
         * @param msg Tracked message.
         * @param cacheMode Unmarshal pass.
         */
        private IdRef(Message msg, boolean cacheMode) {
            super(msg, refQueue);

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

            if (!(o instanceof IdRef ref))
                return false;

            // Read the referent once: a concurrent GC could otherwise clear it between the null check and the compare.
            Message m = get();

            return m != null && m == ref.get() && cacheMode == ref.cacheMode;
        }
    }
}
