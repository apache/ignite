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

package org.apache.ignite.internal.thread.context;

import java.util.Arrays;
import org.jetbrains.annotations.Nullable;

/** Represents storage of {@link ContextAttribute}s and their values bound to the thread. */
class ThreadLocalContextStorage {
    /** */
    private static final ThreadLocal<ThreadLocalContextStorage> INSTANCE = ThreadLocal.withInitial(ThreadLocalContextStorage::new);

    /** */
    private final Cache cache = new Cache();

    /** */
    private ContextSnapshot snapshot = ContextSnapshot.ROOT;

    /** */
    private ThreadLocalContextStorage() {
        // No-op.
    }

    /** */
    @Nullable Context.AttributeValueHolder findValueHolderFor(ContextAttribute<?> attr) {
        if (!snapshot.containsValueFor(attr))
            return null;

        Context.AttributeValueHolder valHolder = cache.get(attr.id());

        if (valHolder == null)
            valHolder = searchFor(attr);

        assert valHolder != null : "Failed to find the attribute value that must be present according to the attribute presence bits";
        assert valHolder.attribute().id() == attr.id();

        return valHolder;
    }

    /** */
    void attach(Context.AttributeValueHolder valHolder) {
        snapshot = snapshot.attach(valHolder);

        storeToCache(valHolder);
    }

    /** */
    void detach(Context.AttributeValueHolder valHolder) {
        Context.AttributeValueHolder data = snapshot.data();

        assert data == valHolder : "Scopes must be closed in the same order and in the same thread they are opened";

        snapshot = snapshot.previous();

        cache.invalidateByIndexBits(data.storedAttributeIdBits());
    }

    /** */
    ContextSnapshot snapshot() {
        return snapshot;
    }

    /** */
    void reinitialize(ContextSnapshot snapshot) {
        this.snapshot = snapshot;

        cache.invalidate();
    }

    /** */
    private Context.AttributeValueHolder searchFor(ContextAttribute<?> attr) {
        for (ContextSnapshot s = snapshot; !s.isEmpty(); s = s.previous()) {
            Context.AttributeValueHolder valHolder = s.data();

            if (!valHolder.containsValueFor(attr))
                continue;

            for (Context.AttributeValueHolder h = valHolder; !h.isEmpty(); h = h.previous()) {
                if (h.attribute().id() == attr.id()) {
                    cache.store(h);

                    return h;
                }
            }

            return null;
        }

        return null;
    }

    /** */
    private void storeToCache(Context.AttributeValueHolder valHolder) {
        int processed = 0;
        
        for (Context.AttributeValueHolder h = valHolder; !h.isEmpty(); h = h.previous()) {
            ContextAttribute<?> attr = h.attribute();

            // The value for an attribute can be specified multiple times during context creation - we ignore all but the last one.
            if ((processed & attr.bitmask()) != 0)
                continue;

            cache.store(h);
            
            processed |= attr.bitmask();
        }
    }

    /** */
    static ThreadLocalContextStorage get() {
        return INSTANCE.get();
    }

    /** */
    private static class Cache {
        /** */
        private static final Context.AttributeValueHolder[] EMPTY = new Context.AttributeValueHolder[0];

        /** */
        private Context.AttributeValueHolder[] vals = EMPTY;

        /** */
        Context.AttributeValueHolder get(int idx) {
            if (vals.length <= idx)
                return null;

            return vals[idx];
        }

        /** */
        void store(Context.AttributeValueHolder sc) {
            assert sc != null;

            byte idx = sc.attribute().id();

            if (vals.length <= idx)
                grow(ContextAttribute.highReservedId());

            vals[idx] = sc;
        }

        /** */
        void invalidateByIndexBits(int idxBits) {
            while (idxBits != 0) {
                int idx = Integer.numberOfTrailingZeros(idxBits);

                invalidate(idx);

                idxBits &= ~1 << idx;
            }
        }

        /** */
        void invalidate() {
            Arrays.fill(vals, null);
        }

        /** */
        private void invalidate(int idx) {
            if (idx < vals.length)
                vals[idx] = null;
        }

        /** */
        private void grow(int size) {
            Context.AttributeValueHolder[] upd = new Context.AttributeValueHolder[size];

            System.arraycopy(vals, 0, upd, 0, vals.length);

            vals = upd;
        }
    }
}
