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

/** */
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
    ScopedContext findScopedContextFor(ContextAttribute<?> attr) {
        if (!snapshot.containsValueFor(attr))
            return null;

        ScopedContext sc = cache.get(attr.id());

        if (sc == null)
            sc = searchFor(attr);

        assert sc != null : "Failed to find the attribute value that must be present according to the attribute presence bits";
        assert sc.attribute().id() == attr.id();

        return sc;
    }

    /** */
    void attach(ScopedContext scopedCtx) {
        snapshot = snapshot.attach(scopedCtx);

        storeToCache(scopedCtx);
    }

    /** */
    void detach(ScopedContext scopedCtx) {
        ScopedContext last = snapshot.scopedContext();

        assert last == scopedCtx : "Scopes must be closed in the same order and in the same thread they are opened";

        snapshot = snapshot.previous();

        cache.invalidateByIndexes(last.storedAttributeBits());
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
    private ScopedContext searchFor(ContextAttribute<?> attr) {
        for (ContextSnapshot s = snapshot; !s.isEmpty(); s = s.previous()) {
            ScopedContext scopedCtx = s.scopedContext();

            if (!scopedCtx.containsValueFor(attr))
                continue;

            for (ScopedContext sc = scopedCtx; !sc.isEmpty(); sc = sc.previous()) {
                if (sc.attribute().id() == attr.id()) {
                    cache.store(sc);

                    return sc;
                }
            }

            return null;
        }

        return null;
    }

    /** */
    private void storeToCache(ScopedContext scopedCtx) {
        int processed = 0;
        
        for (ScopedContext sc = scopedCtx; !sc.isEmpty(); sc = sc.previous()) {
            ContextAttribute<?> attr = sc.attribute();

            // The value for an attribute can be specified multiple times during context creation - we ignore all but the last one.
            if ((processed & attr.bitmask()) != 0)
                continue;

            cache.store(sc);
            
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
        private static final ScopedContext[] EMPTY = new ScopedContext[0];

        /** */
        private ScopedContext[] vals = EMPTY;

        /** */
        ScopedContext get(int attrId) {
            if (vals.length <= attrId)
                return null;

            return vals[attrId];
        }

        /** */
        void store(ScopedContext sc) {
            assert sc != null;

            byte attrId = sc.attribute().id();

            if (vals.length <= attrId)
                grow(ContextAttribute.highReservedId());

            vals[attrId] = sc;
        }

        /** */
        void invalidateByIndexes(int idxBits) {
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
        private void invalidate(int attrId) {
            if (attrId < vals.length)
                vals[attrId] = null;
        }

        /** */
        private void grow(int size) {
            ScopedContext[] upd = new ScopedContext[size];

            System.arraycopy(vals, 0, upd, 0, vals.length);

            vals = upd;
        }
    }
}
