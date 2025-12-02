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
    private ContextStackNode ctxStackHead;

    /** */
    private final Cache cache = new Cache();

    /** */
    private ThreadLocalContextStorage() {
        // No-op.
    }

    /** */
    @Nullable AttributeValueHolder findValueHolderFor(ContextAttribute<?> attr) {
        if (!containsValueFor(attr))
            return null;

        AttributeValueHolder valHolder = cache.get(attr.id());

        if (valHolder == null)
            valHolder = searchFor(attr);

        assert valHolder != null : "Failed to find the attribute value that must be present according to the attribute presence bits";
        assert valHolder.attribute().id() == attr.id();

        return valHolder;
    }

    /** */
    void attach(Context ctx) {
        ctxStackHead = new ContextStackNode(
            ctx,
            ctxStackHead == null ? ctx.storedAttributeIdBits() : ctxStackHead.storedAttrIdBits | ctx.storedAttributeIdBits(),
            ctxStackHead
        );

        storeValuesToCache(ctx);
    }

    /** */
    void detach(Context ctx) {
        assert ctxStackHead != null : "Scopes must be closed in the same order and in the same thread they are opened";

        Context top = ctxStackHead.ctx;

        assert top == ctx : "Scopes must be closed in the same order and in the same thread they are opened";

        ctxStackHead = ctxStackHead.prev;

        cache.invalidateByIndexBits(top.storedAttributeIdBits());
    }

    /** */
    ContextSnapshot createSnapshot() {
        return ctxStackHead == null ? ContextSnapshot.EMPTY : new ThreadLocalContextSnapshot(ctxStackHead);
    }

    /** */
    private void restoreContextStackState(ContextStackNode ctxStackHead) {
        this.ctxStackHead = ctxStackHead;

        cache.invalidate();
    }

    /** */
    private boolean containsValueFor(ContextAttribute<?> attr) {
        if (ctxStackHead == null)
            return false;

        return (ctxStackHead.storedAttrIdBits & attr.bitmask()) != 0;
    }

    /** */
    private AttributeValueHolder searchFor(ContextAttribute<?> attr) {
        for (ContextStackNode stackNode = ctxStackHead; stackNode != null; stackNode = stackNode.prev) {
            if (!stackNode.ctx.containsValueFor(attr))
                continue;

            for (AttributeValueHolder valHolder : stackNode.ctx) {
                if (valHolder.attribute().id() == attr.id()) {
                    cache.store(valHolder);

                    return valHolder;
                }
            }

            return null;
        }

        return null;
    }

    /** */
    private void storeValuesToCache(Context ctx) {
        int processed = 0;
        
        for (AttributeValueHolder h : ctx) {
            ContextAttribute<?> attr = h.attribute();

            // The value for an attribute can be specified multiple times during context creation - we ignore all but the first one.
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
        private static final AttributeValueHolder[] EMPTY = new AttributeValueHolder[0];

        /** */
        private AttributeValueHolder[] vals = EMPTY;

        /** */
        AttributeValueHolder get(int idx) {
            if (vals.length <= idx)
                return null;

            return vals[idx];
        }

        /** */
        void store(AttributeValueHolder valHolder) {
            assert valHolder != null;

            byte idx = valHolder.attribute().id();

            if (vals.length <= idx)
                grow(ContextAttribute.highReservedId());

            vals[idx] = valHolder;
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
            AttributeValueHolder[] upd = new AttributeValueHolder[size];

            System.arraycopy(vals, 0, upd, 0, vals.length);

            vals = upd;
        }
    }

    /** */
    private static class ContextStackNode {
        /** */
        final Context ctx;

        /** */
        final int storedAttrIdBits;

        /** */
        final ContextStackNode prev;

        /** */
        ContextStackNode(Context ctx, int storedAttrIdBits, ContextStackNode prev) {
            this.ctx = ctx;
            this.storedAttrIdBits = storedAttrIdBits;
            this.prev = prev;
        }
    }

    /** */
    private static class ThreadLocalContextSnapshot implements ContextSnapshot {
        /** */
        private final ContextStackNode ctxStackHead;

        /** */
        private ThreadLocalContextSnapshot(ContextStackNode ctxStackHead) {
            assert ctxStackHead != null;

            this.ctxStackHead = ctxStackHead;
        }

        /** {@inheritDoc} */
        @Override public Scope restore() {
            ThreadLocalContextStorage threadStorage = get();

            ContextStackNode stash = threadStorage.ctxStackHead;

            threadStorage.restoreContextStackState(ctxStackHead);

            return () -> {
                ThreadLocalContextStorage ts = get();

                assert ts.ctxStackHead == ctxStackHead : "Scopes must be closed in the same order and in the same thread they are opened";

                ts.restoreContextStackState(stash);
            };
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return false;
        }
    }
}
