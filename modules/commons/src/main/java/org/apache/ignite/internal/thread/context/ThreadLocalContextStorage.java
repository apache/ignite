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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/** Represents storage of {@link ContextAttribute}s and their values bound to the thread. */
class ThreadLocalContextStorage {
    /** */
    private static final ThreadLocal<ThreadLocalContextStorage> INSTANCE = ThreadLocal.withInitial(ThreadLocalContextStorage::new);

    /** */
    private final LinkedList<Context> ctxStack = new LinkedList<>();

    /** */
    private final LinkedList<Integer> storedAttrIdBitsStack = new LinkedList<>();

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
        ctxStack.push(ctx);

        storedAttrIdBitsStack.push(storedAttrIdBitsStack.isEmpty()
            ? ctx.storedAttributeIdBits()
            : ctx.storedAttributeIdBits() | storedAttrIdBitsStack.peek()
        );

        storeValuesToCache(ctx);
    }

    /** */
    void detach(Context ctx) {
        Context top = ctxStack.pop();

        assert top == ctx : "Scopes must be closed in the same order and in the same thread they are opened";

        storedAttrIdBitsStack.pop();

        cache.invalidateByIndexBits(top.storedAttributeIdBits());
    }

    /** */
    Collection<Context> contextStackSnapshot() {
        if (ctxStack.isEmpty())
            return Collections.emptyList();

        List<Context> res = new ArrayList<>(ctxStack.size());

        ctxStack.descendingIterator().forEachRemaining(res::add);

        return res;
    }

    /** */
    void reinitialize(Collection<Context> ctxStackSnp) {
        ctxStack.clear();
        storedAttrIdBitsStack.clear();

        cache.invalidate();

        ctxStackSnp.forEach(this::attach);
    }

    /** */
    private boolean containsValueFor(ContextAttribute<?> attr) {
        if (storedAttrIdBitsStack.isEmpty())
            return false;

        return (storedAttrIdBitsStack.peek() & attr.bitmask()) != 0;
    }

    /** */
    private AttributeValueHolder searchFor(ContextAttribute<?> attr) {
        for (Context ctx : ctxStack) {
            if (!ctx.containsValueFor(attr))
                continue;

            for (AttributeValueHolder valHolder : ctx) {
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
}
