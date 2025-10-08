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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import org.apache.ignite.internal.util.typedef.F;

/** */
class ThreadContextData {
    /** */
    private static final int DFLT_SCOPE_ATTR_VAL_CAPACITY = 2;

    /** */
    private final ThreadContextAttributeRegistry attrReg = ThreadContextAttributeRegistry.instance();

    /** */
    private int activeScopeDepth;

    /** */
    private int activeAttrsCnt;

    /** */
    private Deque<ScopedAttributeValue>[] attrs = new Deque[attrReg.size()];

    /** */
    <T> T get(ThreadContextAttribute<T> attr) {
        if (activeAttrsCnt == 0)
            return attr.initialValue();

        Deque<ScopedAttributeValue> attrScopedVals = attributeScopedValues(attr.id());

        return F.isEmpty(attrScopedVals)
            ? attr.initialValue()
            : attrScopedVals.peek().value();
    }

    /** */
    <T> void put(ThreadContextAttribute<T> attr, T val) {
        if (get(attr) == val)
            return;

        Deque<ScopedAttributeValue> attrScopedVals = attributeScopedValues(attr.id());

        if (attrScopedVals == null)
            attrScopedVals = createAttributeValuesHolder(attr.id());

        if (attrScopedVals.isEmpty())
            ++activeAttrsCnt;
        else if (attrScopedVals.peek().scopeDepth() == activeScopeDepth)
            throw new UnsupportedOperationException("Overriding an existing attribute value within a scope is not supported");

        attrScopedVals.push(new ScopedAttributeValue(activeScopeDepth, val));
    }

    /** */
    ThreadContextSnapshot createSnapshot() {
        if (activeAttrsCnt == 0)
            return ThreadContextSnapshot.emptySnapshot();

        ThreadContextSnapshot snapshot = ThreadContextSnapshot.emptySnapshot();

        for (int attrId = attrs.length - 1; attrId >= 0; attrId--) {
            Deque<ScopedAttributeValue> attrScopedVals = attrs[attrId];

            if (F.isEmpty(attrScopedVals))
                continue;

            snapshot = snapshot.addAttribute(attrId, attrScopedVals.peek().value());
        }

        return snapshot;
    }

    /** */
    void restoreSnapshot(ThreadContextSnapshot snapshot) {
        if (snapshot.isEmpty() && activeAttrsCnt == 0)
            return;

        for (ThreadContextAttribute<?> attr : attrReg.attributes()) {
            Object val;

            if (!snapshot.isEmpty() && snapshot.attributeId() == attr.id()) {
                val = snapshot.attributeValue();

                snapshot = snapshot.next();
            }
            else
                val = attr.initialValue();

            put((ThreadContextAttribute<Object>)attr, val);
        }
    }

    /** */
    void onScopeCreated() {
        ++activeScopeDepth;
    }

    /** */
    void onScopeClosed() {
        if (activeAttrsCnt != 0)
            clearActiveScopeData();

        --activeScopeDepth;
    }

    /** */
    private void clearActiveScopeData() {
        for (Deque<ScopedAttributeValue> attrScopedVals : attrs) {
            if (F.isEmpty(attrScopedVals) || attrScopedVals.peek().scopeDepth() != activeScopeDepth)
                continue;

            attrScopedVals.pop();

            if (attrScopedVals.isEmpty())
                --activeAttrsCnt;
        }
    }

    /** */
    private Deque<ScopedAttributeValue> createAttributeValuesHolder(int id) {
        Deque<ScopedAttributeValue> res = new ArrayDeque<>(DFLT_SCOPE_ATTR_VAL_CAPACITY);

        ensureCapacityFor(id);

        attrs[id] = res;

        return res;
    }

    /** */
    private Deque<ScopedAttributeValue> attributeScopedValues(int id) {
        return id < attrs.length ? attrs[id] : null;
    }

    /** */
    public void ensureCapacityFor(int id) {
        if (attrs.length > id)
            return;

        attrs = Arrays.copyOf(attrs, id + 1);
    }

    /** */
    private static class ScopedAttributeValue {
        /** */
        private final int scopeDepth;

        /** */
        private final Object val;

        /** */
        public ScopedAttributeValue(int scopeDepth, Object val) {
            this.scopeDepth = scopeDepth;
            this.val = val;
        }

        /** */
        public int scopeDepth() {
            return scopeDepth;
        }

        /** */
        public <T> T value() {
            return (T)val;
        }
    }
}
