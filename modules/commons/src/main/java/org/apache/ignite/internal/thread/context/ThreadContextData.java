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
    private AttributeValueStack[] attrs;

    /** */
    ThreadContextData() {
        synchronizeAttributes();
    }

    /** */
    <T> T get(ThreadContextAttribute<T> attr) {
        if (activeAttrsCnt == 0)
            return attr.initialValue();

        return attributeValues(attr.id()).peek();
    }

    /** */
    <T> void put(ThreadContextAttribute<T> attr, T val) {
        put(attr.id(), val);
    }

    /** */
    private <T> void put(int id, T val) {
        AttributeValueStack attrVals = attributeValues(id);

        if (attrVals.isEmpty() & attrVals.push(activeScopeDepth, val))
            ++activeAttrsCnt;
    }

    /** */
    ThreadContextSnapshot createSnapshot() {
        if (activeAttrsCnt == 0)
            return ThreadContextSnapshot.emptySnapshot();

        ThreadContextSnapshot snapshot = ThreadContextSnapshot.emptySnapshot();

        for (int attrId = 0; attrId < attrs.length; attrId++) {
            AttributeValueStack attrVals = attrs[attrId];

            if (!attrVals.isEmpty())
                snapshot = snapshot.addAttribute(attrId, attrVals.peek());
        }

        return snapshot;
    }

    /** */
    void restoreSnapshot(ThreadContextSnapshot snapshot) {
        if (snapshot.isEmpty() && activeAttrsCnt == 0)
            return;

        int maxId = Math.max(snapshot.attributeId(), attrs.length - 1);

        for (int id = maxId; id >= 0; id--) {
            if (!snapshot.isEmpty() && snapshot.attributeId() == id) {
                put(id, snapshot.attributeValue());

                snapshot = snapshot.previous();
            }
            else
                attrs[id].restoreInitial(activeScopeDepth);
        }
    }

    /** */
    private void synchronizeAttributes() {
        AttributeValueStack[] res = new AttributeValueStack[attrReg.size()];

        int size = attrs == null ? 0 : attrs.length;

        if (size != 0)
            System.arraycopy(attrs, 0, res, 0, size);

        for (int id = size; id < res.length; id++)
            res[id] = new AttributeValueStack(attrReg.attribute(id).initialValue());

        attrs = res;
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
        for (AttributeValueStack attrVals : attrs) {
            if (attrVals.pop(activeScopeDepth) && attrVals.isEmpty())
                --activeAttrsCnt;
        }
    }

    /** */
    private AttributeValueStack attributeValues(int id) {
        if (id < attrs.length)
            return attrs[id];

        synchronizeAttributes();

        return attrs[id];
    }

    /** */
    public void ensureCapacityFor(int id) {
        if (attrs.length > id)
            return;

        attrs = Arrays.copyOf(attrs, id + 1);
    }

    /** */
    private static class AttributeValueStack {
        /** */
        private Deque<ScopedAttributeValue> vals;

        /** */
        private final Object initialVal;

        /** */
        public AttributeValueStack(Object initialVal) {
            this.initialVal = initialVal;
        }

        /** */
        public boolean pop(int scopeDepth) {
            if (F.isEmpty(vals) || vals.peek().scopeDepth() != scopeDepth)
                return false;

            vals.pop();

            return true;
        }

        /** */
        public void restoreInitial(int scopeDepth) {
            push(scopeDepth, initialVal);
        }

        /** */
        public boolean push(int scopeDepth, Object val) {
            if (peek() == val)
                return false;

            if (vals == null)
                vals = new ArrayDeque<>(DFLT_SCOPE_ATTR_VAL_CAPACITY);
            else if (!vals.isEmpty() && vals.peek().scopeDepth() == scopeDepth)
                throw new UnsupportedOperationException();

            vals.push(new ScopedAttributeValue(scopeDepth, val));

            return true;
        }

        /** */
        public <T> T peek() {
            return F.isEmpty(vals) ? (T)initialVal : vals.peek().value();
        }

        /** */
        public boolean isEmpty() {
            return F.isEmpty(vals);
        }
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
