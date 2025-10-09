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
        fetchRegisteredAttibutes();
    }

    /** */
    <T> T get(int attrId) {
        return attributeValues(attrId).peek();
    }

    /** */
    void put(int attrId, Object val) {
        AttributeValueStack attrVals = attributeValues(attrId);

        if (attrVals.peek() == val)
            return;

        if (attrVals.isEmpty())
            ++activeAttrsCnt;

        attrVals.push(activeScopeDepth, val);
    }

    /** */
    ThreadContextSnapshot createSnapshot() {
        if (activeAttrsCnt == 0)
            return ThreadContextSnapshot.emptySnapshot();

        ThreadContextSnapshot snapshot = ThreadContextSnapshot.emptySnapshot();

        for (AttributeValueStack attrVals : attrs)
            snapshot = attrVals.exportTo(snapshot);

        return snapshot;
    }

    /** */
    void restoreSnapshot(ThreadContextSnapshot snapshot) {
        if (snapshot.isEmpty() && activeAttrsCnt == 0)
            return;

        for (int id = attrReg.size() - 1; id >= 0; id--) {
            if (!snapshot.isEmpty() && snapshot.attributeId() == id) {
                put(snapshot.attributeId(), snapshot.attributeValue());

                snapshot = snapshot.previous();
            }
            else
                attributeValues(id).restoreInitial(activeScopeDepth);
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
        for (AttributeValueStack attrVals : attrs) {
            if (attrVals.pop(activeScopeDepth) && attrVals.isEmpty())
                --activeAttrsCnt;
        }
    }

    /** */
    private AttributeValueStack attributeValues(int id) {
        if (attrs.length <= id)
            fetchRegisteredAttibutes();

        return attrs[id];
    }

    /** */
    private void fetchRegisteredAttibutes() {
        AttributeValueStack[] upd = new AttributeValueStack[attrReg.size()];

        for (int id = 0; id < attrReg.size(); id++) {
            upd[id] = attrs != null && id < attrs.length
                ? attrs[id]
                : new AttributeValueStack(attrReg.attribute(id));
        }

        attrs = upd;
    }

    /** */
    private static class AttributeValueStack {
        /** */
        private final int attrId;

        /** */
        private final Object initialVal;

        /** */
        private Deque<ScopedAttributeValue> vals;

        /** */
        AttributeValueStack(ThreadContextAttribute<?> attr) {
            initialVal = attr.initialValue();
            attrId = attr.id();
        }

        /** */
        boolean pop(int scopeDepth) {
            if (isEmpty() || vals.peek().scopeDepth() != scopeDepth)
                return false;

            vals.pop();

            return true;
        }

        /** */
        void push(int scopeDepth, Object val) {
            if (vals == null)
                vals = new ArrayDeque<>(DFLT_SCOPE_ATTR_VAL_CAPACITY);

            ScopedAttributeValue scopedVal = vals.peek();

            if (scopedVal != null && scopedVal.scopeDepth == scopeDepth)
                scopedVal.value(val);
            else
                vals.push(new ScopedAttributeValue(scopeDepth, val));
        }

        /** */
        <T> T peek() {
            return isEmpty() ? (T)initialVal : vals.peek().value();
        }

        /** */
        ThreadContextSnapshot exportTo(ThreadContextSnapshot snapshot) {
            Object val = peek();

            return val == initialVal ? snapshot : snapshot.withAttribute(attrId, val);
        }

        /** */
        void restoreInitial(int scopeDepth) {
            if (isEmpty() || vals.peek().value() == initialVal)
                return;

            vals.push(new ScopedAttributeValue(scopeDepth, initialVal));
        }

        /** */
        boolean isEmpty() {
            return F.isEmpty(vals);
        }

        /** */
        private static class ScopedAttributeValue {
            /** */
            private final int scopeDepth;

            /** */
            private Object val;

            /** */
            ScopedAttributeValue(int scopeDepth, Object val) {
                this.scopeDepth = scopeDepth;
                this.val = val;
            }

            /** */
            int scopeDepth() {
                return scopeDepth;
            }

            /** */
            <T> T value() {
                return (T)val;
            }

            /** */
            void value(Object val) {
                this.val = val;
            }
        }
    }
}
