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
class ScopedAttributeValueStack<T> {
    /** */
    private static final int DFLT_SCOPE_ATTR_VAL_CAPACITY = 2;

    /** */
    private final ThreadContextAttribute<T> attr;

    /** */
    private Deque<ScopedAttributeValue<T>> scopedVals;

    /** */
    ScopedAttributeValueStack(ThreadContextAttribute<T> attr) {
        this.attr = attr;
    }

    /** */
    boolean pop(int scopeDepth) {
        if (isEmpty() || scopedVals.peek().scopeDepth() != scopeDepth)
            return false;

        scopedVals.pop();

        return true;
    }

    /** */
    void push(int scopeDepth, T val) {
        if (scopedVals == null)
            scopedVals = new ArrayDeque<>(DFLT_SCOPE_ATTR_VAL_CAPACITY);

        ScopedAttributeValue<T> scopedVal = scopedVals.peek();

        if (scopedVal != null && scopedVal.scopeDepth == scopeDepth)
            scopedVal.value(val);
        else
            scopedVals.push(new ScopedAttributeValue<>(scopeDepth, val));
    }

    /** */
    T peek() {
        return isEmpty() ? attr.initialValue() : scopedVals.peek().value();
    }

    /** */
    ThreadContextSnapshot exportTo(ThreadContextSnapshot snapshot) {
        T val = peek();

        return val == attr.initialValue() ? snapshot : snapshot.withAttribute(attr, val);
    }

    /** */
    void restoreInitial(int scopeDepth) {
        if (isEmpty() || scopedVals.peek().value() == attr.initialValue())
            return;

        scopedVals.push(new ScopedAttributeValue<>(scopeDepth, attr.initialValue()));
    }

    /** */
    boolean isEmpty() {
        return F.isEmpty(scopedVals);
    }

    /** */
    private static class ScopedAttributeValue<T> {
        /** */
        private final int scopeDepth;

        /** */
        private T val;

        /** */
        ScopedAttributeValue(int scopeDepth, T val) {
            this.scopeDepth = scopeDepth;
            this.val = val;
        }

        /** */
        int scopeDepth() {
            return scopeDepth;
        }

        /** */
        T value() {
            return val;
        }

        /** */
        void value(T val) {
            this.val = val;
        }
    }
}
