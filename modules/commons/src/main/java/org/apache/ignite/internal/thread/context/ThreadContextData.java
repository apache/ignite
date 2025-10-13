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

/** */
class ThreadContextData {
    /** */
    private static final ScopedAttributeValueStack<?>[] EMPTY = new ScopedAttributeValueStack[0];

    /** */
    private final ThreadContextAttributeRegistry attrReg = ThreadContextAttributeRegistry.instance();

    /** */
    private int activeScopeDepth;

    /** */
    private int activeAttrsCnt;

    /** */
    private ScopedAttributeValueStack<?>[] attrs = EMPTY;

    /** */
    <T> T get(ThreadContextAttribute<T> attr) {
        ScopedAttributeValueStack<T> attrVals = attributeValues(attr.id());

        return attrVals.peek();
    }

    /** */
    <T> void put(ThreadContextAttribute<T> attr, T val) {
        ScopedAttributeValueStack<T> attrVals = attributeValues(attr.id());

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

        for (ScopedAttributeValueStack<?> attrVals : attrs)
            snapshot = attrVals.exportTo(snapshot);

        return snapshot;
    }

    /** */
    void restoreSnapshot(ThreadContextSnapshot snapshot) {
        if (snapshot.isEmpty() && activeAttrsCnt == 0)
            return;

        for (int id = attrReg.size() - 1; id >= 0; id--) {
            if (!snapshot.isEmpty() && snapshot.attribute().id() == id) {
                put(snapshot.attribute(), snapshot.attributeValue());

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
        for (ScopedAttributeValueStack<?> attrVals : attrs) {
            if (attrVals.pop(activeScopeDepth) && attrVals.isEmpty())
                --activeAttrsCnt;
        }
    }

    /** */
    private <T> ScopedAttributeValueStack<T> attributeValues(int id) {
        if (attrs.length <= id)
            fetchRegisteredAttibutes();

        return (ScopedAttributeValueStack<T>)attrs[id];
    }

    /** */
    private void fetchRegisteredAttibutes() {
        ScopedAttributeValueStack<?>[] upd = new ScopedAttributeValueStack[attrReg.size()];

        if (attrs.length != 0)
            System.arraycopy(attrs, 0, upd, 0, attrs.length);

        for (int id = attrs.length; id < attrReg.size(); id++)
            upd[id] = new ScopedAttributeValueStack<>(attrReg.attribute(id));

        attrs = upd;
    }
}
