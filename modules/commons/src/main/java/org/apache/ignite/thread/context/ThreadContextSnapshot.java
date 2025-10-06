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

package org.apache.ignite.thread.context;

/** */
public class ThreadContextSnapshot {
    /** */
    private static final ThreadContextSnapshot EMPTY = new ThreadContextSnapshot(-1, null, null);

    /** */
    private final int attrId;

    /** */
    private final Object attrVal;

    /** */
    private final ThreadContextSnapshot next;

    /** */
    private ThreadContextSnapshot(int attrId, Object attrVal, ThreadContextSnapshot next) {
        this.attrId = attrId;
        this.attrVal = attrVal;
        this.next = next;
    }

    /** */
    int attributeId() {
        assert !isEmpty();

        return attrId;
    }

    /** */
    <T> T attributeValue() {
        assert !isEmpty();

        return (T)attrVal;
    }

    /** */
    ThreadContextSnapshot next() {
        assert !isEmpty();

        return next;
    }

    /** */
    boolean isEmpty() {
        return this == EMPTY;
    }

    /** */
    ThreadContextSnapshot addAttribute(int attrId, Object attrVal) {
        return new ThreadContextSnapshot(attrId, attrVal, this);
    }

    /** */
    static ThreadContextSnapshot emptySnapshot() {
        return EMPTY;
    }
}
