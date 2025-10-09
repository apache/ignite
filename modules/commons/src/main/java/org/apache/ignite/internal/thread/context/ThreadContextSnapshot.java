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
public class ThreadContextSnapshot {
    /** */
    private static final ThreadContextSnapshot EMPTY = new ThreadContextSnapshot(-1, null, null);

    /** */
    private final int attrId;

    /** */
    private final Object attrVal;

    /** */
    private final ThreadContextSnapshot prev;

    /** */
    private ThreadContextSnapshot(int attrId, Object attrVal, ThreadContextSnapshot prev) {
        this.attrId = attrId;
        this.attrVal = attrVal;
        this.prev = prev;
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
    ThreadContextSnapshot previous() {
        assert !isEmpty();

        return prev;
    }

    /** */
    boolean isEmpty() {
        return this == EMPTY;
    }

    /** */
    <T> ThreadContextSnapshot withAttribute(int attrId, Object val) {
        return new ThreadContextSnapshot(attrId, val, this);
    }

    /** */
    static ThreadContextSnapshot emptySnapshot() {
        return EMPTY;
    }
}
