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

import java.util.concurrent.atomic.AtomicInteger;

/** */
public class ContextAttribute<T> {
    /** */
    static final AtomicInteger ID_GEN = new AtomicInteger();

    /** */
    static final int MAX_ATTRIBUTE_CNT = Integer.SIZE;

    /** */
    private final byte id;

    /** */
    private final int bitmask;

    /** */
    private final T initVal;

    /** */
    private ContextAttribute(byte id, T initVal) {
        this.id = id;
        this.bitmask = 1 << id;
        this.initVal = initVal;
    }

    /** */
    byte id() {
        return id;
    }

    /** */
    int bitmask() {
        return bitmask;
    }

    /** */
    public T get() {
        ScopedContext sc = ThreadLocalContextStorage.get().findScopedContextFor(this);

        return sc == null ? initVal : sc.value();
    }

    /** */
    public static <T> ContextAttribute<T> newInstance() {
        return newInstance(null);
    }

    /** */
    public static <T> ContextAttribute<T> newInstance(T initVal) {
        int id = ID_GEN.getAndIncrement();

        if (MAX_ATTRIBUTE_CNT <= id) {
            throw new RuntimeException("Exceeded maximum supported number of created Context Attributes instances" +
                " [maxCnt=" + MAX_ATTRIBUTE_CNT + ']');
        }

        return new ContextAttribute<>((byte)id, initVal);
    }

    /** */
    static int highReservedId() {
        return ID_GEN.get();
    }
}
