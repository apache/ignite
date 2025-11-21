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

/**
 * Represents an accessor to Context Attribute value bound to the thread.
 *
 * @see Context
 */
public class ContextAttribute<T> {
    /** */
    static final AtomicInteger ID_GEN = new AtomicInteger();

    /** */
    static final int MAX_ATTR_CNT = Integer.SIZE;

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

    /**
     * Gets the value of the Context Attribute bound to the thread from which this method is called. 
     *  
     * @see Context#with(ContextAttribute, Object) 
     */
    public T get() {
        Context.AttributeValueHolder valHolder = ThreadLocalContextStorage.get().findValueHolderFor(this);

        return valHolder == null ? initVal : valHolder.value();
    }

    /** */
    static int highReservedId() {
        return ID_GEN.get();
    }

    /** Creates new instance of the Context Attribute with Initial Value set to {@code null}. */
    public static <T> ContextAttribute<T> newInstance() {
        return newInstance(null);
    }

    /**
     * Creates new instance of the Context Attribute with the specified Initial Value. The Initial Value is returned
     * by {@link ContextAttribute#get} method if the Attribute's value is not explicitly set in the Context.
     *
     * <p>
     * Note, that the maximum number of attribute instances that can be created is currently limited to
     * {@link #MAX_ATTR_CNT} for implementation reasons.
     * </p>
     */
    public static <T> ContextAttribute<T> newInstance(T initVal) {
        int id = ID_GEN.getAndIncrement();

        assert id < MAX_ATTR_CNT : "Exceeded maximum supported number of created Attributes instances [maxCnt=" + MAX_ATTR_CNT + ']';

        return new ContextAttribute<>((byte)id, initVal);
    }
}
