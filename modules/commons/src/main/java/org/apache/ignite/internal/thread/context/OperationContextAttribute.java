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
import org.jetbrains.annotations.Nullable;

/**
 * Represents a key to access and modify Context records.
 *
 * @see OperationContext
 * @see OperationContext#get(OperationContextAttribute)
 * @see OperationContext#set(OperationContextAttribute, Object)
 */
public class OperationContextAttribute<T> {
    /** */
    static final AtomicInteger ID_GEN = new AtomicInteger();

    /** */
    static final int MAX_ATTR_CNT = Integer.SIZE;

    /** */
    private final int bitmask;

    /** */
    @Nullable private final T initVal;

    /** */
    private OperationContextAttribute(int bitmask, @Nullable T initVal) {
        this.bitmask = bitmask;
        this.initVal = initVal;
    }

    /**
     * Initial Value associated with the current Attribute. Initial value will be automatically returned by the
     * {@link OperationContext#get} method if Attribute's value has not been previously set.
     * @see OperationContext#get(OperationContextAttribute)
     */
    @Nullable public T initialValue() {
        return initVal;
    }

    /**
     * Unique attribute bitmask calculated by shifting one from 0 to {@link Integer#SIZE}.
     * It provides an ability to use Context Attribute with bit fields.
     */
    int bitmask() {
        return bitmask;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (!(other instanceof OperationContextAttribute))
            return false;

        return bitmask == ((OperationContextAttribute<?>)other).bitmask;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return bitmask;
    }

    /**
     * Creates new instance of the Context Attribute with Initial Value set to {@code null}.
     * <p>
     * Note, that the maximum number of attribute instances that can be created is currently limited to
     * {@link #MAX_ATTR_CNT} for implementation reasons.
     * </p>
     */
    public static <T> OperationContextAttribute<T> newInstance() {
        return newInstance(null);
    }

    /**
     * Creates new instance of the Context Attribute with the specified Initial Value. The Initial Value is returned
     * by {@link OperationContext#get} method if the Attribute's value is not explicitly set in the Context.
     * <p>
     * Note, that the maximum number of attribute instances that can be created is currently limited to
     * {@link #MAX_ATTR_CNT} for implementation reasons.
     * </p>
     */
    public static <T> OperationContextAttribute<T> newInstance(T initVal) {
        int id = ID_GEN.getAndIncrement();

        assert id < MAX_ATTR_CNT : "Exceeded maximum supported number of created Attributes instances [maxCnt=" + MAX_ATTR_CNT + ']';

        return new OperationContextAttribute<>(1 << id, initVal);
    }
}
