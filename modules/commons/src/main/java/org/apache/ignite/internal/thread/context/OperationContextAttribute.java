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

import org.jetbrains.annotations.Nullable;

/**
 * Represents a key to access and modify {@link OperationContext} records.
 *
 * @see OperationContext
 * @see OperationContext#get(OperationContextAttribute)
 * @see OperationContext#set(OperationContextAttribute, Object)
 */
public class OperationContextAttribute<T> {
    /** */
    public static final int MAX_ATTR_CNT = Integer.SIZE;

    /** */
    private final int bitmask;

    /** */
    @Nullable private final T initVal;

    /** */
    OperationContextAttribute(int bitmask, @Nullable T initVal) {
        assert Integer.numberOfTrailingZeros(bitmask) + Integer.numberOfLeadingZeros(bitmask) == Integer.SIZE - 1;

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
     * Unique attribute bitmask calculated by shifting one from 0 to {@link Integer#SIZE}. It provides an ability to
     * use {@link OperationContext} Attribute with bit fields.
     */
    public int bitmask() {
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
}
