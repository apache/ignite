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

package org.apache.ignite.internal.util.lang;

import java.io.Externalizable;
import java.util.Objects;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Convenience class representing mutable tuple of two values when first is int primitive.
 */
public class IgniteIntObjectTuple<V> {
    /** First value. */
    @GridToStringInclude
    private int val1;

    /** Second value. */
    @GridToStringInclude
    private V val2;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgniteIntObjectTuple() {
        // No-op.
    }

    /**
     * Fully initializes this tuple.
     *
     * @param val1 First value.
     * @param val2 Second value.
     */
    public IgniteIntObjectTuple(int val1, @Nullable V val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    /**
     * Gets first value.
     *
     * @return First value.
     */
    public int get1() {
        return val1;
    }

    /**
     * Gets second value.
     *
     * @return Second value.
     */
    public V get2() {
        return val2;
    }

    /**
     * Sets first value.
     *
     * @param val1 First value.
     */
    public void set1(int val1) {
        this.val1 = val1;
    }

    /**
     * Sets second value.
     *
     * @param val2 Second value.
     */
    public void set2(@Nullable V val2) {
        this.val2 = val2;
    }

    /**
     * Sets both values in the tuple.
     *
     * @param val1 First value.
     * @param val2 Second value.
     */
    public void set(int val1, @Nullable V val2) {
        set1(val1);
        set2(val2);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(val1, val2);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IgniteIntObjectTuple))
            return false;

        IgniteIntObjectTuple<?> t = (IgniteIntObjectTuple<?>)o;

        return val1 == t.val1 && F.eq(val2, t.val2);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteIntObjectTuple.class, this);
    }
}
