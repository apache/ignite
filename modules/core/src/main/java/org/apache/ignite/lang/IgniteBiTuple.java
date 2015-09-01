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

package org.apache.ignite.lang;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Convenience class representing mutable tuple of two values.
 */
public class IgniteBiTuple<V1, V2> implements Map<V1, V2>, Map.Entry<V1, V2>,
    Iterable<Object>, Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** First value. */
    @GridToStringInclude
    private V1 val1;

    /** Second value. */
    @GridToStringInclude
    private V2 val2;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgniteBiTuple() {
        // No-op.
    }

    /**
     * Fully initializes this tuple.
     *
     * @param val1 First value.
     * @param val2 Second value.
     */
    public IgniteBiTuple(@Nullable V1 val1, @Nullable V2 val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    /**
     * Swaps values.
     *
     * @return New tuple with swapped values.
     */
    public IgniteBiTuple<V2, V1> swap() {
        return F.t(val2, val1);
    }

    /**
     * Gets first value.
     *
     * @return First value.
     */
    public V1 get1() {
        return val1;
    }

    /**
     * Gets second value.
     *
     * @return Second value.
     */
    public V2 get2() {
        return val2;
    }

    /**
     * Sets first value.
     *
     * @param val1 First value.
     */
    public void set1(@Nullable V1 val1) {
        this.val1 = val1;
    }

    /**
     * Sets second value.
     *
     * @param val2 Second value.
     */
    public void set2(@Nullable V2 val2) {
        this.val2 = val2;
    }

    /**
     * Sets both values in the tuple.
     *
     * @param val1 First value.
     * @param val2 Second value.
     */
    public void set(@Nullable V1 val1, @Nullable V2 val2) {
        set1(val1);
        set2(val2);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V1 getKey() {
        return val1;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V2 getValue() {
        return val2;
    }

    /** {@inheritDoc} */
    @Override public V2 setValue(@Nullable V2 val) {
        V2 old = val2;

        set2(val);

        return old;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Object> iterator() {
        return new Iterator<Object>() {
            /** */
            private int nextIdx = 1;

            @Override public boolean hasNext() {
                return nextIdx < 3;
            }

            @Nullable @Override public Object next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                Object res = null;

                if (nextIdx == 1)
                    res = get1();
                else if (nextIdx == 2)
                    res = get2();

                nextIdx++;

                return res;
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return val1 == null && val2 == null ? 0 : 1;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return size() == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return F.eq(val1, key);
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(Object val) {
        return F.eq(val2, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V2 get(Object key) {
        return containsKey(key) ? val2 : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override
    public V2 put(V1 key, V2 val) {
        V2 old = containsKey(key) ? val2 : null;

        set(key, val);

        return old;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V2 remove(Object key) {
        if (containsKey(key)) {
            V2 v2 = val2;

            val1 = null;
            val2 = null;

            return v2;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends V1, ? extends V2> m) {
        A.notNull(m, "m");
        A.ensure(m.size() <= 1, "m.size() <= 1");

        for (Map.Entry<? extends V1, ? extends V2> e : m.entrySet())
            put(e.getKey(), e.getValue());
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        val1 = null;
        val2 = null;
    }

    /** {@inheritDoc} */
    @Override public Set<V1> keySet() {
        return Collections.singleton(val1);
    }

    /** {@inheritDoc} */
    @Override public Collection<V2> values() {
        return Collections.singleton(val2);
    }

    /** {@inheritDoc} */
    @Override public Set<Map.Entry<V1, V2>> entrySet() {
        return Collections.<Entry<V1, V2>>singleton(this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"OverriddenMethodCallDuringObjectConstruction", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public Object clone() {
        try {
            return super.clone();
        }
        catch (CloneNotSupportedException ignore) {
            throw new InternalError();
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(val1);
        out.writeObject(val2);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        val1 = (V1)in.readObject();
        val2 = (V2)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return val1 == null ? 0 : val1.hashCode() * 31 + (val2 == null ? 0 : val2.hashCode());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IgniteBiTuple))
            return false;

        IgniteBiTuple<?, ?> t = (IgniteBiTuple<?, ?>)o;

        // Both nulls or equals.
        return F.eq(val1, t.val1) && F.eq(val2, t.val2);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteBiTuple.class, this);
    }
}