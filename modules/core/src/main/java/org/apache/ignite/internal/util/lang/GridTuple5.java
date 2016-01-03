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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Convenience class representing mutable tuple of five values.
 * <h2 class="header">Thread Safety</h2>
 * This class doesn't provide any synchronization for multi-threaded access
 * and it is responsibility of the user of this class to provide outside
 * synchronization, if needed.
 * @see GridFunc#t5()
 * @see GridFunc#t(Object, Object, Object, Object, Object)
 */
public class GridTuple5<V1, V2, V3, V4, V5> implements Iterable<Object>, Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value 1. */
    @GridToStringInclude
    private V1 v1;

    /** Value 2. */
    @GridToStringInclude
    private V2 v2;

    /** Value 3. */
    @GridToStringInclude
    private V3 v3;

    /** Value 4. */
    @GridToStringInclude
    private V4 v4;

    /** Value 5. */
    @GridToStringInclude
    private V5 v5;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridTuple5() {
        // No-op.
    }

    /**
     * Fully initializes this tuple.
     *
     * @param v1 First value.
     * @param v2 Second value.
     * @param v3 Third value.
     * @param v4 Forth value.
     * @param v5 Fifth value.
     */
    public GridTuple5(@Nullable V1 v1, @Nullable V2 v2, @Nullable V3 v3, @Nullable V4 v4, @Nullable V5 v5) {
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
        this.v4 = v4;
        this.v5 = v5;
    }

    /**
     * Gets first value.
     *
     * @return First value.
     */
    @Nullable public V1 get1() {
        return v1;
    }

    /**
     * Gets second value.
     *
     * @return Second value.
     */
    @Nullable public V2 get2() {
        return v2;
    }

    /**
     * Gets third value.
     *
     * @return Third value.
     */
    @Nullable public V3 get3() {
        return v3;
    }

    /**
     * Gets forth value.
     *
     * @return Forth value.
     */
    @Nullable public V4 get4() {
        return v4;
    }

    /**
     * Gets fifth value.
     *
     * @return Fifth value.
     */
    @Nullable public V5 get5() {
        return v5;
    }

    /**
     * Sets first value.
     *
     * @param v1 First value.
     */
    public void set1(@Nullable V1 v1) {
        this.v1 = v1;
    }

    /**
     * Sets second value.
     *
     * @param v2 Second value.
     */
    public void set2(@Nullable V2 v2) {
        this.v2 = v2;
    }

    /**
     * Sets third value.
     *
     * @param v3 Third value.
     */
    public void set3(@Nullable V3 v3) {
        this.v3 = v3;
    }

    /**
     * Sets forth value.
     *
     * @param v4 Forth value.
     */
    public void set4(@Nullable V4 v4) {
        this.v4 = v4;
    }

    /**
     * Sets fifth value.
     *
     * @param v5 Fifth value.
     */
    public void set5(@Nullable V5 v5) {
        this.v5 = v5;
    }

    /**
     * Sets all values.
     *
     * @param val1 First value.
     * @param val2 Second value.
     * @param val3 Third value.
     * @param val4 Fourth value.
     * @param val5 Fifth value.
     */
    public void set(@Nullable V1 val1, @Nullable V2 val2, @Nullable V3 val3, @Nullable V4 val4, @Nullable V5 val5) {
        set1(val1);
        set2(val2);
        set3(val3);
        set4(val4);
        set5(val5);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Object> iterator() {
        return new Iterator<Object>() {
            private int nextIdx = 1;

            @Override public boolean hasNext() {
                return nextIdx < 6;
            }

            @Nullable @Override public Object next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                Object res = null;

                if (nextIdx == 1)
                    res = get1();
                else if (nextIdx == 2)
                    res = get2();
                else if (nextIdx == 3)
                    res = get3();
                else if (nextIdx == 4)
                    res = get4();
                else if (nextIdx == 5)
                    res = get5();

                nextIdx++;

                return res;
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException"})
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
        out.writeObject(v1);
        out.writeObject(v2);
        out.writeObject(v3);
        out.writeObject(v4);
        out.writeObject(v5);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        v1 = (V1)in.readObject();
        v2 = (V2)in.readObject();
        v3 = (V3)in.readObject();
        v4 = (V4)in.readObject();
        v5 = (V5)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridTuple5))
            return false;

        GridTuple5<?, ?, ?, ?, ?> t = (GridTuple5<?, ?, ?, ?, ?>)o;

        return F.eq(v1, t.v1) && F.eq(v2, t.v2) && F.eq(v3, t.v3) && F.eq(v4, t.v4) && F.eq(v5, t.v5);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = v1 != null ? v1.hashCode() : 0;

        res = 13 * res + (v2 != null ? v2.hashCode() : 0);
        res = 17 * res + (v3 != null ? v3.hashCode() : 0);
        res = 19 * res + (v4 != null ? v4.hashCode() : 0);
        res = 31 * res + (v5 != null ? v5.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTuple5.class, this);
    }
}