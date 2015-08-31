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
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Constructs a tuple over a given array.
 * <h2 class="header">Thread Safety</h2>
 * This class doesn't provide any synchronization for multi-threaded access
 * and it is responsibility of the user of this class to provide outside
 * synchronization, if needed.
 * @see GridFunc#tv(Object...)
 */
public class GridTupleV implements Iterable<Object>, Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Tuple values. */
    @GridToStringInclude
    private Object[] vals;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridTupleV() {
        // No-op.
    }

    /**
     * Initializes tuple with given object count.
     *
     * @param cnt Count of objects to be stored in the tuple.
     */
    public GridTupleV(int cnt) {
        A.ensure(cnt > 0, "cnt > 0");

        vals = new Object[cnt];
    }

    /**
     * Constructs tuple around passed in array.
     *
     * @param vals Values.
     */
    public GridTupleV(Object... vals) {
        this.vals = vals;
    }

    /**
     * Retrieves value at given index.
     *
     * @param i Index of the value to get.
     * @param <V> Value type.
     * @return Value at given index.
     */
    @SuppressWarnings({"unchecked"})
    public <V> V get(int i) {
        A.ensure(i < vals.length, "i < vals.length");

        return (V)vals[i];
    }

    /**
     * Sets value at given index.
     *
     * @param i Index to set.
     * @param v Value to set.
     * @param <V> Value type.
     */
    public <V> void set(int i, V v) {
        A.ensure(i < vals.length, "i < vals.length");

        vals[i] = v;
    }

    /**
     * Sets given values starting at {@code 0} position.
     *
     * @param v Values to set.
     */
    public void set(Object... v) {
        A.ensure(v.length <= vals.length, "v.length <= vals.length");

        if (v.length > 0)
            System.arraycopy(v, 0, vals, 0, v.length);
    }

    /**
     * Sets given values starting at provided position in the tuple.
     *
     * @param pos Position to start from.
     * @param v Values to set.
     */
    public void set(int pos, Object... v) {
        A.ensure(pos > 0, "pos > 0");
        A.ensure(v.length + pos <= vals.length, "v.length + pos <= vals.length");

        if (v.length > 0)
            System.arraycopy(v, 0, vals, pos, v.length);
    }

    /**
     * Gets internal array. Changes to this array will change this tuple.
     *
     * @return Internal array.
     */
    public Object[] getAll() {
        return vals;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Object> iterator() {
        return new Iterator<Object>() {
            private int nextIdx;

            @Override public boolean hasNext() {
                return nextIdx < vals.length;
            }

            @Override public Object next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                return vals[nextIdx++];
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
        U.writeArray(out, vals);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        vals = U.readArray(in);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(vals);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || o instanceof GridTupleV && Arrays.equals(vals, ((GridTupleV)o).vals);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTupleV.class, this);
    }
}