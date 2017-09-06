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

package org.apache.ignite.ml.math.impls.storage.vector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;

/**
 * Vector storage holding a single non-zero value at some index.
 */
public class SingleElementVectorStorage implements VectorStorage {
    /** */
    private int idx;
    /** */
    private double val;
    /** */
    private int size;

    /**
     *
     */
    public SingleElementVectorStorage() {
        // No-op.
    }

    /**
     * @param size Parent vector size.
     * @param idx Element index in the parent vector.
     * @param val Value of the element.
     */
    public SingleElementVectorStorage(int size, int idx, double val) {
        assert size > 0;
        assert idx >= 0;

        this.size = size;
        this.idx = idx;
        this.val = val;
    }

    /**
     * @return Index of the element in the parent vector.
     */
    public int index() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return i == idx ? val : 0.0;
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (i == idx)
            val = v;
        else
            throw new UnsupportedOperationException("Can't set element outside of index: " + idx);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(idx);
        out.writeDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        idx = in.readInt();
        val = in.readDouble();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SingleElementVectorStorage that = (SingleElementVectorStorage)o;

        return idx == that.idx && Double.compare(that.val, val) == 0 && size == that.size;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = idx;
        long temp = Double.doubleToLongBits(val);

        res = 31 * res + (int)(temp ^ (temp >>> 32));
        res = 31 * res + size;

        return res;
    }
}
