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
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;

/**
 * Single value view storage over another vector.
 */
public class SingleElementVectorDelegateStorage implements VectorStorage {
    /** */
    private int idx;
    /** */
    private Vector vec;

    /**
     *
     */
    public SingleElementVectorDelegateStorage() {
        // No-op.
    }

    /**
     * @param vec Parent vector.
     * @param idx Element index.
     */
    public SingleElementVectorDelegateStorage(Vector vec, int idx) {
        assert vec != null;
        assert idx >= 0;

        this.vec = vec;
        this.idx = idx;
    }

    /**
     * @return Index of the element in the parent vector.
     */
    public int index() {
        return idx;
    }

    /**
     * @return Parent vector.
     */
    public Vector delegate() {
        return vec;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return vec.size();
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return i == idx ? vec.get(i) : 0.0;
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (i == idx)
            vec.set(i, v);
        else
            throw new UnsupportedOperationException("Can't set element outside of index: " + idx);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(vec);
        out.writeInt(idx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        vec = (Vector)in.readObject();
        idx = in.readInt();
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

        SingleElementVectorDelegateStorage that = (SingleElementVectorDelegateStorage)o;

        return idx == that.idx && (vec != null ? vec.equals(that.vec) : that.vec == null);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = idx;

        res = 31 * res + (vec != null ? vec.hashCode() : 0);

        return res;
    }
}
