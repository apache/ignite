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
 * Constant read-only vector storage.
 */
public class ConstantVectorStorage implements VectorStorage {
    /** */ private int size;
    /** */ private double val;

    /**
     *
     */
    public ConstantVectorStorage() {
        // No-op.
    }

    /**
     * @param size Vector size.
     * @param val Value to set for vector elements.
     */
    public ConstantVectorStorage(int size, double val) {
        assert size > 0;

        this.size = size;
        this.val = val;
    }

    /**
     *
     *
     */
    public double constant() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        throw new UnsupportedOperationException("Can't set value into constant vector.");
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeDouble(val);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
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
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + size;
        res = res * 37 + Double.hashCode(val);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ConstantVectorStorage that = (ConstantVectorStorage)o;

        return size == that.size && val == that.val;
    }
}
