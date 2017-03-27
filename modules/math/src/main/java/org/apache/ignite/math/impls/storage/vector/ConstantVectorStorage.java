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

package org.apache.ignite.math.impls.storage.vector;

import org.apache.ignite.math.VectorStorage;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import java.io.*;

/**
 * Constant read-only vector storage.
 */
public class ConstantVectorStorage implements VectorStorage {
    private int size;
    private double val;

    /**
     *
     */
    public ConstantVectorStorage() {
        // No-op.
    }

    /**
     *
     * @param size
     * @param val
     */
    public ConstantVectorStorage(int size, double val) {
        assert size > 0;

        this.size = size;
        this.val = val;
    }

    /**
     * 
     * @return
     */
    public double constant() {
        return val;
    }

    @Override public int size() {
        return size;
    }

    @Override public double get(int i) {
        return val;
    }

    @Override public void set(int i, double v) {
        throw new UnsupportedOperationException("Can't set value into constant vector.");
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeDouble(val);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        val = in.readDouble();
    }

    @Override public boolean isSequentialAccess() {
        return true;
    }

    @Override public boolean isDense() {
        return true;
    }

    @Override public boolean isRandomAccess() {
        return true;
    }

    @Override public boolean isDistributed() {
        return false;
    }

    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = 1;

        result = result * 37 + size;
        result = result * 37 + Double.hashCode(val);

        return result;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ConstantVectorStorage that = (ConstantVectorStorage) o;

        return size == that.size && val == that.val;
    }
}
