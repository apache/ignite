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

package org.apache.ignite.examples.ml.math.vector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.impls.storage.vector.ArrayVectorStorage;

/**
 * Example vector storage, modeled after {@link ArrayVectorStorage}.
 */
class ExampleVectorStorage implements VectorStorage {
    /** */
    private double[] data;

    /**
     * IMPL NOTE required by Externalizable.
     */
    public ExampleVectorStorage() {
        // No-op.
    }

    /**
     * @param data Backing data array.
     */
    ExampleVectorStorage(double[] data) {
        assert data != null;

        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return data == null ? 0 : data.length;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return data[i];
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        data[i] = v;
    }

    /** {@inheritDoc}} */
    @Override public boolean isArrayBased() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        return data;
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
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = (double[])in.readObject();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + Arrays.hashCode(data);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        ExampleVectorStorage that = (ExampleVectorStorage)obj;

        return Arrays.equals(data, (that.data));
    }
}
