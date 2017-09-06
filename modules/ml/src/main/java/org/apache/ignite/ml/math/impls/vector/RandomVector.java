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

package org.apache.ignite.ml.math.impls.vector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.matrix.RandomMatrix;
import org.apache.ignite.ml.math.impls.storage.vector.RandomVectorStorage;

/**
 * Random vector. Each value is taken from {-1,0,1} with roughly equal probability. Note
 * that by default, the value is determined by a relatively simple hash of the index.
 */
public class RandomVector extends AbstractReadOnlyVector {
    /** */
    private boolean fastHash;

    /**
     * @param size Vector cardinality.
     * @param fastHash Whether or not to use fast hashing or Murmur hashing.
     */
    private VectorStorage mkStorage(int size, boolean fastHash) {
        this.fastHash = fastHash;

        return new RandomVectorStorage(size, fastHash);
    }

    /**
     * @param size Vector cardinality.
     * @param fastHash Whether or not to use fast hashing or Murmur hashing.
     */
    public RandomVector(int size, boolean fastHash) {
        setStorage(mkStorage(size, fastHash));
    }

    /**
     * @param size Vector cardinality.
     */
    public RandomVector(int size) {
        this(size, true);
    }

    /**
     * @param args Parameters to create new vector instance.
     */
    public RandomVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size") && args.containsKey("fastHash"))
            setStorage(mkStorage((int)args.get("size"), (boolean)args.get("fastHash")));
        else if (args.containsKey("size"))
            setStorage(mkStorage((int)args.get("size"), true));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /** */
    public RandomVector() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new RandomVector(crd, fastHash);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return new RandomMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(fastHash);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        fastHash = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + Boolean.hashCode(fastHash);
        res = res * 37 + getStorage().hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        RandomVector that = (RandomVector)o;
        VectorStorage sto = getStorage();

        return fastHash == that.fastHash && (sto != null ? sto.equals(that.getStorage()) : that.getStorage() == null);
    }
}
