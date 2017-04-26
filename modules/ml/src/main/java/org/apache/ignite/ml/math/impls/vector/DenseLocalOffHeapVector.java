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

import java.util.stream.IntStream;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOffHeapMatrix;
import org.apache.ignite.ml.math.impls.storage.vector.DenseLocalOffHeapVectorStorage;

/**
 * Implementation for {@link Vector} assuming dense logic and local offheap JVM storage.
 * It is suitable for data sets where local, non-distributed execution is satisfactory and on-heap JVM storage
 * is not enough to keep the entire data set.
 */
public class DenseLocalOffHeapVector extends AbstractVector {
    /** */
    public DenseLocalOffHeapVector() {
        // No-op.
    }

    /** */
    private void makeOffheapStorage(int size) {
        setStorage(new DenseLocalOffHeapVectorStorage(size));
    }

    /**
     * @param arr Array to copy to offheap storage.
     */
    public DenseLocalOffHeapVector(double[] arr) {
        makeOffheapStorage(arr.length);

        assign(arr);
    }

    /**
     * @param size Vector cardinality.
     */
    public DenseLocalOffHeapVector(int size) {
        makeOffheapStorage(size);
    }

    /** {@inheritDoc} */
    @Override public Vector assign(Vector vec) {
        checkCardinality(vec);

        IntStream.range(0, size()).parallel().forEach(idx -> set(idx, vec.get(idx)));

        return this;
    }

    /** {@inheritDoc} */
    @Override public Vector times(double x) {
        if (x == 0.0)
            return like(size()).assign(0);
        else
            return super.times(x);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new DenseLocalOffHeapVector(crd);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return new DenseLocalOffHeapMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return o != null && getClass().equals(o.getClass()) && (getStorage().equals(((Vector)o).getStorage()));
    }
}
