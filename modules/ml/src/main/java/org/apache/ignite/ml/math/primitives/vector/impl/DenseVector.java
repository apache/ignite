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

package org.apache.ignite.ml.math.primitives.vector.impl;

import java.util.Map;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.AbstractVector;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorStorage;
import org.apache.ignite.ml.math.primitives.vector.storage.DenseVectorStorage;

/**
 * Basic implementation for vector.
 * <p>
 * This is a trivial implementation for vector assuming dense logic, local on-heap JVM storage
 * based on {@code double[]} array. It is only suitable for data sets where
 * local, non-distributed execution is satisfactory and on-heap JVM storage is enough
 * to keep the entire data set.
 */
public class DenseVector extends AbstractVector {
    /**
     * @param size Vector cardinality.
     */
    private VectorStorage mkStorage(int size) {
        return new DenseVectorStorage(size);
    }

    /**
     * @param arr Source array.
     * @param cp {@code true} to clone array, reuse it otherwise.
     */
    private VectorStorage mkStorage(double[] arr, boolean cp) {
        assert arr != null;

        return new DenseVectorStorage(cp ? arr.clone() : arr);
    }

    /**
     * @param args Parameters for new Vector.
     */
    public DenseVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size"))
            setStorage(mkStorage((int)args.get("size")));
        else if (args.containsKey("arr") && args.containsKey("copy"))
            setStorage(mkStorage((double[])args.get("arr"), (boolean)args.get("copy")));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /** */
    public DenseVector() {
        // No-op.
    }

    /**
     * @param size Vector cardinality.
     */
    public DenseVector(int size) {
        setStorage(mkStorage(size));
    }

    /**
     * @param arr Source array.
     * @param shallowCp {@code true} to use shallow copy.
     */
    public DenseVector(double[] arr, boolean shallowCp) {
        setStorage(mkStorage(arr, shallowCp));
    }

    /**
     * @param arr Source array.
     */
    public DenseVector(double[] arr) {
        this(arr, false);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return new DenseMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new DenseVector(crd);
    }
}
