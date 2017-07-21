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

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.impls.matrix.FunctionMatrix;

/**
 * This class provides a helper implementation of the read-only implementation of {@link Vector}
 * interface to minimize the effort required to implement it.
 * Subclasses may override some of the implemented methods if a more
 * specific or optimized implementation is desirable.
 */
public abstract class AbstractReadOnlyVector extends AbstractVector {
    /** */
    public AbstractReadOnlyVector() {
        // No-op.
    }

    /**
     * @param sto Storage.
     */
    public AbstractReadOnlyVector(VectorStorage sto) {
        super(true, sto);
    }

    /** {@inheritDoc} */
    @Override public Matrix cross(Vector vec) {
        return new FunctionMatrix(size(), vec.size(),
            (row, col) -> vec.get(col) * get(row));
    }

    /** {@inheritDoc} */
    @Override public Matrix toMatrix(boolean rowLike) {
        return new FunctionMatrix(rowLike ? 1 : size(), rowLike ? size() : 1,
            (row, col) -> rowLike ? get(col) : get(row));
    }

    /** {@inheritDoc} */
    @Override public Matrix toMatrixPlusOne(boolean rowLike, double zeroVal) {
        return new FunctionMatrix(rowLike ? 1 : size() + 1, rowLike ? size() + 1 : 1, (row, col) -> {
            if (row == 0 && col == 0)
                return zeroVal;

            return rowLike ? get(col - 1) : get(row - 1);
        });
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        return this; // This exploits read-only feature of this type vector.
    }

    /** {@inheritDoc} */
    @Override public Vector logNormalize() {
        return logNormalize(2.0, Math.sqrt(getLengthSquared()));
    }

    /** {@inheritDoc} */
    @Override public Vector logNormalize(double power) {
        return logNormalize(power, kNorm(power));
    }

    /** {@inheritDoc} */
    @Override public Vector map(IgniteDoubleFunction<Double> fun) {
        return new FunctionVector(size(), (i) -> fun.apply(get(i)));
    }

    /** {@inheritDoc} */
    @Override public Vector map(Vector vec, IgniteBiFunction<Double, Double, Double> fun) {
        checkCardinality(vec);

        return new FunctionVector(size(), (i) -> fun.apply(get(i), vec.get(i)));
    }

    /** {@inheritDoc} */
    @Override public Vector map(IgniteBiFunction<Double, Double, Double> fun, double y) {
        return new FunctionVector(size(), (i) -> fun.apply(get(i), y));
    }

    /** {@inheritDoc} */
    @Override public Vector divide(double x) {
        if (x == 1.0)
            return this;

        return new FunctionVector(size(), (i) -> get(i) / x);
    }

    /** {@inheritDoc} */
    @Override public Vector times(double x) {
        return x == 0 ? new ConstantVector(size(), 0) : new FunctionVector(size(), (i) -> get(i) * x);
    }

    /**
     * @param power Power.
     * @param normLen Normalized length.
     * @return logNormalized value.
     */
    private Vector logNormalize(double power, double normLen) {
        assert !(Double.isInfinite(power) || power <= 1.0);

        double denominator = normLen * Math.log(power);

        return new FunctionVector(size(), (idx) -> Math.log1p(get(idx)) / denominator);
    }
}
