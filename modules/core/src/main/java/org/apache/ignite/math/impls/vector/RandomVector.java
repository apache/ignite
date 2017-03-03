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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.matrix.FunctionMatrix;
import org.apache.ignite.math.impls.matrix.RandomMatrix;
import org.apache.ignite.math.impls.storage.vector.RandomVectorStorage;

import java.io.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;

/**
 * Random vector. Each value is taken from {-1,0,1} with roughly equal probability. Note
 * that by default, the value is determined by a relatively simple hash of the index.
 */
public class RandomVector extends AbstractVector {
    /** */ private boolean fastHash;

    /**
     * @param size Vector cardinality.
     * @param fastHash
     */
    private VectorStorage mkStorage(int size, boolean fastHash) {
        this.fastHash = fastHash;

        return new RandomVectorStorage(size, fastHash);
    }

    /**
     *
     * @param size
     * @param fastHash
     */
    public RandomVector(int size, boolean fastHash) {
        setStorage(mkStorage(size, fastHash));
    }

    /**
     *
     * @param size
     */
    public RandomVector(int size) {
        this(size, true);
    }

    /**
     * @param args
     */
    public RandomVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size") && args.containsKey("fastHash"))
            setStorage(mkStorage((int) args.get("size"), (boolean) args.get("fastHash")));
        else if (args.containsKey("size"))
            setStorage(mkStorage((int) args.get("size"), true));
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

            return rowLike ? get(col - 1) : get(row -1);
        });
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        return this; // IMPL NOTE this exploits read-only feature of this type vector
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
    @Override public Vector map(DoubleFunction<Double> fun) {
        return new FunctionVector(size(), (i) -> fun.apply(get(i)));
    }

    /** {@inheritDoc} */
    @Override public Vector map(Vector vec, BiFunction<Double, Double, Double> fun) {
        checkCardinality(vec);

        return new FunctionVector(size(), (i) -> fun.apply(get(i), vec.get(i)));
    }

    /** {@inheritDoc} */
    @Override public Vector map(BiFunction<Double, Double, Double> fun, double y) {
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
        return new FunctionVector(size(), (i) -> get(i) * x);
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
