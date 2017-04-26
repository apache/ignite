// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

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

package org.apache.ignite.ml.math.impls.matrix;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.CacheUtils;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;

/**
 * Sparse distributed matrix implementation based on data grid.
 * <p>
 * Unlike {@link CacheMatrix} that is based on existing cache, this implementation creates distributed
 * cache internally and doesn't rely on pre-existing cache.</p>
 * <p>
 * You also need to call {@link #destroy()} to remove the underlying cache when you no longer need this
 * matrix.</p>
 * <p>
 * <b>Currently fold supports only commutative operations.<b/></p>
 */
public class SparseDistributedMatrix extends AbstractMatrix implements StorageConstants {
    /**
     *
     */
    public SparseDistributedMatrix() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     * @param stoMode Matrix storage mode.
     * @param acsMode Matrix elements access mode.
     */
    public SparseDistributedMatrix(int rows, int cols, int stoMode, int acsMode) {
        assert rows > 0;
        assert cols > 0;
        assertAccessMode(acsMode);
        assertStorageMode(stoMode);

        setStorage(new SparseDistributedMatrixStorage(rows, cols, stoMode, acsMode));
    }

    /**
     *
     *
     */
    private SparseDistributedMatrixStorage storage() {
        return (SparseDistributedMatrixStorage)getStorage();
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param d Value to divide to.
     */
    @Override public Matrix divide(double d) {
        return mapOverValues((Double v) -> v / d);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x Value to add.
     */
    @Override public Matrix plus(double x) {
        return mapOverValues((Double v) -> v + x);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x Value to multiply.
     */
    @Override public Matrix times(double x) {
        return mapOverValues((Double v) -> v * x);
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(double val) {
        return mapOverValues((Double v) -> val);
    }

    /** {@inheritDoc} */
    @Override public Matrix map(IgniteDoubleFunction<Double> fun) {
        return mapOverValues(fun::apply);
    }

    /**
     * @param mapper Mapping function.
     * @return Matrix with mapped values.
     */
    private Matrix mapOverValues(IgniteFunction<Double, Double> mapper) {
        CacheUtils.sparseMap(storage().cache().getName(), mapper);

        return this;
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        return CacheUtils.sparseSum(storage().cache().getName());
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        return CacheUtils.sparseMax(storage().cache().getName());
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        return CacheUtils.sparseMin(storage().cache().getName());
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }
}
