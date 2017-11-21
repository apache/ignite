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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;
import org.apache.ignite.ml.math.impls.storage.vector.SparseDistributedVectorStorage;
import org.apache.ignite.ml.math.impls.vector.SparseDistributedVector;

/**
 * Sparse distributed matrix implementation based on data grid. <p> Unlike {@link CacheMatrix} that is based on existing
 * cache, this implementation creates distributed cache internally and doesn't rely on pre-existing cache.</p> <p> You
 * also need to call {@link #destroy()} to remove the underlying cache when you no longer need this matrix.</p> <p>
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
     * @param data Data to fill the matrix
     */
    public SparseDistributedMatrix(double[][] data) {
        assert data.length > 0;
        setStorage(new SparseDistributedMatrixStorage(data.length, getMaxAmountOfColumns(data), StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE));

        for (int i = 0; i < data.length; i++)
            for (int j = 0; j < data[i].length; j++)
                storage().set(i, j, data[i][j]);
    }

    /**
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     */
    public SparseDistributedMatrix(int rows, int cols) {
        this(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
    }

    /** */
    private SparseDistributedMatrixStorage storage() {
        return (SparseDistributedMatrixStorage)getStorage();
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param d Value to divide to.
     */
    @Override public Matrix divide(double d) {
        return mapOverValues(v -> v / d);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x Value to add.
     */
    @Override public Matrix plus(double x) {
        return mapOverValues(v -> v + x);
    }

    /**
     * Return the same matrix with updates values (broken contract).
     *
     * @param x Value to multiply.
     */
    @Override public Matrix times(double x) {
        return mapOverValues(v -> v * x);
    }

    /** {@inheritDoc} */
    @Override public Matrix times(Matrix mtx) {
        if (mtx == null)
            throw new IllegalArgumentException("The matrix should be not null.");

        if (columnSize() != mtx.rowSize())
            throw new CardinalityException(columnSize(), mtx.rowSize());

        SparseDistributedMatrix matrixA = this;
        SparseDistributedMatrix matrixB = (SparseDistributedMatrix)mtx;

        String cacheName = storage().cacheName();
        SparseDistributedMatrix matrixC = new SparseDistributedMatrix(matrixA.rowSize(), matrixB.columnSize()
            , getStorage().storageMode(), getStorage().isRandomAccess() ? RANDOM_ACCESS_MODE : SEQUENTIAL_ACCESS_MODE);

        CacheUtils.bcast(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            Affinity<RowColMatrixKey> affinity = ignite.affinity(cacheName);

            ClusterNode locNode = ignite.cluster().localNode();

            SparseDistributedMatrixStorage storageC = matrixC.storage();

            Map<ClusterNode, Collection<RowColMatrixKey>> keysCToNodes = affinity.mapKeysToNodes(storageC.getAllKeys());
            Collection<RowColMatrixKey> locKeys = keysCToNodes.get(locNode);

            boolean isRowMode = storageC.storageMode() == ROW_STORAGE_MODE;

            if (locKeys == null)
                return;

            // compute Cij locally on each node
            // TODO: IGNITE:5114, exec in parallel
            locKeys.forEach(key -> {
                int idx = key.index();

                if (isRowMode) {
                    Vector Aik = matrixA.getRow(idx);

                    for (int i = 0; i < matrixB.columnSize(); i++) {
                        Vector Bkj = matrixB.getCol(i);
                        matrixC.set(idx, i, Aik.times(Bkj).sum());
                    }
                }
                else {
                    Vector Bkj = matrixB.getCol(idx);

                    for (int i = 0; i < matrixA.rowSize(); i++) {
                        Vector Aik = matrixA.getRow(i);
                        matrixC.set(idx, i, Aik.times(Bkj).sum());
                    }
                }
            });
        });

        return matrixC;
    }

    /** {@inheritDoc} */
    @Override public Vector times(Vector vec) {
        if (vec == null)
            throw new IllegalArgumentException("The vector should be not null.");

        if (columnSize() != vec.size())
            throw new CardinalityException(columnSize(), vec.size());

        SparseDistributedMatrix matrixA = this;
        SparseDistributedVector vectorB = (SparseDistributedVector)vec;

        String cacheName = storage().cacheName();
        int rows = this.rowSize();

        SparseDistributedVector vectorC = (SparseDistributedVector)likeVector(rows);

        CacheUtils.bcast(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            Affinity<RowColMatrixKey> affinity = ignite.affinity(cacheName);

            ClusterNode locNode = ignite.cluster().localNode();

            SparseDistributedVectorStorage storageC = vectorC.storage();

            Map<ClusterNode, Collection<RowColMatrixKey>> keysCToNodes = affinity.mapKeysToNodes(storageC.getAllKeys());
            Collection<RowColMatrixKey> locKeys = keysCToNodes.get(locNode);

            if (locKeys == null)
                return;

            // compute Cij locally on each node
            // TODO: IGNITE:5114, exec in parallel
            locKeys.forEach(key -> {
                int idx = key.index();
                Vector Aik = matrixA.getRow(idx);
                vectorC.set(idx, Aik.times(vectorB).sum());
            });
        });

        return vectorC;
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(double val) {
        return mapOverValues(v -> val);
    }

    /** {@inheritDoc} */
    @Override public Matrix map(IgniteDoubleFunction<Double> fun) {
        return mapOverValues(fun);
    }

    /**
     * @param mapper Mapping function.
     * @return Matrix with mapped values.
     */
    private Matrix mapOverValues(IgniteDoubleFunction<Double> mapper) {
        CacheUtils.sparseMap(getUUID(), mapper, storage().cacheName());

        return this;
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        return CacheUtils.sparseSum(getUUID(), storage().cacheName());
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        return CacheUtils.sparseMax(getUUID(), storage().cacheName());
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        return CacheUtils.sparseMin(getUUID(), storage().cacheName());
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        Matrix cp = like(rowSize(), columnSize());

        cp.assign(this);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        if (storage() == null)
            return new SparseDistributedMatrix(rows, cols);
        else
            return new SparseDistributedMatrix(rows, cols, storage().storageMode(), storage().accessMode());

    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        return new SparseDistributedVector(crd, StorageConstants.RANDOM_ACCESS_MODE);
    }

    /** */
    public UUID getUUID() {
        return ((SparseDistributedMatrixStorage)getStorage()).getUUID();
    }
}
