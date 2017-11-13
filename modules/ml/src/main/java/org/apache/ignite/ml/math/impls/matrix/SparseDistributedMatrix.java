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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
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

            IgniteCache<RowColMatrixKey, BlockEntry> cache = ignite.getOrCreateCache(cacheName);
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
                
                if (isRowMode){
                    Vector Aik = matrixA.getCol(idx);

                    for (int i = 0; i < columnSize(); i++) {
                        Vector Bkj = matrixB.getRow(i);
                        matrixC.set(idx, i, Aik.times(Bkj).sum());
                    }
                } else {
                    Vector Bkj = matrixB.getRow(idx);

                    for (int i = 0; i < rowSize(); i++) {
                        Vector Aik = matrixA.getCol(i);
                        matrixC.set(idx, i, Aik.times(Bkj).sum());
                    }
                }
            });
        });

        return matrixC;
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
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        return new SparseDistributedMatrix(rows, cols, storage().storageMode(), storage().accessMode());
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        throw new UnsupportedOperationException();
    }

    /** */
    public UUID getUUID() {
        return ((SparseDistributedMatrixStorage)getStorage()).getUUID();
    }
}
