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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distributed.CacheUtils;
import org.apache.ignite.ml.math.distributed.keys.impl.MatrixBlockKey;
import org.apache.ignite.ml.math.distributed.keys.impl.VectorBlockKey;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.impls.storage.matrix.BlockMatrixStorage;
import org.apache.ignite.ml.math.impls.storage.matrix.BlockVectorStorage;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVector;
import org.apache.ignite.ml.math.impls.vector.SparseDistributedVector;
import org.apache.ignite.ml.math.impls.vector.VectorBlockEntry;

/**
 * Sparse block distributed matrix. This matrix represented by blocks 32x32 {@link MatrixBlockEntry}.
 *
 * Using separate cache with keys {@link MatrixBlockKey} and values {@link MatrixBlockEntry}.
 */
public class SparseBlockDistributedMatrix extends AbstractMatrix implements StorageConstants {
    /**
     *
     */
    public SparseBlockDistributedMatrix() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     */
    public SparseBlockDistributedMatrix(int rows, int cols) {
        assert rows > 0;
        assert cols > 0;

        setStorage(new BlockMatrixStorage(rows, cols));
    }

    /**
     * @param data Data to fill the matrix
     */
    public SparseBlockDistributedMatrix(double[][] data) {
        assert data.length > 0;

        setStorage(new BlockMatrixStorage(data.length, getMaxAmountOfColumns(data)));

        for (int i = 0; i < data.length; i++)
            for (int j = 0; j < data[i].length; j++)
                storage().set(i, j, data[i][j]);
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

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"unchecked"})
    @Override public Matrix times(final Matrix mtx) {
        if (mtx == null)
            throw new IllegalArgumentException("The matrix should be not null.");

        if (columnSize() != mtx.rowSize())
            throw new CardinalityException(columnSize(), mtx.rowSize());

        SparseBlockDistributedMatrix matrixA = this;
        SparseBlockDistributedMatrix matrixB = (SparseBlockDistributedMatrix)mtx;

        String cacheName = this.storage().cacheName();
        SparseBlockDistributedMatrix matrixC = new SparseBlockDistributedMatrix(matrixA.rowSize(), matrixB.columnSize());

        CacheUtils.bcast(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            Affinity<MatrixBlockKey> affinity = ignite.affinity(cacheName);

            IgniteCache<MatrixBlockKey, MatrixBlockEntry> cache = ignite.getOrCreateCache(cacheName);
            ClusterNode locNode = ignite.cluster().localNode();

            BlockMatrixStorage storageC = matrixC.storage();

            Map<ClusterNode, Collection<MatrixBlockKey>> keysCToNodes = affinity.mapKeysToNodes(storageC.getAllKeys());
            Collection<MatrixBlockKey> locKeys = keysCToNodes.get(locNode);

            if (locKeys == null)
                return;

            // compute Cij locally on each node
            // TODO: IGNITE:5114, exec in parallel
            locKeys.forEach(key -> {
                long newBlockIdRow = key.blockRowId();
                long newBlockIdCol = key.blockColId();

                IgnitePair<Long> newBlockId = new IgnitePair<>(newBlockIdRow, newBlockIdCol);

                MatrixBlockEntry blockC = null;

                List<MatrixBlockEntry> aRow = matrixA.storage().getRowForBlock(newBlockId);
                List<MatrixBlockEntry> bCol = matrixB.storage().getColForBlock(newBlockId);

                for (int i = 0; i < aRow.size(); i++) {
                    MatrixBlockEntry blockA = aRow.get(i);
                    MatrixBlockEntry blockB = bCol.get(i);

                    MatrixBlockEntry tmpBlock = new MatrixBlockEntry(blockA.times(blockB));

                    blockC = blockC == null ? tmpBlock : new MatrixBlockEntry(blockC.plus(tmpBlock));
                }

                cache.put(storageC.getCacheKey(newBlockIdRow, newBlockIdCol), blockC);
            });
        });

        return matrixC;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"unchecked"})
    @Override public Vector times(final Vector vec) {
        if (vec == null)
            throw new IllegalArgumentException("The vector should be not null.");

        if (columnSize() != vec.size())
            throw new CardinalityException(columnSize(), vec.size());

        SparseBlockDistributedMatrix matrixA = this;
        SparseBlockDistributedVector vectorB = (SparseBlockDistributedVector)vec;

        String cacheName = this.storage().cacheName();
        SparseBlockDistributedVector vectorC = new SparseBlockDistributedVector(matrixA.rowSize());

        CacheUtils.bcast(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            Affinity<VectorBlockKey> affinity = ignite.affinity(cacheName);

            IgniteCache<VectorBlockKey, VectorBlockEntry> cache = ignite.getOrCreateCache(cacheName);
            ClusterNode locNode = ignite.cluster().localNode();

            BlockVectorStorage storageC = vectorC.storage();

            Map<ClusterNode, Collection<VectorBlockKey>> keysCToNodes = affinity.mapKeysToNodes(storageC.getAllKeys());
            Collection<VectorBlockKey> locKeys = keysCToNodes.get(locNode);

            if (locKeys == null)
                return;

            // compute Cij locally on each node
            // TODO: IGNITE:5114, exec in parallel
            locKeys.forEach(key -> {
                long newBlockId = key.blockId();

                IgnitePair<Long> newBlockIdForMtx = new IgnitePair<>(newBlockId, 0L);

                VectorBlockEntry blockC = null;

                List<MatrixBlockEntry> aRow = matrixA.storage().getRowForBlock(newBlockIdForMtx);
                List<VectorBlockEntry> bCol = vectorB.storage().getColForBlock(newBlockId);

                for (int i = 0; i < aRow.size(); i++) {
                    MatrixBlockEntry blockA = aRow.get(i);
                    VectorBlockEntry blockB = bCol.get(i);

                    VectorBlockEntry tmpBlock = new VectorBlockEntry(blockA.times(blockB));

                    blockC = blockC == null ? tmpBlock : new VectorBlockEntry(blockC.plus(tmpBlock));
                }

                cache.put(storageC.getCacheKey(newBlockId), blockC);
            });
        });
        return vectorC;
    }

    /** {@inheritDoc} */
    @Override public Vector getCol(int col) {
        checkColumnIndex(col);

        Vector res = new SparseDistributedVector(rowSize());

        for (int i = 0; i < rowSize(); i++)
            res.setX(i, getX(i, col));
        return res;
    }

    /** {@inheritDoc} */
    @Override public Vector getRow(int row) {
        checkRowIndex(row);

        Vector res = new SparseDistributedVector(columnSize());

        for (int i = 0; i < columnSize(); i++)
            res.setX(i, getX(row, i));
        return res;
    }

    /** {@inheritDoc} */
    @Override public Matrix assign(double val) {
        return mapOverValues(v -> val);
    }

    /** {@inheritDoc} */
    @Override public Matrix map(IgniteDoubleFunction<Double> fun) {
        return mapOverValues(fun);
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        return CacheUtils.sparseSum(getUUID(), this.storage().cacheName());
    }

    /** {@inheritDoc} */
    @Override public double maxValue() {
        return CacheUtils.sparseMax(getUUID(), this.storage().cacheName());
    }

    /** {@inheritDoc} */
    @Override public double minValue() {
        return CacheUtils.sparseMin(getUUID(), this.storage().cacheName());
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        Matrix cp = like(rowSize(), columnSize());

        cp.assign(this);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        return new SparseBlockDistributedMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        return new SparseBlockDistributedVector(crd);
    }

    /** */
    private UUID getUUID() {
        return ((BlockMatrixStorage)getStorage()).getUUID();
    }

    /**
     * @param mapper Mapping function.
     * @return Matrix with mapped values.
     */
    private Matrix mapOverValues(IgniteDoubleFunction<Double> mapper) {
        CacheUtils.sparseMap(getUUID(), mapper, this.storage().cacheName());

        return this;
    }

    /**
     *
     */
    private BlockMatrixStorage storage() {
        return (BlockMatrixStorage)getStorage();
    }
}
