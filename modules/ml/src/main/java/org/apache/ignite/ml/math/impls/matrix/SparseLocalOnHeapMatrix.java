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

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseLocalOnHeapMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.SparseLocalVector;

/**
 * Sparse local onheap matrix with {@link SparseLocalVector} as rows.
 */
public class SparseLocalOnHeapMatrix extends AbstractMatrix implements StorageConstants {
    /**
     *
     */
    public SparseLocalOnHeapMatrix() {
        // No-op.
    }

    /**
     * Construct new {@link SparseLocalOnHeapMatrix}.
     *
     * By default storage sets in row optimized mode and in random access mode.
     */
    public SparseLocalOnHeapMatrix(int rows, int cols) {
        setStorage(mkStorage(rows, cols));
    }

    /**
     * Create new {@link SparseLocalOnHeapMatrixStorage}.
     */
    private MatrixStorage mkStorage(int rows, int cols) {
        return new SparseLocalOnHeapMatrixStorage(rows, cols, StorageConstants.RANDOM_ACCESS_MODE, StorageConstants.ROW_STORAGE_MODE);
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        return new SparseLocalOnHeapMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        return new SparseLocalVector(crd, StorageConstants.RANDOM_ACCESS_MODE);
    }

    /** {@inheritDoc} */
    @Override public int nonZeroElements() {
        int res = 0;
        IntIterator rowIter = indexesMap().keySet().iterator();

        while (rowIter.hasNext()) {
            int row = rowIter.nextInt();
            res += indexesMap().get(row).size();
        }

        return res;
    }

    /** */
    public Int2ObjectArrayMap<IntSet> indexesMap() {
        return ((SparseLocalOnHeapMatrixStorage)getStorage()).indexesMap();
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        Matrix cp = like(rowSize(), columnSize());

        cp.assign(this);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public void compute(int row, int col, IgniteTriFunction<Integer, Integer, Double, Double> f) {
        ((SparseLocalOnHeapMatrixStorage)getStorage()).compute(row, col, f);
    }
}
