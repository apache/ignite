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

package org.apache.ignite.ml.math.primitives.matrix.impl;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.matrix.AbstractMatrix;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.MatrixStorage;
import org.apache.ignite.ml.math.primitives.matrix.storage.SparseMatrixStorage;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;

/**
 * Sparse local onheap matrix with {@link SparseVector} as rows.
 */
public class SparseMatrix extends AbstractMatrix implements StorageConstants {
    /**
     *
     */
    public SparseMatrix() {
        // No-op.
    }

    /**
     * Construct new {@link SparseMatrix}.
     *
     * By default storage sets in row optimized mode and in random access mode.
     */
    public SparseMatrix(int rows, int cols) {
        setStorage(mkStorage(rows, cols));
    }

    /**
     * Create new {@link SparseMatrixStorage}.
     */
    private MatrixStorage mkStorage(int rows, int cols) {
        return new SparseMatrixStorage(rows, cols, StorageConstants.RANDOM_ACCESS_MODE, StorageConstants.ROW_STORAGE_MODE);
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        return new SparseMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        return new SparseVector(crd, StorageConstants.RANDOM_ACCESS_MODE);
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
        return ((SparseMatrixStorage)getStorage()).indexesMap();
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        Matrix cp = like(rowSize(), columnSize());

        cp.assign(this);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public void compute(int row, int col, IgniteTriFunction<Integer, Integer, Double, Double> f) {
        ((SparseMatrixStorage)getStorage()).compute(row, col, f);
    }
}
