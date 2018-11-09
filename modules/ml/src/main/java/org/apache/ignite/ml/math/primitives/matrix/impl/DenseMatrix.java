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

import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.primitives.matrix.AbstractMatrix;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.OrderedMatrix;
import org.apache.ignite.ml.math.primitives.matrix.storage.DenseMatrixStorage;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

/**
 * Basic implementation for matrix.
 *
 * This is a trivial implementation for matrix assuming dense logic, local on-heap JVM storage
 * based on <code>double[][]</code> array. It is only suitable for data sets where
 * local, non-distributed execution is satisfactory and on-heap JVM storage is enough
 * to keep the entire data set.
 */
public class DenseMatrix extends AbstractMatrix implements OrderedMatrix {
    /**
     *
     */
    public DenseMatrix() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in matrix.
     * @param cols Amount of columns in matrix.
     */
    public DenseMatrix(int rows, int cols) {
        this(rows, cols, StorageConstants.ROW_STORAGE_MODE);
    }

    /**
     * @param rows Amount of rows in matrix.
     * @param cols Amount of columns in matrix.
     * @param acsMode Storage order (row or column-based).
     */
    public DenseMatrix(int rows, int cols, int acsMode) {
        assert rows > 0;
        assert cols > 0;

        setStorage(new DenseMatrixStorage(rows, cols, acsMode));
    }

    /**
     * @param mtx Backing data array.
     * @param acsMode Access mode.
     */
    public DenseMatrix(double[][] mtx, int acsMode) {
        assert mtx != null;

        setStorage(new DenseMatrixStorage(mtx, acsMode));
    }

    /**
     * @param mtx Backing data array.
     */
    public DenseMatrix(double[][] mtx) {
        this(mtx, StorageConstants.ROW_STORAGE_MODE);
    }

    /**
     * @param mtx Backing data array.
     * @param acsMode Access mode.
     */
    public DenseMatrix(double[] mtx, int rows, int acsMode) {
        assert mtx != null;

        setStorage(new DenseMatrixStorage(mtx, rows, acsMode));
    }

    /**
     * Build new matrix from flat raw array.
     */
    public DenseMatrix(double[] mtx, int rows) {
        this(mtx, StorageConstants.ROW_STORAGE_MODE, rows);
    }

    /** */
    private DenseMatrix(DenseMatrix orig) {
        this(orig, orig.accessMode());
    }

    /**
     * @param orig Original matrix.
     * @param acsMode Access mode.
     */
    private DenseMatrix(DenseMatrix orig, int acsMode) {
        assert orig != null;

        setStorage(new DenseMatrixStorage(orig.rowSize(), orig.columnSize(), acsMode));

        assign(orig);
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        return new DenseMatrix(this, accessMode());
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        return new DenseMatrix(rows, cols, getStorage() != null ? accessMode() : StorageConstants.ROW_STORAGE_MODE);
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        return new DenseVector(crd);
    }

    /** {@inheritDoc} */
    @Override public int accessMode() {
        return getStorage().accessMode();
    }
}
