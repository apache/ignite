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
import org.apache.ignite.ml.math.OrderedMatrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.storage.matrix.ArrayMatrixStorage;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/**
 * Basic implementation for matrix.
 *
 * This is a trivial implementation for matrix assuming dense logic, local on-heap JVM storage
 * based on <code>double[][]</code> array. It is only suitable for data sets where
 * local, non-distributed execution is satisfactory and on-heap JVM storage is enough
 * to keep the entire data set.
 */
public class DenseLocalOnHeapMatrix extends AbstractMatrix implements OrderedMatrix {
    /**
     *
     */
    public DenseLocalOnHeapMatrix() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in matrix.
     * @param cols Amount of columns in matrix.
     */
    public DenseLocalOnHeapMatrix(int rows, int cols) {
        this(rows, cols, StorageConstants.ROW_STORAGE_MODE);
    }

    /**
     * @param rows Amount of rows in matrix.
     * @param cols Amount of columns in matrix.
     * @param acsMode Storage order (row or column-based).
     */
    public DenseLocalOnHeapMatrix(int rows, int cols, int acsMode) {
        assert rows > 0;
        assert cols > 0;

        setStorage(new ArrayMatrixStorage(rows, cols, acsMode));
    }

    /**
     * @param mtx Backing data array.
     * @param acsMode Access mode.
     */
    public DenseLocalOnHeapMatrix(double[][] mtx, int acsMode) {
        assert mtx != null;

        setStorage(new ArrayMatrixStorage(mtx, acsMode));
    }

    /**
     * @param mtx Backing data array.
     */
    public DenseLocalOnHeapMatrix(double[][] mtx) {
        this(mtx, StorageConstants.ROW_STORAGE_MODE);
    }

    /**
     * @param mtx Backing data array.
     * @param acsMode Access mode.
     */
    public DenseLocalOnHeapMatrix(double[] mtx, int rows, int acsMode) {
        assert mtx != null;

        setStorage(new ArrayMatrixStorage(mtx, rows, acsMode));
    }

    /**
     * Build new matrix from flat raw array.
     */
    public DenseLocalOnHeapMatrix(double[] mtx, int rows) {
        this(mtx, StorageConstants.ROW_STORAGE_MODE, rows);
    }

    /** */
    private DenseLocalOnHeapMatrix(DenseLocalOnHeapMatrix orig) {
        this(orig, orig.accessMode());
    }

    /**
     * @param orig Original matrix.
     * @param acsMode Access mode.
     */
    private DenseLocalOnHeapMatrix(DenseLocalOnHeapMatrix orig, int acsMode) {
        assert orig != null;

        setStorage(new ArrayMatrixStorage(orig.rowSize(), orig.columnSize(), acsMode));

        assign(orig);
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        return new DenseLocalOnHeapMatrix(this, accessMode());
    }

    /** {@inheritDoc} */
    @Override public Matrix like(int rows, int cols) {
        return new DenseLocalOnHeapMatrix(rows, cols, getStorage() != null ? accessMode() : StorageConstants.ROW_STORAGE_MODE);
    }

    /** {@inheritDoc} */
    @Override public Vector likeVector(int crd) {
        return new DenseLocalOnHeapVector(crd);
    }

    /** {@inheritDoc} */
    @Override public int accessMode() {
        return getStorage().accessMode();
    }
}
