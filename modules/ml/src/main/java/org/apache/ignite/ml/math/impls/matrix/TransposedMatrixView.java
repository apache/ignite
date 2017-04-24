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
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.storage.matrix.MatrixDelegateStorage;

/**
 * Implements transposed view of the parent {@link Matrix}.
 */
public class TransposedMatrixView extends AbstractMatrix {
    /** */
    public TransposedMatrixView() {
        //No-op.
    }

    /**
     * @param mtx Parent matrix.
     */
    public TransposedMatrixView(Matrix mtx) {
        this(mtx == null ? null : mtx.getStorage());
    }

    /** */
    private TransposedMatrixView(MatrixStorage sto) {
        super(new MatrixDelegateStorage(sto, 0, 0,
            sto == null ? 0 : sto.rowSize(), sto == null ? 0 : sto.columnSize()));
    }

    /** {@inheritDoc} */
    @Override protected void storageSet(int row, int col, double v) {
        super.storageSet(col, row, v);
    }

    /** {@inheritDoc} */
    @Override protected double storageGet(int row, int col) {
        return super.storageGet(col, row);
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return getStorage().columnSize();
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return getStorage().rowSize();
    }

    /** {@inheritDoc} */
    @Override public Matrix copy() {
        MatrixDelegateStorage sto = (MatrixDelegateStorage)getStorage();

        return new TransposedMatrixView(sto.delegate());
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
