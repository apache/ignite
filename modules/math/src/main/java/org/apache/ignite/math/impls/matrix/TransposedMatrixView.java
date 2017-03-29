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

package org.apache.ignite.math.impls.matrix;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.impls.storage.matrix.MatrixDelegateStorage;

/**
 * TODO: add description.
 */
public class TransposedMatrixView extends AbstractMatrix {
    /** */
    public TransposedMatrixView(){
        //No-op.
    }

    /**
     *
     */
    public TransposedMatrixView(Matrix mtx){
        assert mtx != null;

        setStorage(new MatrixDelegateStorage(mtx.getStorage(), 0, 0, mtx.rowSize(), mtx.columnSize()));
    }

    /** {@inheritDoc} */
    @Override public double get(int row, int col) {
        return super.get(col, row);
    }

    /** {@inheritDoc} */
    @Override public double getX(int row, int col) {
        return super.getX(col, row);
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return super.columnSize();
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return super.rowSize();
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
