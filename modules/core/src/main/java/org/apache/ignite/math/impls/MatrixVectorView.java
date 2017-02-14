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

package org.apache.ignite.math.impls;

import org.apache.ignite.math.*;
import org.apache.ignite.math.impls.storage.*;

/**
 * Row or column vector view off the matrix.
 */
public class MatrixVectorView extends AbstractVector {
    private Matrix parent;

    private int row, col;
    private int rowStride, colStride;

    /**
     *
     */
    public MatrixVectorView() {
        // No-op.
    }

    /**
     * 
     * @param parent
     * @param row
     * @param col
     * @param rowStride
     * @param colStride
     */
    public MatrixVectorView(Matrix parent, int row, int col, int rowStride, int colStride) {
        if (row < 0 || row >= parent.rowSize())
            throw new IndexException(row);
        if (col < 0 || col >= parent.columnSize())
            throw new IndexException(col);

        this.parent = parent;

        this.row = row;
        this.col = col;

        this.rowStride = rowStride;
        this.colStride = colStride;

        setStorage(new MatrixVectorStorage(parent, row, col, rowStride, colStride));
    }

    @Override
    public Vector copy() {
        return null; // TODO
    }

    @Override
    public Vector like(int crd) {
        return null; // TODO
    }

    @Override
    public Matrix likeMatrix(int rows, int cols) {
        return null; // TODO
    }
}
