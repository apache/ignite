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

package org.apache.ignite.math.decompositions;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.exceptions.CardinalityException;

/**
 * Calculates the LU-decomposition of a square matrix.
 *
 * TODO: add description.
 * TODO: WIP
 */
public class LUDecomposition {
    /** Pivot permutation associated with LU decomposition. */
    private final Vector pivot;
    /** Cached value of L. */
    private Matrix cachedL;
    /** Cached value of U. */
    private Matrix cachedU;
    /** Cached value of P. */
    private Matrix cachedP;
    /** Original matrix. */
    private Matrix matrix;
    /** Entries of LU decomposition. */
    private Matrix luMatrix;

    /**
     * Calculates the LU-decomposition of the given matrix.
     *
     * @param matrix Matrix to decompose.
     * @throws CardinalityException if matrix is not square.
     */
    public LUDecomposition(Matrix matrix){
        int rows = matrix.rowSize();
        int cols = matrix.columnSize();

        if (rows != cols)
            throw new CardinalityException(rows, cols);

        this.matrix = matrix;

        luMatrix = matrix.copy();
        pivot = matrix.likeVector(cols);
        cachedL = null;
        cachedU = null;
        cachedP = null;

        for (int col = 0; col < cols; col++) {
            for (int row = 0; row < col; row++) {
                double sum = 0;

                for (int i = 0; i < row; i++)
                    sum -= luMatrix.getX(col, i) * luMatrix.getX(i, col);

                luMatrix.setX(row, col, sum);
            }

            for (int row = col; row < rows; row++) {
                //TODO
            }
        }
    }

    public Matrix getCachedL() {
        return cachedL;
    }

    public Matrix getCachedU() {
        return cachedU;
    }

    public Matrix getCachedP(){
        return cachedP;
    }

    public double determinant(){
        return 0d;
    }
}
