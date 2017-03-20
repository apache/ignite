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
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.math.impls.matrix.RandomMatrix;
import org.apache.ignite.math.impls.vector.DenseLocalOnHeapVector;

/**
 * Calculates the LU-decomposition of a square matrix.
 *
 * This class inspired by class from Apache Common Math with similar name.
 *
 * @see <a href="http://mathworld.wolfram.com/LUDecomposition.html">MathWorld</a>
 * @see <a href="http://en.wikipedia.org/wiki/LU_decomposition">Wikipedia</a>
 */
public class LUDecomposition {
    /** Default bound to determine effective singularity in LU decomposition. */
    private static final double DEFAULT_TOO_SMALL = 1e-11;
    /** Pivot permutation associated with LU decomposition. */
    private final Vector pivot;
    /** Parity of the permutation associated with the LU decomposition. */
    private boolean even;
    /** Singularity indicator. */
    private boolean singular;
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
     * This constructor uses 1e-11 as default value for the singularity
     * threshold.
     *
     * @param matrix Matrix to decompose.
     * @throws CardinalityException if matrix is not square.
     */
    public LUDecomposition(Matrix matrix) {
        this(matrix, DEFAULT_TOO_SMALL);
    }

    /**
     * Calculates the LUP-decomposition of the given matrix.
     *
     * @param matrix Matrix to decompose.
     * @param singularityThreshold threshold (based on partial row norm).
     * @throws CardinalityException if matrix is not square.
     */
    public LUDecomposition(Matrix matrix, double singularityThreshold){
        int rows = matrix.rowSize();
        int cols = matrix.columnSize();

        if (rows != cols)
            throw new CardinalityException(rows, cols);

        this.matrix = matrix;

        luMatrix = copy(matrix);

        pivot = likeVector(matrix);
        for (int i = 0; i < pivot.size(); i++)
            pivot.setX(i, i);

        even     = true;
        singular = false;
        
        cachedL = null;
        cachedU = null;
        cachedP = null;

        for (int col = 0; col < cols; col++) {
            for (int row = 0; row < col; row++) {
                double sum = luMatrix.getX(row, col);

                for (int i = 0; i < row; i++)
                    sum -= luMatrix.getX(col, i) * luMatrix.getX(i, col);

                luMatrix.setX(row, col, sum);
            }

            int max = col;
            double largest = Double.NEGATIVE_INFINITY;

            for (int row = col; row < rows; row++) {
                double sum = luMatrix.getX(row, col);

                for (int i = 0; i < col; i++)
                    sum -= luMatrix.getX(col, i) * luMatrix.getX(i, col);

                luMatrix.setX(row, col, sum);

                if (Math.abs(sum) > largest){
                    largest = Math.abs(sum);
                    max = row;
                }
            }

            // Singularity check
            if (Math.abs(luMatrix.getX(max, col)) < singularityThreshold) {
                singular = true;
                return;
            }

            // Pivot if necessary
            if (max != col) {
                double tmp;
                Vector luMax = luMatrix.viewColumn(max);
                Vector luCol = luMatrix.viewColumn(col);

                for (int i = 0; i < cols; i++) {
                    tmp = luMax.getX(i);
                    luMax.setX(i, luCol.getX(i));
                    luCol.setX(i, tmp);
                }

                int temp = (int)pivot.getX(max);

                pivot.setX(max, pivot.getX(col));
                pivot.setX(col, temp);

                even = !even;
            }

            // Divide the lower elements by the "winning" diagonal elt.
            final double luDiag = luMatrix.getX(col, col);

            for (int row = col + 1; row < cols; row++)
                luMatrix.setX(row, col, luMatrix.getX(row, col) / luDiag) ;
        }
    }

    /**
     * Returns the matrix L of the decomposition.
     * <p>L is a lower-triangular matrix</p>
     * @return the L matrix (or null if decomposed matrix is singular).
     */
    public Matrix getL() {
        if ((cachedL == null) && !singular) {
            final int m = pivot.size();

            cachedL = like(matrix);
            cachedL.assign(0.0);

            for (int i = 0; i < m; ++i) {
                for (int j = 0; j < i; ++j)
                    cachedL.setX(i, j, luMatrix.getX(i, j));

                cachedL.setX(i, i, 1.0);
            }
        }

        return cachedL;
    }

    /**
     * Returns the matrix U of the decomposition.
     * <p>U is an upper-triangular matrix</p>
     * @return the U matrix (or null if decomposed matrix is singular).
     */
    public Matrix getU() {
        if ((cachedU == null) && !singular) {
            final int m = pivot.size();

            cachedU = like(matrix);
            cachedU.assign(0.0);

            for (int i = 0; i < m; ++i)
                for (int j = i; j < m; ++j)
                    cachedU.setX(i, j, luMatrix.getX(i, j));
        }

        return cachedU;
    }

    /**
     * Returns the P rows permutation matrix.
     * <p>P is a sparse matrix with exactly one element set to 1.0 in
     * each row and each column, all other elements being set to 0.0.</p>
     * <p>The positions of the 1 elements are given by the {@link #getPivot()
     * pivot permutation vector}.</p>
     * @return the P rows permutation matrix (or null if decomposed matrix is singular).
     * @see #getPivot()
     */
    public Matrix getP(){
        if ((cachedP == null) && !singular) {
            final int m = pivot.size();

            cachedU = like(matrix);
            cachedU.assign(0.0);

            for (int i = 0; i < m; ++i)
                cachedP.setX(i, (int)pivot.get(i), 1.0);
        }

        return cachedP;
    }

    /**
     * Returns the pivot permutation vector.
     * @return the pivot permutation vector.
     * @see #getP()
     */
    public Vector getPivot() {
        return pivot.copy();
    }

    /**
     * Return the determinant of the matrix.
     * @return determinant of the matrix.
     */
    public double determinant(){
        if (singular)
            return 0;

        final int m = pivot.size();
        double determinant = even ? 1 : -1;

        for (int i = 0; i < m; i++)
            determinant *= luMatrix.getX(i, i);

        return determinant;
    }

    /**
     * Create the like matrix with read-only matrices support.
     *
     * @param matrix Matrix for like.
     * @return Like matrix.
     */
    private Matrix like(Matrix matrix){
        if (matrix instanceof RandomMatrix)
            return new DenseLocalOnHeapMatrix(matrix.rowSize(), matrix.columnSize());
        else
            return matrix.like(matrix.rowSize(), matrix.columnSize());
    }

    /**
     * Create the like vector with read-only matrices support.
     *
     * @param matrix Matrix for like.
     * @return Like vector.
     */
    private Vector likeVector(Matrix matrix){
        if (matrix instanceof RandomMatrix)
            return new DenseLocalOnHeapVector(matrix.rowSize());
        else
            return matrix.likeVector(matrix.rowSize());
    }

    /**
     * Create the copy of matrix with read-only matrices support.
     *
     * @param matrix Matrix for copy.
     * @return Copy.
     */
    private Matrix copy(Matrix matrix){
        if (matrix instanceof RandomMatrix){
            DenseLocalOnHeapMatrix cp = new DenseLocalOnHeapMatrix(matrix.rowSize(), matrix.columnSize());

            cp.assign(matrix);

            return cp;
        } else
            return matrix.copy();
    }
}
