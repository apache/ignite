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

package org.apache.ignite.ml.math.primitives.matrix;

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.exceptions.math.SingularMatrixException;
import org.apache.ignite.ml.math.primitives.vector.Vector;

import static org.apache.ignite.ml.math.util.MatrixUtil.copy;
import static org.apache.ignite.ml.math.util.MatrixUtil.like;
import static org.apache.ignite.ml.math.util.MatrixUtil.likeVector;

/**
 * Calculates the LU-decomposition of a square matrix.
 * <p>
 * This class is inspired by class from Apache Common Math with similar name.</p>
 *
 * @see <a href="http://mathworld.wolfram.com/LUDecomposition.html">MathWorld</a>
 * @see <a href="http://en.wikipedia.org/wiki/LU_decomposition">Wikipedia</a>
 *
 * <p>TODO: IGNITE-11192, remove after resolve this issue.</p>
 */
public class LUDecomposition implements AutoCloseable {
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
    private Matrix lu;

    /**
     * Calculates the LU-decomposition of the given matrix. This constructor uses 1e-11 as default value for the
     * singularity threshold.
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
    public LUDecomposition(Matrix matrix, double singularityThreshold) {
        assert matrix != null;

        int rows = matrix.rowSize();
        int cols = matrix.columnSize();

        if (rows != cols)
            throw new CardinalityException(rows, cols);

        this.matrix = matrix;

        lu = copy(matrix);

        pivot = likeVector(matrix);

        for (int i = 0; i < pivot.size(); i++)
            pivot.setX(i, i);

        even = true;
        singular = false;

        cachedL = null;
        cachedU = null;
        cachedP = null;

        for (int col = 0; col < cols; col++) {

            //upper
            for (int row = 0; row < col; row++) {
                Vector luRow = lu.viewRow(row);
                double sum = luRow.get(col);

                for (int i = 0; i < row; i++)
                    sum -= luRow.getX(i) * lu.getX(i, col);

                luRow.setX(col, sum);
            }

            // permutation row
            int max = col;

            double largest = Double.NEGATIVE_INFINITY;

            // lower
            for (int row = col; row < rows; row++) {
                Vector luRow = lu.viewRow(row);
                double sum = luRow.getX(col);

                for (int i = 0; i < col; i++)
                    sum -= luRow.getX(i) * lu.getX(i, col);

                luRow.setX(col, sum);

                if (Math.abs(sum) > largest) {
                    largest = Math.abs(sum);
                    max = row;
                }
            }

            // Singularity check
            if (Math.abs(lu.getX(max, col)) < singularityThreshold) {
                singular = true;
                return;
            }

            // Pivot if necessary
            if (max != col) {
                double tmp;
                Vector luMax = lu.viewRow(max);
                Vector luCol = lu.viewRow(col);

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
            final double luDiag = lu.getX(col, col);

            for (int row = col + 1; row < cols; row++) {
                double val = lu.getX(row, col) / luDiag;
                lu.setX(row, col, val);
            }
        }
    }

    /**
     * Destroys decomposition components and other internal components of decomposition.
     */
    @Override public void close() {
        if (cachedL != null)
            cachedL.destroy();
        if (cachedU != null)
            cachedU.destroy();
        if (cachedP != null)
            cachedP.destroy();
        lu.destroy();
    }

    /**
     * Returns the matrix L of the decomposition.
     * <p>L is a lower-triangular matrix</p>
     *
     * @return the L matrix (or null if decomposed matrix is singular).
     */
    public Matrix getL() {
        if ((cachedL == null) && !singular) {
            final int m = pivot.size();

            cachedL = like(matrix);
            cachedL.assign(0.0);

            for (int i = 0; i < m; ++i) {
                for (int j = 0; j < i; ++j)
                    cachedL.setX(i, j, lu.getX(i, j));

                cachedL.setX(i, i, 1.0);
            }
        }

        return cachedL;
    }

    /**
     * Returns the matrix U of the decomposition.
     * <p>U is an upper-triangular matrix</p>
     *
     * @return the U matrix (or null if decomposed matrix is singular).
     */
    public Matrix getU() {
        if ((cachedU == null) && !singular) {
            final int m = pivot.size();

            cachedU = like(matrix);
            cachedU.assign(0.0);

            for (int i = 0; i < m; ++i)
                for (int j = i; j < m; ++j)
                    cachedU.setX(i, j, lu.getX(i, j));
        }

        return cachedU;
    }

    /**
     * Returns the P rows permutation matrix.
     * <p>P is a sparse matrix with exactly one element set to 1.0 in
     * each row and each column, all other elements being set to 0.0.</p>
     * <p>The positions of the 1 elements are given by the {@link #getPivot()
     * pivot permutation vector}.</p>
     *
     * @return the P rows permutation matrix (or null if decomposed matrix is singular).
     * @see #getPivot()
     */
    public Matrix getP() {
        if ((cachedP == null) && !singular) {
            final int m = pivot.size();

            cachedP = like(matrix);
            cachedP.assign(0.0);

            for (int i = 0; i < m; ++i)
                cachedP.setX(i, (int)pivot.get(i), 1.0);
        }

        return cachedP;
    }

    /**
     * Returns the pivot permutation vector.
     *
     * @return the pivot permutation vector.
     * @see #getP()
     */
    public Vector getPivot() {
        return pivot.copy();
    }

    /**
     * Return the determinant of the matrix.
     *
     * @return determinant of the matrix.
     */
    public double determinant() {
        if (singular)
            return 0;

        final int m = pivot.size();
        double determinant = even ? 1 : -1;

        for (int i = 0; i < m; i++)
            determinant *= lu.getX(i, i);

        return determinant;
    }

    /**
     * @param b Vector to solve using this decomposition.
     * @return Solution vector.
     */
    public Vector solve(Vector b) {
        final int m = pivot.size();

        if (b.size() != m)
            throw new CardinalityException(b.size(), m);

        if (singular)
            throw new SingularMatrixException();

        final double[] bp = new double[m];

        // Apply permutations to b
        for (int row = 0; row < m; row++)
            bp[row] = b.get((int)pivot.get(row));

        // Solve LY = b
        for (int col = 0; col < m; col++) {
            final double bpCol = bp[col];

            for (int i = col + 1; i < m; i++)
                bp[i] -= bpCol * lu.get(i, col);
        }

        // Solve UX = Y
        for (int col = m - 1; col >= 0; col--) {
            bp[col] /= lu.get(col, col);
            final double bpCol = bp[col];

            for (int i = 0; i < col; i++)
                bp[i] -= bpCol * lu.get(i, col);
        }

        return b.like(m).assign(bp);
    }

    /**
     * @param b Matrix to solve using this decomposition.
     * @return Solution matrix.
     */
    public Matrix solve(Matrix b) {
        final int m = pivot.size();

        if (b.rowSize() != m)
            throw new CardinalityException(b.rowSize(), m);

        if (singular)
            throw new SingularMatrixException();

        final int nColB = b.columnSize();

        // Apply permutations to b
        final double[][] bp = new double[m][nColB];
        for (int row = 0; row < m; row++) {
            final double[] bpRow = bp[row];
            final int pRow = (int)pivot.get(row);

            for (int col = 0; col < nColB; col++)
                bpRow[col] = b.get(pRow, col);
        }

        // Solve LY = b
        for (int col = 0; col < m; col++) {
            final double[] bpCol = bp[col];
            for (int i = col + 1; i < m; i++) {
                final double[] bpI = bp[i];
                final double luICol = lu.get(i, col);

                for (int j = 0; j < nColB; j++)
                    bpI[j] -= bpCol[j] * luICol;
            }
        }

        // Solve UX = Y
        for (int col = m - 1; col >= 0; col--) {
            final double[] bpCol = bp[col];
            final double luDiag = lu.getX(col, col);

            for (int j = 0; j < nColB; j++)
                bpCol[j] /= luDiag;

            for (int i = 0; i < col; i++) {
                final double[] bpI = bp[i];
                final double luICol = lu.get(i, col);

                for (int j = 0; j < nColB; j++)
                    bpI[j] -= bpCol[j] * luICol;
            }
        }

        return b.like(b.rowSize(), b.columnSize()).assign(bp);
    }
}
