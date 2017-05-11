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

package org.apache.ignite.ml.math.decompositions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.NonPositiveDefiniteMatrixException;
import org.apache.ignite.ml.math.exceptions.NonSymmetricMatrixException;

/**
 * Calculates the Cholesky decomposition of a matrix.
 *
 * This class inspired by class from Apache Common Math with similar name.
 *
 * @see <a href="http://mathworld.wolfram.com/CholeskyDecomposition.html">MathWorld</a>
 * @see <a href="http://en.wikipedia.org/wiki/Cholesky_decomposition">Wikipedia</a>
 */
public class CholeskyDecomposition extends DecompositionSupport {
    /**
     * Default threshold above which off-diagonal elements are considered too different
     * and matrix not symmetric.
     */
    public static final double DFLT_REL_SYMMETRY_THRESHOLD = 1.0e-15;

    /**
     * Default threshold below which diagonal elements are considered null
     * and matrix not positive definite.
     */
    public static final double DFLT_ABS_POSITIVITY_THRESHOLD = 1.0e-10;

    /** Row-oriented storage for L<sup>T</sup> matrix data. */
    private double[][] lTData;
    /** Cached value of L. */
    private Matrix cachedL;
    /** Cached value of LT. */
    private Matrix cachedLT;
    /** Origin matrix */
    private Matrix origin;

    /**
     * Calculates the Cholesky decomposition of the given matrix.
     *
     * Calling this constructor is equivalent to call {@link #CholeskyDecomposition(Matrix, double, double)} with the
     * thresholds set to the default values {@link #DFLT_REL_SYMMETRY_THRESHOLD} and
     * {@link #DFLT_ABS_POSITIVITY_THRESHOLD}.
     *
     * @param mtx the matrix to decompose.
     * @throws CardinalityException if matrix is not square.
     * @see #CholeskyDecomposition(Matrix, double, double)
     * @see #DFLT_REL_SYMMETRY_THRESHOLD
     * @see #DFLT_ABS_POSITIVITY_THRESHOLD
     */
    public CholeskyDecomposition(final Matrix mtx) {
        this(mtx, DFLT_REL_SYMMETRY_THRESHOLD, DFLT_ABS_POSITIVITY_THRESHOLD);
    }

    /**
     * Calculates the Cholesky decomposition of the given matrix.
     *
     * @param mtx the matrix to decompose.
     * @param relSymmetryThreshold threshold above which off-diagonal elements are considered too different and matrix
     * not symmetric
     * @param absPositivityThreshold threshold below which diagonal elements are considered null and matrix not positive
     * definite
     * @see #CholeskyDecomposition(Matrix)
     * @see #DFLT_REL_SYMMETRY_THRESHOLD
     * @see #DFLT_ABS_POSITIVITY_THRESHOLD
     */
    public CholeskyDecomposition(final Matrix mtx, final double relSymmetryThreshold,
        final double absPositivityThreshold) {
        assert mtx != null;

        if (mtx.columnSize() != mtx.rowSize())
            throw new CardinalityException(mtx.rowSize(), mtx.columnSize());

        origin = mtx;

        final int order = mtx.rowSize();

        lTData = toDoubleArr(mtx);
        cachedL = null;
        cachedLT = null;

        // Check the matrix before transformation.
        for (int i = 0; i < order; ++i) {
            final double[] lI = lTData[i];

            // Check off-diagonal elements (and reset them to 0).
            for (int j = i + 1; j < order; ++j) {
                final double[] lJ = lTData[j];

                final double lIJ = lI[j];
                final double lJI = lJ[i];

                final double maxDelta = relSymmetryThreshold * Math.max(Math.abs(lIJ), Math.abs(lJI));

                if (Math.abs(lIJ - lJI) > maxDelta)
                    throw new NonSymmetricMatrixException(i, j, relSymmetryThreshold);

                lJ[i] = 0;
            }
        }

        // Transform the matrix.
        for (int i = 0; i < order; ++i) {
            final double[] ltI = lTData[i];

            // Check diagonal element.
            if (ltI[i] <= absPositivityThreshold)
                throw new NonPositiveDefiniteMatrixException(ltI[i], i, absPositivityThreshold);

            ltI[i] = Math.sqrt(ltI[i]);
            final double inverse = 1.0 / ltI[i];

            for (int q = order - 1; q > i; --q) {
                ltI[q] *= inverse;
                final double[] ltQ = lTData[q];

                for (int p = q; p < order; ++p)
                    ltQ[p] -= ltI[q] * ltI[p];
            }
        }
    }

    /** */
    @Override public void destroy() {
        if (cachedL != null)
            cachedL.destroy();
        if (cachedLT != null)
            cachedLT.destroy();
    }

    /**
     * Returns the matrix L of the decomposition.
     * <p>L is an lower-triangular matrix</p>
     *
     * @return the L matrix
     */
    public Matrix getL() {
        if (cachedL == null)
            cachedL = getLT().transpose();

        return cachedL;
    }

    /**
     * Returns the transpose of the matrix L of the decomposition.
     * <p>L<sup>T</sup> is an upper-triangular matrix</p>
     *
     * @return the transpose of the matrix L of the decomposition
     */
    public Matrix getLT() {

        if (cachedLT == null) {
            Matrix like = like(origin, origin.rowSize(), origin.columnSize());
            like.assign(lTData);

            cachedLT = like;
        }

        // return the cached matrix
        return cachedLT;
    }

    /**
     * Return the determinant of the matrix
     *
     * @return determinant of the matrix
     */
    public double getDeterminant() {
        double determinant = 1.0;

        for (int i = 0; i < lTData.length; ++i) {
            double lTii = lTData[i][i];
            determinant *= lTii * lTii;
        }

        return determinant;
    }

    /**
     * Solve the linear equation A &times; X = B for matrices A.
     *
     * @param b right-hand side of the equation A &times; X = B
     * @return a vector X that minimizes the two norm of A &times; X - B
     * @throws CardinalityException if the vectors dimensions do not match
     */
    public Vector solve(final Vector b) {
        final int m = lTData.length;

        if (b.size() != m)
            throw new CardinalityException(b.size(), m);

        final double[] x = b.getStorage().data();

        // Solve LY = b
        for (int j = 0; j < m; j++) {
            final double[] lJ = lTData[j];

            x[j] /= lJ[j];

            final double xJ = x[j];

            for (int i = j + 1; i < m; i++)
                x[i] -= xJ * lJ[i];
        }

        // Solve LTX = Y
        for (int j = m - 1; j >= 0; j--) {
            x[j] /= lTData[j][j];

            final double xJ = x[j];

            for (int i = 0; i < j; i++)
                x[i] -= xJ * lTData[i][j];
        }

        return likeVector(origin, m).assign(x);
    }

    /**
     * Solve the linear equation A &times; X = B for matrices A.
     *
     * @param b right-hand side of the equation A &times; X = B
     * @return a matrix X that minimizes the two norm of A &times; X - B
     * @throws CardinalityException if the matrices dimensions do not match
     */
    public Matrix solve(final Matrix b) {
        final int m = lTData.length;

        if (b.rowSize() != m)
            throw new CardinalityException(b.rowSize(), m);

        final int nColB = b.columnSize();
        final double[][] x = b.getStorage().data();

        // Solve LY = b
        for (int j = 0; j < m; j++) {
            final double[] lJ = lTData[j];
            final double lJJ = lJ[j];
            final double[] xJ = x[j];

            for (int k = 0; k < nColB; ++k)
                xJ[k] /= lJJ;

            for (int i = j + 1; i < m; i++) {
                final double[] xI = x[i];
                final double lJI = lJ[i];

                for (int k = 0; k < nColB; ++k)
                    xI[k] -= xJ[k] * lJI;
            }
        }

        // Solve LTX = Y
        for (int j = m - 1; j >= 0; j--) {
            final double lJJ = lTData[j][j];
            final double[] xJ = x[j];

            for (int k = 0; k < nColB; ++k)
                xJ[k] /= lJJ;

            for (int i = 0; i < j; i++) {
                final double[] xI = x[i];
                final double lIJ = lTData[i][j];

                for (int k = 0; k < nColB; ++k)
                    xI[k] -= xJ[k] * lIJ;
            }
        }

        return like(origin, m, b.columnSize()).assign(x);
    }

    /** */
    private double[][] toDoubleArr(Matrix mtx) {
        if (mtx.isArrayBased())
            return mtx.getStorage().data();

        double[][] res = new double[mtx.rowSize()][];

        for (int row = 0; row < mtx.rowSize(); row++) {
            res[row] = new double[mtx.columnSize()];
            for (int col = 0; col < mtx.columnSize(); col++)
                res[row][col] = mtx.get(row, col);
        }

        return res;
    }
}
