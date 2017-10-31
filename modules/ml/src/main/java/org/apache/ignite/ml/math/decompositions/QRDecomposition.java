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

import org.apache.ignite.ml.math.Destroyable;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.SingularMatrixException;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.util.MatrixUtil;

import static org.apache.ignite.ml.math.util.MatrixUtil.copy;
import static org.apache.ignite.ml.math.util.MatrixUtil.like;

/**
 * For an {@code m x n} matrix {@code A} with {@code m >= n}, the QR decomposition
 * is an {@code m x n} orthogonal matrix {@code Q} and an {@code n x n} upper
 * triangular matrix {@code R} so that {@code A = Q*R}.
 */
public class QRDecomposition implements Destroyable {
    /** */
    private final Matrix q;
    /** */
    private final Matrix r;

    /** */
    private final Matrix mType;
    /** */
    private final boolean fullRank;

    /** */
    private final int rows;
    /** */
    private final int cols;
    /** */
    private double threshold;

    /**
     * @param v Value to be checked for being an ordinary double.
     */
    private void checkDouble(double v) {
        if (Double.isInfinite(v) || Double.isNaN(v))
            throw new ArithmeticException("Invalid intermediate result");
    }

    /**
     * Constructs a new QR decomposition object computed by Householder reflections.
     * Threshold for singularity check used in this case is 0.
     *
     * @param mtx A rectangular matrix.
     */
    public QRDecomposition(Matrix mtx) {
        this(mtx, 0.0);
    }

    /**
     * Constructs a new QR decomposition object computed by Householder reflections.
     *
     * @param mtx A rectangular matrix.
     * @param threshold Value used for detecting singularity of {@code R} matrix in decomposition.
     */
    public QRDecomposition(Matrix mtx, double threshold) {
        assert mtx != null;

        rows = mtx.rowSize();

        int min = Math.min(mtx.rowSize(), mtx.columnSize());

        cols = mtx.columnSize();

        mType = like(mtx, 1, 1);
        MatrixUtil.toString("mtx", mtx, cols, rows);
        Matrix qTmp = copy(mtx);
        MatrixUtil.toString("qTmp", qTmp, cols, rows);

        boolean fullRank = true;

        r = like(mtx, min, cols);
        this.threshold = threshold;

        for (int i = 0; i < min; i++) {
            Vector qi = qTmp.viewColumn(i);

            double alpha = qi.kNorm(2);

            if (Math.abs(alpha) > Double.MIN_VALUE)
                qi.map(Functions.div(alpha));
            else {
                checkDouble(alpha);

                fullRank = false;
            }

            r.set(i, i, alpha);

            for (int j = i + 1; j < cols; j++) {
                Vector qj = qTmp.viewColumn(j);

                double norm = qj.kNorm(2);

                if (Math.abs(norm) > Double.MIN_VALUE) {
                    double beta = qi.dot(qj);

                    r.set(i, j, beta);

                    if (j < min)
                        qj.map(qi, Functions.plusMult(-beta));
                }
                else
                    checkDouble(norm);
            }
        }

        if (cols > min)
            q = qTmp.viewPart(0, rows, 0, min).copy();
        else
            q = qTmp;

        this.fullRank = fullRank;

//        MatrixUtil.toString("R ", r, cols, cols);
 //       MatrixUtil.toString("Q ", q, cols, rows);
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        q.destroy();
        r.destroy();
        mType.destroy();
    }

    /**
     * Gets orthogonal factor {@code Q}.
     */
    public Matrix getQ() {
        return q;
    }

    /**
     * Gets triangular factor {@code R}.
     */
    public Matrix getR() {
        return r;
    }

    /**
     * Returns whether the matrix {@code A} has full rank.
     *
     * @return true if {@code R}, and hence {@code A} , has full rank.
     */
    public boolean hasFullRank() {
        return fullRank;
    }

    /**
     * Least squares solution of {@code A*X = B}; {@code returns X}.
     *
     * @param mtx A matrix with as many rows as {@code A} and any number of cols.
     * @return {@code X<} that minimizes the two norm of {@code Q*R*X - B}.
     * @throws IllegalArgumentException if {@code B.rows() != A.rows()}.
     */
    public Matrix solve(Matrix mtx) {
        if (mtx.rowSize() != rows)
            throw new IllegalArgumentException("Matrix row dimensions must agree.");

        int cols = mtx.columnSize();
        Matrix r = getR();
        MatrixUtil.toString("Before singular in QR", r, r.columnSize(), r.rowSize());
        checkSingular(r, threshold, true);
        Matrix x = like(mType, this.cols, cols);
        MatrixUtil.toString("XX", x, x.columnSize(), x.rowSize());
        Matrix qt = getQ().transpose();
        MatrixUtil.toString("QT", qt, qt.columnSize(), qt.rowSize());
        MatrixUtil.toString("mtx", mtx, mtx.columnSize(), mtx.rowSize());
        Matrix y = qt.times(mtx);
        MatrixUtil.toString("y", y, y.columnSize(), y.rowSize());

        for (int k = Math.min(this.cols, rows) - 1; k >= 0; k--) {
            // X[k,] = Y[k,] / R[k,k], note that X[k,] starts with 0 so += is same as =
            MatrixUtil.toString("X+", x, cols, this.cols);
            Vector vector = x.viewRow(k);
            MatrixUtil.toString("x", vector, cols);
            x.viewRow(k).map(y.viewRow(k), Functions.plusMult(1 / r.get(k, k)));
            MatrixUtil.toString("X-", x, cols, this.cols);
            if (k == 0)
                continue;

            // Y[0:(k-1),] -= R[0:(k-1),k] * X[k,]
            Vector rCol = r.viewColumn(k).viewPart(0, k);
            MatrixUtil.toString("rCol on iteration k = " + k, rCol, k);

            for (int c = 0; c < cols; c++){
                Vector part = y.viewColumn(c).viewPart(0, k);
                part.map(rCol, Functions.plusMult(-x.get(k, c)));
                MatrixUtil.toString("part on iteration c after mult = " + c, part, k);
            }

        }

        return x;
    }

    /**
     * Least squares solution of {@code A*X = B}; {@code returns X}.
     *
     * @param vec A vector with as many rows as {@code A}.
     * @return {@code X<} that minimizes the two norm of {@code Q*R*X - B}.
     * @throws IllegalArgumentException if {@code B.rows() != A.rows()}.
     */
    public Vector solve(Vector vec) {
        MatrixUtil.toString("vec", vec.likeMatrix(vec.size(), 1).assignColumn(0, vec),  1, vec.size());
        Matrix res = solve(vec.likeMatrix(vec.size(), 1).assignColumn(0, vec));
        MatrixUtil.toString("Result of solving = ", res, res.columnSize(), res.rowSize());
        return vec.like(res.rowSize()).assign(res.viewColumn(0));
    }

    /**
     * Returns a rough string rendition of a QR.
     */
    @Override public String toString() {
        return String.format("QR(%d x %d, fullRank=%s)", rows, cols, hasFullRank());
    }

    /**
     * Check singularity.
     *
     * @param r R matrix.
     * @param min Singularity threshold.
     * @param raise Whether to raise a {@link SingularMatrixException} if any element of the diagonal fails the check.
     * @return {@code true} if any element of the diagonal is smaller or equal to {@code min}.
     * @throws SingularMatrixException if the matrix is singular and {@code raise} is {@code true}.
     */
    private static boolean checkSingular(Matrix r, double min, boolean raise) {
        // TODO: IGNITE-5828, Not a very fast approach for distributed matrices. would be nice if we could independently check
        // parts on different nodes for singularity and do fold with 'or'.

        final int len = r.columnSize();
        for (int i = 0; i < len; i++) {
            final double d = r.getX(i, i);
            if (Math.abs(d) <= min)
                if (raise)
                    throw new SingularMatrixException("Number is too small (%f, while " +
                        "threshold is %f). Index of diagonal element is (%d, %d)", d, min, i, i);
                else
                    return true;

        }
        return false;
    }
}
