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

        Matrix qTmp = copy(mtx);

        boolean fullRank = true;

        r = like(mtx, min, cols);

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

        verifyNonSingularR(threshold);

        this.fullRank = fullRank;
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
        return new QRDSolver(q, r).solve(mtx);
    }

    /**
     * Least squares solution of {@code A*X = B}; {@code returns X}.
     *
     * @param vec A vector with as many rows as {@code A}.
     * @return {@code X<} that minimizes the two norm of {@code Q*R*X - B}.
     * @throws IllegalArgumentException if {@code B.rows() != A.rows()}.
     */
    public Vector solve(Vector vec) {
        Matrix res = new QRDSolver(q, r).solve(vec.likeMatrix(vec.size(), 1).assignColumn(0, vec));

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
     * @param min Singularity threshold.
     * @throws SingularMatrixException if the matrix is singular and {@code raise} is {@code true}.
     */
    private void verifyNonSingularR(double min) {
        // TODO: IGNITE-5828, Not a very fast approach for distributed matrices. would be nice if we could independently
        // check parts on different nodes for singularity and do fold with 'or'.

        final int len = r.columnSize() > r.rowSize() ? r.rowSize() : r.columnSize();
        for (int i = 0; i < len; i++) {
            final double d = r.getX(i, i);
            if (Math.abs(d) <= min)
                throw new SingularMatrixException("Number is too small (%f, while " +
                    "threshold is %f). Index of diagonal element is (%d, %d)", d, min, i, i);

        }
    }
}
