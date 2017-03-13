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

import org.apache.ignite.math.*;
import org.apache.ignite.math.functions.*;

/**
 * For an <tt>m x n</tt> matrix <tt>A</tt> with {@code m >= n}, the QR decomposition
 * is an <tt>m x n</tt> orthogonal matrix <tt>Q</tt> and an <tt>n x n</tt> upper
 * triangular matrix <tt>R</tt> so that <tt>A = Q*R</tt>.
 */
public class QRDecomposition {
    private final Matrix q;
    private final Matrix r;
    private final Matrix mType;
    private final boolean fullRank;

    private final int rows;
    private final int cols;

    /**
     *
     * @param v
     */
    private void checkDouble(double v) {
        if (Double.isInfinite(v) || Double.isNaN(v))
            throw new ArithmeticException("Invalid intermediate result");
    }

    /**
     * Constructs a new QR decomposition object computed by Householder reflections.
     *
     * @param mtx A rectangular matrix.
     */
    public QRDecomposition(Matrix mtx) {
        rows = mtx.rowSize();

        int min = Math.min(mtx.rowSize(), mtx.columnSize());

        cols = mtx.columnSize();

        mType = mtx.like(1, 1);

        Matrix qTmp = mtx.copy();

        boolean fullRank = true;

        r = mtx.like(min, cols);

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
    }

    /**
     * Gets orthogonal factor <tt>Q</tt>.
     */
    public Matrix getQ() {
        return q;
    }

    /**
     * Gets triangular factor <tt>R</tt>.
     */
    public Matrix getR() {
        return r;
    }

    /**
     * Returns whether the matrix <tt>A</tt> has full rank.
     *
     * @return true if <tt>R</tt>, and hence <tt>A</tt>, has full rank.
     */
    public boolean hasFullRank() {
        return fullRank;
    }

    /**
     * Least squares solution of <tt>A*X = B</tt>; <tt>returns X</tt>.
     *
     * @param mtx A matrix with as many rows as <tt>A</tt> and any number of cols.
     * @return <tt>X</tt> that minimizes the two norm of <tt>Q*R*X - B</tt>.
     * @throws IllegalArgumentException if <tt>B.rows() != A.rows()</tt>.
     */
    public Matrix solve(Matrix mtx) {
        if (mtx.rowSize() != rows)
            throw new IllegalArgumentException("Matrix row dimensions must agree.");

        int cols = mtx.columnSize();

        Matrix x = mType.like(this.cols, cols);

        Matrix qt = getQ().transpose();
        Matrix y = qt.times(mtx);

        Matrix r = getR();

        for (int k = Math.min(this.cols, rows) - 1; k >= 0; k--) {
            // X[k,] = Y[k,] / R[k,k], note that X[k,] starts with 0 so += is same as =
            x.viewRow(k).map(y.viewRow(k), Functions.plusMult(1 / r.get(k, k)));

            // Y[0:(k-1),] -= R[0:(k-1),k] * X[k,]
            Vector rColumn = r.viewColumn(k).viewPart(0, k);

            for (int c = 0; c < cols; c++)
                y.viewColumn(c).viewPart(0, k).map(rColumn, Functions.plusMult(-x.get(k, c)));
        }

        return x;
    }

    /**
     * Returns a rough string rendition of a QR.
     */
    @Override public String toString() {
        return String.format("QR(%d x %d, fullRank=%s)", rows, cols, hasFullRank());
    }
}
