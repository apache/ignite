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

import java.io.Serializable;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.NoDataException;
import org.apache.ignite.ml.math.exceptions.NullArgumentException;
import org.apache.ignite.ml.math.exceptions.SingularMatrixException;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.util.MatrixUtil;

import static org.apache.ignite.ml.math.util.MatrixUtil.like;

/**
 * For an {@code m x n} matrix {@code A} with {@code m >= n}, the QR decomposition
 * is an {@code m x n} orthogonal matrix {@code Q} and an {@code n x n} upper
 * triangular matrix {@code R} so that {@code A = Q*R}.
 */
public class QRDSolver implements Serializable {
    /** */
    private final Matrix q;

    /** */
    private final Matrix r;

    /**
     * Constructs a new QR decomposition solver object.
     *
     * @param q An orthogonal matrix.
     * @param r An upper triangular matrix
     */
    public QRDSolver(Matrix q, Matrix r) {
        this.q = q;
        this.r = r;
    }

    /**
     * Least squares solution of {@code A*X = B}; {@code returns X}.
     *
     * @param mtx A matrix with as many rows as {@code A} and any number of cols.
     * @return {@code X<} that minimizes the two norm of {@code Q*R*X - B}.
     * @throws IllegalArgumentException if {@code B.rows() != A.rows()}.
     */
    public Matrix solve(Matrix mtx) {
        if (mtx.rowSize() != q.rowSize())
            throw new IllegalArgumentException("Matrix row dimensions must agree.");

        int cols = mtx.columnSize();
        Matrix x = like(r, r.columnSize(), cols);

        Matrix qt = q.transpose();
        Matrix y = qt.times(mtx);

        for (int k = Math.min(r.columnSize(), q.rowSize()) - 1; k >= 0; k--) {
            // X[k,] = Y[k,] / R[k,k], note that X[k,] starts with 0 so += is same as =
            x.viewRow(k).map(y.viewRow(k), Functions.plusMult(1 / r.get(k, k)));

            if (k == 0)
                continue;

            // Y[0:(k-1),] -= R[0:(k-1),k] * X[k,]
            Vector rCol = r.viewColumn(k).viewPart(0, k);

            for (int c = 0; c < cols; c++)
                y.viewColumn(c).viewPart(0, k).map(rCol, Functions.plusMult(-x.get(k, c)));
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
        if (vec == null)
            throw new NullArgumentException();
        if (vec.size() == 0)
            throw new NoDataException();
        // TODO: IGNITE-5826, Should we copy here?

        Matrix res = solve(vec.likeMatrix(vec.size(), 1).assignColumn(0, vec));

        return vec.like(res.rowSize()).assign(res.viewColumn(0));
    }

    /**
     * <p>Compute the "hat" matrix.
     * </p>
     * <p>The hat matrix is defined in terms of the design matrix X
     * by X(X<sup>T</sup>X)<sup>-1</sup>X<sup>T</sup>
     * </p>
     * <p>The implementation here uses the QR decomposition to compute the
     * hat matrix as Q I<sub>p</sub>Q<sup>T</sup> where I<sub>p</sub> is the
     * p-dimensional identity matrix augmented by 0's.  This computational
     * formula is from "The Hat Matrix in Regression and ANOVA",
     * David C. Hoaglin and Roy E. Welsch,
     * <i>The American Statistician</i>, Vol. 32, No. 1 (Feb., 1978), pp. 17-22.
     * </p>
     * <p>Data for the model must have been successfully loaded using one of
     * the {@code newSampleData} methods before invoking this method; otherwise
     * a {@code NullPointerException} will be thrown.</p>
     *
     * @return the hat matrix
     * @throws NullPointerException unless method {@code newSampleData} has been called beforehand.
     */
    public Matrix calculateHat() {
        // Create augmented identity matrix
        // No try-catch or advertised NotStrictlyPositiveException - NPE above if n < 3
        Matrix augI = MatrixUtil.like(q, q.columnSize(), q.columnSize());

        int n = augI.columnSize();
        int p = r.columnSize();

        for (int i = 0; i < n; i++)
            for (int j = 0; j < n; j++)
                if (i == j && i < p)
                    augI.setX(i, j, 1d);
                else
                    augI.setX(i, j, 0d);

        // Compute and return Hat matrix
        // No DME advertised - args valid if we get here
        return q.times(augI).times(q.transpose());
    }

    /**
     * <p>Calculates the variance-covariance matrix of the regression parameters.
     * </p>
     * <p>Var(b) = (X<sup>T</sup>X)<sup>-1</sup>
     * </p>
     * <p>Uses QR decomposition to reduce (X<sup>T</sup>X)<sup>-1</sup>
     * to (R<sup>T</sup>R)<sup>-1</sup>, with only the top p rows of
     * R included, where p = the length of the beta vector.</p>
     *
     * <p>Data for the model must have been successfully loaded using one of
     * the {@code newSampleData} methods before invoking this method; otherwise
     * a {@code NullPointerException} will be thrown.</p>
     *
     * @param p Size of the beta variance-covariance matrix
     * @return The beta variance-covariance matrix
     * @throws SingularMatrixException if the design matrix is singular
     * @throws NullPointerException if the data for the model have not been loaded
     */
    public Matrix calculateBetaVariance(int p) {
        Matrix rAug = MatrixUtil.copy(r.viewPart(0, p, 0, p));
        Matrix rInv = rAug.inverse();

        return rInv.times(rInv.transpose());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        QRDSolver solver = (QRDSolver)o;

        return q.equals(solver.q) && r.equals(solver.r);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = q.hashCode();
        res = 31 * res + r.hashCode();
        return res;
    }

    /**
     * Returns a rough string rendition of a QRD solver.
     */
    @Override public String toString() {
        return String.format("QRD Solver(%d x %d)", q.rowSize(), r.columnSize());
    }
}
