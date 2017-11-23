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
import org.apache.ignite.ml.math.functions.Functions;

import static org.apache.ignite.ml.math.util.MatrixUtil.like;

/**
 * For an {@code m x n} matrix {@code A} with {@code m >= n}, the QR decomposition
 * is an {@code m x n} orthogonal matrix {@code Q} and an {@code n x n} upper
 * triangular matrix {@code R} so that {@code A = Q*R}.
 */
class QRDSolver {
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
    QRDSolver(Matrix q, Matrix r) {
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
     * Returns a rough string rendition of a QRD solver.
     */
    @Override public String toString() {
        return String.format("QRD Solver(%d x %d)", q.rowSize(), r.columnSize());
    }
}
