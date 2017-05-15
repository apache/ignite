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

package org.apache.ignite.ml.math.util;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.CacheMatrix;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.MatrixView;
import org.apache.ignite.ml.math.impls.matrix.PivotedMatrixView;
import org.apache.ignite.ml.math.impls.matrix.RandomMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/**
 * Utility class for various matrix operations.
 */
public class MatrixUtil {
    /**
     * Create the like matrix with read-only matrices support.
     *
     * @param matrix Matrix for like.
     * @return Like matrix.
     */
    public static Matrix like(Matrix matrix) {
        if (isCopyLikeSupport(matrix))
            return new DenseLocalOnHeapMatrix(matrix.rowSize(), matrix.columnSize());
        else
            return matrix.like(matrix.rowSize(), matrix.columnSize());
    }

    /**
     * Create the identity matrix like a given matrix.
     *
     * @param matrix Matrix for like.
     * @return Identity matrix.
     */
    public static Matrix identityLike(Matrix matrix, int n) {
        Matrix res = like(matrix, n, n);
        // TODO: Maybe we should introduce API for walking(and changing) matrix in
        // a fastest possible visiting order.
        for (int i = 0; i < n; i++)
            res.setX(i, i, 1.0);
        return res;
    }

    /**
     * Create the like matrix with specified size with read-only matrices support.
     *
     * @param matrix Matrix for like.
     * @return Like matrix.
     */
    public static Matrix like(Matrix matrix, int rows, int cols) {
        if (isCopyLikeSupport(matrix))
            return new DenseLocalOnHeapMatrix(rows, cols);
        else
            return matrix.like(rows, cols);
    }

    /**
     * Create the like vector with read-only matrices support.
     *
     * @param matrix Matrix for like.
     * @param crd Cardinality of the vector.
     * @return Like vector.
     */
    public static Vector likeVector(Matrix matrix, int crd) {
        if (isCopyLikeSupport(matrix))
            return new DenseLocalOnHeapVector(crd);
        else
            return matrix.likeVector(crd);
    }

    /**
     * Create the like vector with read-only matrices support.
     *
     * @param matrix Matrix for like.
     * @return Like vector.
     */
    public static Vector likeVector(Matrix matrix) {
        return likeVector(matrix, matrix.rowSize());
    }

    /**
     * Create the copy of matrix with read-only matrices support.
     *
     * @param matrix Matrix for copy.
     * @return Copy.
     */
    public static Matrix copy(Matrix matrix) {
        if (isCopyLikeSupport(matrix)) {
            DenseLocalOnHeapMatrix cp = new DenseLocalOnHeapMatrix(matrix.rowSize(), matrix.columnSize());

            cp.assign(matrix);

            return cp;
        }
        else
            return matrix.copy();
    }

    /** */
    private static boolean isCopyLikeSupport(Matrix matrix) {
        return matrix instanceof RandomMatrix || matrix instanceof MatrixView || matrix instanceof CacheMatrix ||
            matrix instanceof PivotedMatrixView || matrix instanceof SparseDistributedMatrix;
    }
}
