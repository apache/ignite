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

import java.util.List;
import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.CacheMatrix;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.MatrixView;
import org.apache.ignite.ml.math.impls.matrix.PivotedMatrixView;
import org.apache.ignite.ml.math.impls.matrix.RandomMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseBlockDistributedMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseLocalOnHeapMatrix;
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
        // TODO: IGNITE-5216, Maybe we should introduce API for walking(and changing) matrix in.
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
     * Check if a given matrix is distributed.
     *
     * @param matrix Matrix for like.
     */
    private static boolean isDistributed(Matrix matrix) {
        return matrix instanceof SparseDistributedMatrix || matrix instanceof SparseBlockDistributedMatrix;
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
    public static DenseLocalOnHeapMatrix asDense(SparseLocalOnHeapMatrix m, int acsMode) {
        DenseLocalOnHeapMatrix res = new DenseLocalOnHeapMatrix(m.rowSize(), m.columnSize(), acsMode);

        for (int row : m.indexesMap().keySet())
            for (int col : m.indexesMap().get(row))
                res.set(row, col, m.get(row, col));

        return res;
    }

    /** */
    private static boolean isCopyLikeSupport(Matrix matrix) {
        return matrix instanceof RandomMatrix || matrix instanceof MatrixView || matrix instanceof CacheMatrix ||
            matrix instanceof PivotedMatrixView;
    }

    /** */
    public static DenseLocalOnHeapMatrix fromList(List<Vector> vecs, boolean entriesAreRows) {
        GridArgumentCheck.notEmpty(vecs, "vecs");

        int dim = vecs.get(0).size();
        int vecsSize = vecs.size();

        DenseLocalOnHeapMatrix res = new DenseLocalOnHeapMatrix(entriesAreRows ? vecsSize : dim,
            entriesAreRows ? dim : vecsSize);

        for (int i = 0; i < vecsSize; i++) {
            for (int j = 0; j < dim; j++) {
                int r = entriesAreRows ? i : j;
                int c = entriesAreRows ? j : i;

                res.setX(r, c, vecs.get(i).get(j));
            }
        }

        return res;
    }

    /** TODO: IGNITE-5723, rewrite in a more optimal way. */
    public static DenseLocalOnHeapVector localCopyOf(Vector vec) {
        DenseLocalOnHeapVector res = new DenseLocalOnHeapVector(vec.size());

        for (int i = 0; i < vec.size(); i++)
            res.setX(i, vec.getX(i));

        return res;
    }

    /** */
    public static double[][] unflatten(double[] fArr, int colsCnt, int stoMode) {
        int rowsCnt = fArr.length / colsCnt;

        boolean isRowMode = stoMode == StorageConstants.ROW_STORAGE_MODE;

        double[][] res = new double[rowsCnt][colsCnt];

        for (int i = 0; i < rowsCnt; i++)
            for (int j = 0; j < colsCnt; j++)
                res[i][j] = fArr[!isRowMode ? i * colsCnt + j : j * rowsCnt + i];

        return res;
    }

    /** */
    public static void unflatten(double[] fArr, Matrix mtx) {
        assert mtx != null;

        if (fArr.length <= 0)
            return;

        int rowsCnt = mtx.rowSize();
        int colsCnt = mtx.columnSize();

        boolean isRowMode = mtx.getStorage().storageMode() == StorageConstants.ROW_STORAGE_MODE;

        for (int i = 0; i < rowsCnt; i++)
            for (int j = 0; j < colsCnt; j++)
                mtx.setX(i, j, fArr[!isRowMode ? i * colsCnt + j : j * rowsCnt + i]);
    }

    /** */
    public static double[] flatten(double[][] arr, int stoMode) {
        assert arr != null;
        assert arr[0] != null;

        boolean isRowMode = stoMode == StorageConstants.ROW_STORAGE_MODE;

        int size = arr.length * arr[0].length;
        int rows = isRowMode ? arr.length : arr[0].length;
        int cols = size / rows;

        double[] res = new double[size];

        int iLim = isRowMode ? rows : cols;
        int jLim = isRowMode ? cols : rows;

        for (int i = 0; i < iLim; i++)
            for (int j = 0; j < jLim; j++)
                res[isRowMode ? j * iLim + i : i * jLim + j] = arr[i][j];

        return res;
    }
}
