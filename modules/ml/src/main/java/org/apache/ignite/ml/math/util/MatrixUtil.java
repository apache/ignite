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
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.SparseMatrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.ViewMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

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
            return new DenseMatrix(matrix.rowSize(), matrix.columnSize());
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
            return new DenseMatrix(rows, cols);
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
            return new DenseVector(crd);
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
            DenseMatrix cp = new DenseMatrix(matrix.rowSize(), matrix.columnSize());

            cp.assign(matrix);

            return cp;
        }
        else
            return matrix.copy();
    }

    /** */
    public static DenseMatrix asDense(SparseMatrix m, int acsMode) {
        DenseMatrix res = new DenseMatrix(m.rowSize(), m.columnSize(), acsMode);

        for (int row : m.indexesMap().keySet())
            for (int col : m.indexesMap().get(row))
                res.set(row, col, m.get(row, col));

        return res;
    }

    /** */
    private static boolean isCopyLikeSupport(Matrix matrix) {
        return matrix instanceof ViewMatrix;
    }

    /** */
    public static DenseMatrix fromList(List<Vector> vecs, boolean entriesAreRows) {
        GridArgumentCheck.notEmpty(vecs, "vecs");

        int dim = vecs.get(0).size();
        int vecsSize = vecs.size();

        DenseMatrix res = new DenseMatrix(entriesAreRows ? vecsSize : dim,
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
    public static DenseVector localCopyOf(Vector vec) {
        DenseVector res = new DenseVector(vec.size());

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

    /**
     * Zip two vectors with given tri-function taking as third argument position in vector
     * (i.e. apply binary function to both vector elementwise and construct vector from results).
     * Example zipWith({200, 400, 600}, {100, 300, 500}, plusAndMultiplyByIndex) = {(200 + 100) * 0, (400 + 300) * 1, (600 + 500) * 3}.
     * Length of result is length of shortest of vectors.
     *
     * @param v1 First vector.
     * @param v2 Second vector.
     * @param f Function to zip with.
     * @return Result of zipping.
     */
    public static Vector zipWith(Vector v1, Vector v2, IgniteTriFunction<Double, Double, Integer, Double> f) {
        int size = Math.min(v1.size(), v2.size());

        Vector res = v1.like(size);

        for (int row = 0; row < size; row++)
            res.setX(row, f.apply(v1.getX(row), v2.getX(row), row));

        return res;
    }

    /**
     * Zips two matrices by column-by-column with specified function. Result is matrix same flavour as first matrix.
     *
     * @param mtx1 First matrix.
     * @param mtx2 Second matrix.
     * @param fun Function to zip with.
     * @return Vector consisting from values resulted from zipping column-by-column.
     */
    public static Vector zipFoldByColumns(Matrix mtx1, Matrix mtx2, IgniteBiFunction<Vector, Vector, Double> fun) {
        int cols = Math.min(mtx1.columnSize(), mtx2.columnSize());

        Vector vec = mtx1.likeVector(cols);

        for (int i = 0; i < cols; i++)
            vec.setX(i, fun.apply(mtx1.getCol(i), mtx2.getCol(i)));

        return vec;
    }

    /**
     * Zips two matrices by row-by-row with specified function. Result is matrix same flavour as first matrix.
     *
     * @param mtx1 First matrix.
     * @param mtx2 Second matrix.
     * @param fun Function to zip with.
     * @return Vector consisting from values resulted from zipping row-by-row.
     */
    public static Vector zipFoldByRows(Matrix mtx1, Matrix mtx2, IgniteBiFunction<Vector, Vector, Double> fun) {
        int rows = Math.min(mtx1.rowSize(), mtx2.rowSize());

        Vector vec = mtx1.likeVector(rows);

        for (int i = 0; i < rows; i++)
            vec.setX(i, fun.apply(mtx1.viewRow(i), mtx2.viewRow(i)));

        return vec;
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

    /**
     * Performs in-place matrix subtraction.
     *
     * @param mtx1 Operand to be changed and subtracted from.
     * @param mtx2 Operand to subtract.
     * @return Updated first operand.
     */
    public static Matrix elementWiseMinus(Matrix mtx1, Matrix mtx2) {
        mtx1.map(mtx2, (a, b) -> a - b);

        return mtx1;
    }

    /**
     * Performs in-place matrix multiplication.
     *
     * @param mtx1 Operand to be changed and first multiplication operand.
     * @param mtx2 Second multiplication operand.
     * @return Updated first operand.
     */
    public static Matrix elementWiseTimes(Matrix mtx1, Matrix mtx2) {
        mtx1.map(mtx2, (a, b) -> a * b);

        return mtx1;
    }
}
