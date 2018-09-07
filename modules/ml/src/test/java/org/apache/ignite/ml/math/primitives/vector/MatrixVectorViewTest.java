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

package org.apache.ignite.ml.math.primitives.vector;

import org.apache.ignite.ml.math.exceptions.IndexException;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.impl.VectorizedViewMatrix;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link VectorizedViewMatrix}.
 */
public class MatrixVectorViewTest {
    /** */
    private static final String UNEXPECTED_VALUE = "Unexpected value";
    /** */
    private static final int SMALL_SIZE = 3;
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    private Matrix parent;

    /** */
    @Before
    public void setup() {
        parent = newMatrix(SMALL_SIZE, SMALL_SIZE);
    }

    /** */
    @Test
    public void testDiagonal() {
        Vector vector = parent.viewDiagonal();

        for (int i = 0; i < SMALL_SIZE; i++)
            assertView(i, i, vector, i);
    }

    /** */
    @Test
    public void testRow() {
        for (int i = 0; i < SMALL_SIZE; i++) {
            Vector viewRow = parent.viewRow(i);

            for (int j = 0; j < SMALL_SIZE; j++)
                assertView(i, j, viewRow, j);
        }
    }

    /** */
    @Test
    public void testCols() {
        for (int i = 0; i < SMALL_SIZE; i++) {
            Vector viewCol = parent.viewColumn(i);

            for (int j = 0; j < SMALL_SIZE; j++)
                assertView(j, i, viewCol, j);
        }
    }

    /** */
    @Test
    public void basicTest() {
        for (int rowSize : new int[] {1, 2, 3, 4})
            for (int colSize : new int[] {1, 2, 3, 4})
                for (int row = 0; row < rowSize; row++)
                    for (int col = 0; col < colSize; col++)
                        for (int rowStride = 0; rowStride < rowSize; rowStride++)
                            for (int colStride = 0; colStride < colSize; colStride++)
                                if (rowStride != 0 || colStride != 0)
                                    assertMatrixVectorView(newMatrix(rowSize, colSize), row, col, rowStride, colStride);
    }

    /** */
    @Test(expected = AssertionError.class)
    public void parentNullTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(null, 1, 1, 1, 1).size());
    }

    /** */
    @Test(expected = IndexException.class)
    public void rowNegativeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(parent, -1, 1, 1, 1).size());
    }

    /** */
    @Test(expected = IndexException.class)
    public void colNegativeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(parent, 1, -1, 1, 1).size());
    }

    /** */
    @Test(expected = IndexException.class)
    public void rowTooLargeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(parent, parent.rowSize() + 1, 1, 1, 1).size());
    }

    /** */
    @Test(expected = IndexException.class)
    public void colTooLargeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(parent, 1, parent.columnSize() + 1, 1, 1).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void rowStrideNegativeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(parent, 1, 1, -1, 1).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void colStrideNegativeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(parent, 1, 1, 1, -1).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void rowStrideTooLargeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(parent, 1, 1, parent.rowSize() + 1, 1).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void colStrideTooLargeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(parent, 1, 1, 1, parent.columnSize() + 1).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void bothStridesZeroTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new VectorizedViewMatrix(parent, 1, 1, 0, 0).size());
    }

    /** */
    private void assertMatrixVectorView(Matrix parent, int row, int col, int rowStride, int colStride) {
        VectorizedViewMatrix view = new VectorizedViewMatrix(parent, row, col, rowStride, colStride);

        String desc = "parent [" + parent.rowSize() + "x" + parent.columnSize() + "], view ["
            + row + "x" + col + "], strides [" + rowStride + ", " + colStride + "]";

        final int size = view.size();

        final int sizeByRows = rowStride == 0 ? IMPOSSIBLE_SIZE : (parent.rowSize() - row) / rowStride;
        final int sizeByCols = colStride == 0 ? IMPOSSIBLE_SIZE : (parent.columnSize() - col) / colStride;

        assertTrue("Size " + size + " differs from expected for " + desc,
            size == sizeByRows || size == sizeByCols);

        for (int idx = 0; idx < size; idx++) {
            final int rowIdx = row + idx * rowStride;
            final int colIdx = col + idx * colStride;

            assertEquals(UNEXPECTED_VALUE + " at view index " + idx + desc,
                parent.get(rowIdx, colIdx), view.get(idx), 0d);
        }
    }

    /** */
    private Matrix newMatrix(int rowSize, int colSize) {
        Matrix res = new DenseMatrix(rowSize, colSize);

        for (int i = 0; i < res.rowSize(); i++)
            for (int j = 0; j < res.columnSize(); j++)
                res.set(i, j, i * res.rowSize() + j);

        return res;
    }

    /** */
    private void assertView(int row, int col, Vector view, int viewIdx) {
        assertValue(row, col, view, viewIdx);

        parent.set(row, col, parent.get(row, col) + 1);

        assertValue(row, col, view, viewIdx);

        view.set(viewIdx, view.get(viewIdx) + 2);

        assertValue(row, col, view, viewIdx);
    }

    /** */
    private void assertValue(int row, int col, Vector view, int viewIdx) {
        assertEquals(UNEXPECTED_VALUE + " at row " + row + " col " + col, parent.get(row, col), view.get(viewIdx), 0d);
    }
}
