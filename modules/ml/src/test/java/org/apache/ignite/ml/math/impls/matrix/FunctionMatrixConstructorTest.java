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

package org.apache.ignite.ml.math.impls.matrix;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** */
public class FunctionMatrixConstructorTest {
    /** */
    @Test
    public void invalidArgsTest() {
        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new FunctionMatrix(0, 1, (i, j) -> 0.0),
            "Invalid row parameter.");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new FunctionMatrix(1, 0, (i, j) -> 0.0),
            "Invalid col parameter.");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new FunctionMatrix(1, 1, null),
            "Invalid func parameter.");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new FunctionMatrix(0, 1, (i, j) -> 0.0, null),
            "Invalid row parameter, with setter func.");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new FunctionMatrix(1, 0, (i, j) -> 0.0, null),
            "Invalid col parameter, with setter func.");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new FunctionMatrix(1, 1, null, null),
            "Invalid func parameter, with setter func.");
    }

    /** */
    @Test
    public void basicTest() {
        for (int rows : new int[] {1, 2, 3})
            for (int cols : new int[] {1, 2, 3})
                basicTest(rows, cols);

        Matrix m = new FunctionMatrix(1, 1, (i, j) -> 1d);
        //noinspection EqualsWithItself
        assertTrue("Matrix is expected to be equal to self.", m.equals(m));
        //noinspection ObjectEqualsNull
        assertFalse("Matrix is expected to be not equal to null.", m.equals(null));
    }

    /** */
    private void basicTest(int rows, int cols) {
        double[][] data = new double[rows][cols];

        for (int row = 0; row < rows; row++)
            for (int col = 0; col < cols; col++)
                data[row][col] = row * cols + row;

        Matrix mReadOnly = new FunctionMatrix(rows, cols, (i, j) -> data[i][j]);

        assertEquals("Rows in matrix.", rows, mReadOnly.rowSize());
        assertEquals("Cols in matrix.", cols, mReadOnly.columnSize());

        for (int row = 0; row < rows; row++)
            for (int col = 0; col < cols; col++) {
                assertEquals("Unexpected value at " + row + "x" + col, data[row][col], mReadOnly.get(row, col), 0d);

                boolean expECaught = false;

                try {
                    mReadOnly.set(row, col, 0.0);
                }
                catch (UnsupportedOperationException uoe) {
                    expECaught = true;
                }

                assertTrue("Expected exception wasn't thrown at " + row + "x" + col, expECaught);
            }

        Matrix m = new FunctionMatrix(rows, cols, (i, j) -> data[i][j], (i, j, val) -> data[i][j] = val);

        assertEquals("Rows in matrix, with setter function.", rows, m.rowSize());
        assertEquals("Cols in matrix, with setter function.", cols, m.columnSize());

        for (int row = 0; row < rows; row++)
            for (int col = 0; col < cols; col++) {
                assertEquals("Unexpected value at " + row + "x" + col, data[row][col], m.get(row, col), 0d);

                m.set(row, col, -data[row][col]);
            }

        for (int row = 0; row < rows; row++)
            for (int col = 0; col < cols; col++)
                assertEquals("Unexpected value set at " + row + "x" + col, -(row * cols + row), m.get(row, col), 0d);

        assertTrue("Incorrect copy for empty matrix.", m.copy().equals(m));
    }
}
