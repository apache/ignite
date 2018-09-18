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

package org.apache.ignite.ml.math.primitives.matrix;

import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.ViewMatrix;
import org.apache.ignite.ml.math.primitives.matrix.storage.ViewMatrixStorage;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** */
public class MatrixViewConstructorTest {
    /** */
    @Test
    public void invalidArgsTest() {
        Matrix m = new DenseMatrix(1, 1);

        DenseMatrixConstructorTest.verifyAssertionError(() -> new ViewMatrix((Matrix)null, 0, 0, 1, 1),
            "Null parent matrix.");

        DenseMatrixConstructorTest.verifyAssertionError(() -> new ViewMatrix(m, -1, 0, 1, 1),
            "Invalid row offset.");

        DenseMatrixConstructorTest.verifyAssertionError(() -> new ViewMatrix(m, 0, -1, 1, 1),
            "Invalid col offset.");

        DenseMatrixConstructorTest.verifyAssertionError(() -> new ViewMatrix(m, 0, 0, 0, 1),
            "Invalid rows.");

        DenseMatrixConstructorTest.verifyAssertionError(() -> new ViewMatrix(m, 0, 0, 1, 0),
            "Invalid cols.");
    }

    /** */
    @Test
    public void basicTest() {
        for (Matrix m : new Matrix[] {
            new DenseMatrix(3, 3),
            new DenseMatrix(3, 4), new DenseMatrix(4, 3)})
            for (int rowOff : new int[] {0, 1})
                for (int colOff : new int[] {0, 1})
                    for (int rows : new int[] {1, 2})
                        for (int cols : new int[] {1, 2})
                            basicTest(m, rowOff, colOff, rows, cols);
    }

    /** */
    private void basicTest(Matrix parent, int rowOff, int colOff, int rows, int cols) {
        for (int row = 0; row < parent.rowSize(); row++)
            for (int col = 0; col < parent.columnSize(); col++)
                parent.set(row, col, row * parent.columnSize() + col + 1);

        Matrix view = new ViewMatrix(parent, rowOff, colOff, rows, cols);

        assertEquals("Rows in view.", rows, view.rowSize());
        assertEquals("Cols in view.", cols, view.columnSize());

        for (int row = 0; row < rows; row++)
            for (int col = 0; col < cols; col++)
                assertEquals("Unexpected value at " + row + "x" + col,
                    parent.get(row + rowOff, col + colOff), view.get(row, col), 0d);

        for (int row = 0; row < rows; row++)
            for (int col = 0; col < cols; col++)
                view.set(row, col, 0d);

        for (int row = 0; row < rows; row++)
            for (int col = 0; col < cols; col++)
                assertEquals("Unexpected value set at " + row + "x" + col,
                    0d, parent.get(row + rowOff, col + colOff), 0d);
    }

    /** */
    @Test
    public void attributeTest() {
        for (Matrix m : new Matrix[] {
            new DenseMatrix(3, 3),
            new DenseMatrix(3, 4), new DenseMatrix(4, 3)}) {
            ViewMatrix matrixView = new ViewMatrix(m, 0, 0, m.rowSize(), m.columnSize());

            ViewMatrixStorage delegateStorage = (ViewMatrixStorage)matrixView.getStorage();

            assertEquals(m.rowSize(), matrixView.rowSize());
            assertEquals(m.columnSize(), matrixView.columnSize());

            assertEquals(m.rowSize(), (delegateStorage).rowsLength());
            assertEquals(m.columnSize(), (delegateStorage).columnsLength());

            assertEquals(m.isSequentialAccess(), delegateStorage.isSequentialAccess());
            assertEquals(m.isRandomAccess(), delegateStorage.isRandomAccess());
            assertEquals(m.isDistributed(), delegateStorage.isDistributed());
            assertEquals(m.isDense(), delegateStorage.isDense());
            assertEquals(m.isArrayBased(), delegateStorage.isArrayBased());

            assertArrayEquals(m.getStorage().data(), delegateStorage.data(), 0.0);
        }
    }
}
