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

import java.util.Arrays;
import org.apache.ignite.ml.math.Matrix;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** */
public class PivotedMatrixViewConstructorTest {
    /** */
    @Test
    public void invalidArgsTest() {
        Matrix m = new DenseLocalOnHeapMatrix(1, 1);

        int[] pivot = new int[] {0};

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new PivotedMatrixView(null),
            "Null parent matrix.");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new PivotedMatrixView(null, pivot),
            "Null parent matrix, with pivot.");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new PivotedMatrixView(m, null),
            "Null pivot.");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new PivotedMatrixView(m, null, pivot),
            "Null row pivot.");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new PivotedMatrixView(m, pivot, null),
            "Null col pivot.");
    }

    /** */
    @Test
    public void basicTest() {
        Matrix m = new DenseLocalOnHeapMatrix(2, 2);

        int[] pivot = new int[] {0, 1};

        PivotedMatrixView view = new PivotedMatrixView(m, pivot);

        assertEquals("Rows in view.", m.rowSize(), view.rowSize());
        assertEquals("Cols in view.", m.columnSize(), view.columnSize());

        assertTrue("Row pivot array in view.", Arrays.equals(pivot, view.rowPivot()));
        assertTrue("Col pivot array in view.", Arrays.equals(pivot, view.columnPivot()));

        Assert.assertEquals("Base matrix in view.", m, view.getBaseMatrix());

        assertEquals("Row pivot value in view.", 0, view.rowPivot(0));
        assertEquals("Col pivot value in view.", 0, view.columnPivot(0));

        assertEquals("Row unpivot value in view.", 0, view.rowUnpivot(0));
        assertEquals("Col unpivot value in view.", 0, view.columnUnpivot(0));

        Matrix swap = view.swap(1, 1);

        for (int row = 0; row < view.rowSize(); row++)
            for (int col = 0; col < view.columnSize(); col++)
                assertEquals("Unexpected swap value set at (" + row + "," + col + ").",
                    view.get(row, col), swap.get(row, col), 0d);

        //noinspection EqualsWithItself
        assertTrue("View is expected to be equal to self.", view.equals(view));
        //noinspection ObjectEqualsNull
        assertFalse("View is expected to be not equal to null.", view.equals(null));
    }

    /** */
    @Test
    public void pivotTest() {
        int[] pivot = new int[] {2, 1, 0, 3};

        for (Matrix m : new Matrix[] {
            new DenseLocalOnHeapMatrix(3, 3),
            new DenseLocalOnHeapMatrix(3, 4), new DenseLocalOnHeapMatrix(4, 3)})
            pivotTest(m, pivot);
    }

    /** */
    private void pivotTest(Matrix parent, int[] pivot) {
        for (int row = 0; row < parent.rowSize(); row++)
            for (int col = 0; col < parent.columnSize(); col++)
                parent.set(row, col, row * parent.columnSize() + col + 1);

        Matrix view = new PivotedMatrixView(parent, pivot);

        int rows = parent.rowSize();
        int cols = parent.columnSize();

        assertEquals("Rows in view.", rows, view.rowSize());
        assertEquals("Cols in view.", cols, view.columnSize());

        for (int row = 0; row < rows; row++)
            for (int col = 0; col < cols; col++)
                assertEquals("Unexpected value at " + row + "x" + col,
                    parent.get(pivot[row], pivot[col]), view.get(row, col), 0d);

        int min = rows < cols ? rows : cols;

        for (int idx = 0; idx < min; idx++)
            view.set(idx, idx, 0d);

        for (int idx = 0; idx < min; idx++)
            assertEquals("Unexpected value set at " + idx,
                0d, parent.get(pivot[idx], pivot[idx]), 0d);
    }
}
