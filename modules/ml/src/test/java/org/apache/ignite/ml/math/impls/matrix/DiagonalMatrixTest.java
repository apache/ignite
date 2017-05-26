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

import org.apache.ignite.ml.math.ExternalizeTest;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.MathTestConstants;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DiagonalMatrix}.
 */
public class DiagonalMatrixTest extends ExternalizeTest<DiagonalMatrix> {
    /** */
    public static final String UNEXPECTED_VALUE = "Unexpected value";

    /** */
    private DiagonalMatrix testMatrix;

    /** */
    @Before
    public void setup() {
        DenseLocalOnHeapMatrix parent = new DenseLocalOnHeapMatrix(MathTestConstants.STORAGE_SIZE, MathTestConstants.STORAGE_SIZE);
        fillMatrix(parent);
        testMatrix = new DiagonalMatrix(parent);
    }

    /** {@inheritDoc} */
    @Override public void externalizeTest() {
        externalizeTest(testMatrix);
    }

    /** */
    @Test
    public void testSetGetBasic() {
        double testVal = 42;
        for (int i = 0; i < MathTestConstants.STORAGE_SIZE; i++) {
            testMatrix.set(i, i, testVal);

            assertEquals(UNEXPECTED_VALUE + " at (" + i + "," + i + ")", testMatrix.get(i, i), testVal, 0d);
        }

        //noinspection EqualsWithItself
        assertTrue("Matrix is expected to be equal to self.", testMatrix.equals(testMatrix));
        //noinspection ObjectEqualsNull
        assertFalse("Matrix is expected to be not equal to null.", testMatrix.equals(null));
    }

    /** */
    @Test
    public void testSetGet() {
        verifyDiagonal(testMatrix);

        final int size = MathTestConstants.STORAGE_SIZE;

        for (Matrix m : new Matrix[] {
            new DenseLocalOnHeapMatrix(size + 1, size),
            new DenseLocalOnHeapMatrix(size, size + 1)}) {
            fillMatrix(m);

            verifyDiagonal(new DiagonalMatrix(m));
        }

        final double[] data = new double[size];

        for (int i = 0; i < size; i++)
            data[i] = 1 + i;

        final Matrix m = new DiagonalMatrix(new DenseLocalOnHeapVector(data));

        assertEquals("Rows in matrix constructed from vector", size, m.rowSize());
        assertEquals("Cols in matrix constructed from vector", size, m.columnSize());

        for (int i = 0; i < size; i++)
            assertEquals(UNEXPECTED_VALUE + " at vector index " + i, data[i], m.get(i, i), 0d);

        verifyDiagonal(m);

        final Matrix m1 = new DiagonalMatrix(data);

        assertEquals("Rows in matrix constructed from array", size, m1.rowSize());
        assertEquals("Cols in matrix constructed from array", size, m1.columnSize());

        for (int i = 0; i < size; i++)
            assertEquals(UNEXPECTED_VALUE + " at array index " + i, data[i], m1.get(i, i), 0d);

        verifyDiagonal(m1);
    }

    /** */
    @Test
    public void testConstant() {
        final int size = MathTestConstants.STORAGE_SIZE;

        for (double val : new double[] {-1.0, 0.0, 1.0}) {
            Matrix m = new DiagonalMatrix(size, val);

            assertEquals("Rows in matrix", size, m.rowSize());
            assertEquals("Cols in matrix", size, m.columnSize());

            for (int i = 0; i < size; i++)
                assertEquals(UNEXPECTED_VALUE + " at index " + i, val, m.get(i, i), 0d);

            verifyDiagonal(m, true);
        }
    }

    /** */
    @Test
    public void testAttributes() {
        assertTrue(UNEXPECTED_VALUE, testMatrix.rowSize() == MathTestConstants.STORAGE_SIZE);
        assertTrue(UNEXPECTED_VALUE, testMatrix.columnSize() == MathTestConstants.STORAGE_SIZE);

        assertFalse(UNEXPECTED_VALUE, testMatrix.isArrayBased());
        assertTrue(UNEXPECTED_VALUE, testMatrix.isDense());
        assertFalse(UNEXPECTED_VALUE, testMatrix.isDistributed());

        assertEquals(UNEXPECTED_VALUE, testMatrix.isRandomAccess(), !testMatrix.isSequentialAccess());
        assertTrue(UNEXPECTED_VALUE, testMatrix.isRandomAccess());
    }

    /** */
    @Test
    public void testNullParams() {
        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new DiagonalMatrix((Matrix)null), "Null Matrix parameter");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new DiagonalMatrix((Vector)null), "Null Vector parameter");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new DiagonalMatrix((double[])null), "Null double[] parameter");
    }

    /** */
    private void verifyDiagonal(Matrix m, boolean readonly) {
        final int rows = m.rowSize(), cols = m.columnSize();

        final String sizeDetails = "rows" + "X" + "cols " + rows + "X" + cols;

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++) {
                final String details = " at (" + i + "," + j + "), " + sizeDetails;

                final boolean diagonal = i == j;

                final double old = m.get(i, j);

                if (!diagonal)
                    assertEquals(UNEXPECTED_VALUE + details, 0, old, 0d);

                final double exp = diagonal && !readonly ? old + 1 : old;

                boolean expECaught = false;

                try {
                    m.set(i, j, exp);
                }
                catch (UnsupportedOperationException uoe) {
                    if (diagonal && !readonly)
                        throw uoe;

                    expECaught = true;
                }

                if ((!diagonal || readonly) && !expECaught)
                    fail("Expected exception was not caught " + details);

                assertEquals(UNEXPECTED_VALUE + details, exp, m.get(i, j), 0d);
            }
    }

    /** */
    private void verifyDiagonal(Matrix m) {
        verifyDiagonal(m, false);
    }

    /** */
    private void fillMatrix(Matrix m) {
        final int rows = m.rowSize(), cols = m.columnSize();

        boolean negative = false;

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                m.set(i, j, (negative = !negative) ? -(i * cols + j + 1) : i * cols + j + 1);
    }
}
