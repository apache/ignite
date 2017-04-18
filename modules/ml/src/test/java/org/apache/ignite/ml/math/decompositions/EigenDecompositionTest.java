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
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link EigenDecomposition}
 */
public class EigenDecompositionTest {
    /** */
    private static final double EPSILON = 1e-11;

    /** */
    @Test
    public void testMatrixWithRealEigenvalues() {
        test(new double[][] {
                {1.0d, 0.0d, 0.0d, 0.0d},
                {0.0d, 1.0d, 0.0d, 0.0d},
                {0.0d, 0.0d, 2.0d, 0.0d},
                {1.0d, 1.0d, 0.0d, 2.0d}},
            new double[] {1, 2, 2, 1});
    }

    /** */
    @Test
    public void testSymmetricMatrix() {
        EigenDecomposition decomposition = new EigenDecomposition(new DenseLocalOnHeapMatrix(new double[][] {
            {1.0d, 0.0d, 0.0d, 1.0d},
            {0.0d, 1.0d, 0.0d, 1.0d},
            {0.0d, 0.0d, 2.0d, 0.0d},
            {1.0d, 1.0d, 0.0d, 2.0d}}));

        Matrix d = decomposition.getD();
        Matrix v = decomposition.getV();

        assertNotNull("Matrix d is expected to be not null.", d);
        assertNotNull("Matrix v is expected to be not null.", v);

        assertEquals("Unexpected rows in d matrix.", 4, d.rowSize());
        assertEquals("Unexpected cols in d matrix.", 4, d.columnSize());

        assertEquals("Unexpected rows in v matrix.", 4, v.rowSize());
        assertEquals("Unexpected cols in v matrix.", 4, v.columnSize());

        assertIsDiagonalNonZero(d);

        decomposition.destroy();
    }

    /** */
    @Test
    public void testNonSquareMatrix() {
        EigenDecomposition decomposition = new EigenDecomposition(new DenseLocalOnHeapMatrix(new double[][] {
            {1.0d, 0.0d, 0.0d},
            {0.0d, 1.0d, 0.0d},
            {0.0d, 0.0d, 2.0d},
            {1.0d, 1.0d, 0.0d}}));
        // todo find out why decomposition of 3X4 matrix throws row index exception

        Matrix d = decomposition.getD();
        Matrix v = decomposition.getV();

        assertNotNull("Matrix d is expected to be not null.", d);
        assertNotNull("Matrix v is expected to be not null.", v);

        assertEquals("Unexpected rows in d matrix.", 4, d.rowSize());
        assertEquals("Unexpected cols in d matrix.", 4, d.columnSize());

        assertEquals("Unexpected rows in v matrix.", 4, v.rowSize());
        assertEquals("Unexpected cols in v matrix.", 3, v.columnSize());

        assertIsDiagonal(d, true);

        decomposition.destroy();
    }

    /** */
    private void test(double[][] mRaw, double[] expRealEigenValues) {
        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(mRaw);
        EigenDecomposition decomposition = new EigenDecomposition(m);

        Matrix d = decomposition.getD();
        Matrix v = decomposition.getV();

        assertIsDiagonalNonZero(d);

        // check that d's diagonal consists of eigenvalues of m.
        assertDiagonalConsistsOfEigenvalues(m, d, v);

        // m = v d v^{-1} is equivalent to
        // m v = v d
        assertMatricesAreEqual(m.times(v), v.times(d));

        assertEigenvalues(decomposition, expRealEigenValues);

        decomposition.destroy();
    }

    /** */
    private void assertEigenvalues(EigenDecomposition decomposition, double[] expRealEigenValues) {
        Vector real = decomposition.getRealEigenValues();
        Vector imag = decomposition.getImagEigenvalues();

        assertEquals("Real values size differs from expected.", expRealEigenValues.length, real.size());
        assertEquals("Imag values size differs from expected.", expRealEigenValues.length, imag.size());

        for (int idx = 0; idx < expRealEigenValues.length; idx++) {
            assertEquals("Real eigen value differs from expected at " + idx,
                expRealEigenValues[idx], real.get(idx), 0d);

            assertEquals("Imag eigen value differs from expected at " + idx,
                0d, imag.get(idx), 0d);
        }

    }

    /** */
    private void assertDiagonalConsistsOfEigenvalues(DenseLocalOnHeapMatrix m, Matrix d, Matrix v) {
        int n = m.columnSize();
        for (int i = 0; i < n; i++) {
            Vector eigenVector = v.viewColumn(i);
            double eigenVal = d.getX(i, i);
            assertVectorsAreEqual(m.times(eigenVector), eigenVector.times(eigenVal));
        }

    }

    /** */
    private void assertMatricesAreEqual(Matrix exp, Matrix actual) {
        assertTrue("The row sizes of matrices are not equal", exp.rowSize() == actual.rowSize());
        assertTrue("The col sizes of matrices are not equal", exp.columnSize() == actual.columnSize());

        // Since matrix is square, we need only one dimension
        int n = exp.columnSize();

        for (int i = 0; i < n; i++)
            for (int j = 0; j < n; j++)
                assertEquals("Values should be equal", exp.getX(i, j), actual.getX(i, j), EPSILON);
    }

    /** */
    private void assertVectorsAreEqual(Vector exp, Vector actual) {
        assertTrue("Vectors sizes are not equal", exp.size() == actual.size());

        // Since matrix is square, we need only one dimension
        int n = exp.size();

        for (int i = 0; i < n; i++)
            assertEquals("Values should be equal", exp.getX(i), actual.getX(i), EPSILON);
    }

    /** */
    private void assertIsDiagonalNonZero(Matrix m) {
        assertIsDiagonal(m, false);
    }

    /** */
    private void assertIsDiagonal(Matrix m, boolean zeroesAllowed) {
        // Since matrix is square, we need only one dimension
        int n = m.columnSize();

        assertEquals("Diagonal matrix is not square", n, m.rowSize());

        for (int i = 0; i < n; i++)
            for (int j = 0; j < n; j++)
                assertTrue("Matrix is not diagonal, violation at (" + i + "," + j + ")",
                    ((i == j) && (zeroesAllowed || m.getX(i, j) != 0))
                        || ((i != j) && m.getX(i, j) == 0));
    }
}
