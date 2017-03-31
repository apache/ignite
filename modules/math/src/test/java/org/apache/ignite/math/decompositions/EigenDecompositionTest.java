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

package org.apache.ignite.math.decompositions;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for {@link EigenDecomposition}
 */
public class EigenDecompositionTest {
    private static final double EPSILON = 1e-11;

    /** */
    @Test
    public void testMatrixWithRealEigenvalues() {
        test(new double[][] {
            {1.0d, 0.0d, 0.0d, 0.0d},
            {0.0d, 1.0d, 0.0d, 0.0d},
            {0.0d, 0.0d, 2.0d, 0.0d},
            {1.0d, 1.0d, 0.0d, 2.0d}});
    }

    /** */
    private void test(double[][] mRaw) {
        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(mRaw);
        EigenDecomposition decomposition = new EigenDecomposition(m);

        Matrix d = decomposition.getD();
        Matrix v = decomposition.getV();

        assertIsDiagonal(d);

        // check that d's diagonal consists of eigenvalues of m.
        assertDiagonalConsistsOfEigenvalues(m, d, v);

        // m = v d v^{-1} is equivalent to
        // m v = v d
        assertMatricesAreEqual(m.times(v), v.times(d));
    }

    /** */
    private void assertDiagonalConsistsOfEigenvalues(DenseLocalOnHeapMatrix m, Matrix d, Matrix v) {
        int n = m.columnSize();
        for (int i = 0; i < n; i++) {
            Vector eigenVector = v.viewColumn(i);
            double eigenValue = d.getX(i, i);
            assertVectorsAreEqual(m.times(eigenVector), eigenVector.times(eigenValue));
        }

    }

    /** */
    private void assertMatricesAreEqual(Matrix expected, Matrix actual) {
        assertTrue("The row sizes of matrices are not equal", expected.rowSize() == actual.rowSize());
        assertTrue("The col sizes of matrices are not equal", expected.columnSize() == actual.columnSize());

        // Since matrix is square, we need only one dimension
        int n = expected.columnSize();

        for (int i = 0; i < n; i++)
            for (int j = 0; j < n; j++)
                assertEquals("Values should be equal", expected.getX(i, j), actual.getX(i, j), EPSILON);
    }

    /** */
    private void assertVectorsAreEqual(Vector expected, Vector actual) {
        assertTrue("Vectors sizes are not equal", expected.size() == actual.size());

        // Since matrix is square, we need only one dimension
        int n = expected.size();

        for (int i = 0; i < n; i++)
            assertEquals("Values should be equal", expected.getX(i), actual.getX(i), EPSILON);
    }

    /** */
    private void assertIsDiagonal(Matrix m) {
        // Since matrix is square, we need only one dimension
        int n = m.columnSize();

        for (int i = 0; i < n; i++)
            for (int j = 0; j < n; j++)
                assertTrue("Matrix is not diagonal, violation at (" + i + "," + j + ")",
                    ((i == j) && m.getX(i, j) != 0) || ((i != j) && m.getX(i, j) == 0));
    }
}