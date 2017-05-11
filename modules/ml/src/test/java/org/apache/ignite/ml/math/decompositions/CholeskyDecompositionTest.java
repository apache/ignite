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
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.NonPositiveDefiniteMatrixException;
import org.apache.ignite.ml.math.exceptions.NonSymmetricMatrixException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.PivotedMatrixView;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** */
public class CholeskyDecompositionTest {
    /** */
    @Test
    public void basicTest() {
        basicTest(new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        }));
    }

    /**
     * Test for {@link DecompositionSupport} features.
     */
    @Test
    public void decompositionSupportTest() {
        basicTest(new PivotedMatrixView(new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        })));
    }

    /** */
    @Test(expected = AssertionError.class)
    public void nullMatrixTest() {
        new CholeskyDecomposition(null);
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void wrongMatrixSizeTest() {
        new CholeskyDecomposition(new DenseLocalOnHeapMatrix(2, 3));
    }

    /** */
    @Test(expected = NonSymmetricMatrixException.class)
    public void nonSymmetricMatrixTest() {
        new CholeskyDecomposition(new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 10.0d},
            {-1.0d, 2.0d, -1.0d},
            {-10.0d, -1.0d, 2.0d}
        }));
    }

    /** */
    @Test(expected = NonPositiveDefiniteMatrixException.class)
    public void nonAbsPositiveMatrixTest() {
        new CholeskyDecomposition(new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 0.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        }));
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void solveWrongVectorSizeTest() {
        new CholeskyDecomposition(new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        })).solve(new DenseLocalOnHeapVector(2));
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void solveWrongMatrixSizeTest() {
        new CholeskyDecomposition(new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        })).solve(new DenseLocalOnHeapMatrix(2, 3));
    }

    /** */
    private void basicTest(Matrix m) {
        // This decomposition is useful when dealing with systems of linear equations of the form
        // m x = b where m is a Hermitian matrix.
        // For such systems Cholesky decomposition provides
        // more effective method of solving compared to LU decomposition.
        // Suppose we want to solve system
        // m x = b for various bs. Then after we computed Cholesky decomposition, we can feed various bs
        // as a matrix of the form
        // (b1, b2, ..., bm)
        // to the method Cholesky::solve which returns solutions in the form
        // (sol1, sol2, ..., solm)
        CholeskyDecomposition dec = new CholeskyDecomposition(m);
        assertEquals("Unexpected value for decomposition determinant.",
            4d, dec.getDeterminant(), 0d);

        Matrix l = dec.getL();
        Matrix lt = dec.getLT();

        assertNotNull("Matrix l is expected to be not null.", l);
        assertNotNull("Matrix lt is expected to be not null.", lt);

        for (int row = 0; row < l.rowSize(); row++)
            for (int col = 0; col < l.columnSize(); col++)
                assertEquals("Unexpected value transposed matrix at (" + row + "," + col + ").",
                    l.get(row, col), lt.get(col, row), 0d);

        Matrix bs = new DenseLocalOnHeapMatrix(new double[][] {
            {4.0, -6.0, 7.0},
            {1.0, 1.0, 1.0}
        }).transpose();
        Matrix sol = dec.solve(bs);

        assertNotNull("Solution matrix is expected to be not null.", sol);
        assertEquals("Solution rows are not as expected.", bs.rowSize(), sol.rowSize());
        assertEquals("Solution columns are not as expected.", bs.columnSize(), sol.columnSize());

        for (int i = 0; i < sol.columnSize(); i++)
            assertNotNull("Solution matrix column is expected to be not null at index " + i, sol.viewColumn(i));

        Vector b = new DenseLocalOnHeapVector(new double[] {4.0, -6.0, 7.0});
        Vector solVec = dec.solve(b);

        for (int idx = 0; idx < b.size(); idx++)
            assertEquals("Unexpected value solution vector at " + idx,
                b.get(idx), solVec.get(idx), 0d);

        dec.destroy();
    }
}
