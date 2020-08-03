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

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.exceptions.math.SingularMatrixException;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LUDecomposition}.
 */
public class LUDecompositionTest {
    /** */
    private Matrix testL;

    /** */
    private Matrix testU;

    /** */
    private Matrix testP;

    /** */
    private Matrix testMatrix;

    /** */
    private int[] rawPivot;

    /** */
    @Before
    public void setUp() {
        double[][] rawMatrix = new double[][] {
            {2.0d, 1.0d, 1.0d, 0.0d},
            {4.0d, 3.0d, 3.0d, 1.0d},
            {8.0d, 7.0d, 9.0d, 5.0d},
            {6.0d, 7.0d, 9.0d, 8.0d}};
        double[][] rawL = {
            {1.0d, 0.0d, 0.0d, 0.0d},
            {3.0d / 4.0d, 1.0d, 0.0d, 0.0d},
            {1.0d / 2.0d, -2.0d / 7.0d, 1.0d, 0.0d},
            {1.0d / 4.0d, -3.0d / 7.0d, 1.0d / 3.0d, 1.0d}};
        double[][] rawU = {
            {8.0d, 7.0d, 9.0d, 5.0d},
            {0.0d, 7.0d / 4.0d, 9.0d / 4.0d, 17.0d / 4.0d},
            {0.0d, 0.0d, -6.0d / 7.0d, -2.0d / 7.0d},
            {0.0d, 0.0d, 0.0d, 2.0d / 3.0d}};
        double[][] rawP = new double[][] {
            {0, 0, 1.0d, 0},
            {0, 0, 0, 1.0d},
            {0, 1.0d, 0, 0},
            {1.0d, 0, 0, 0}};

        rawPivot = new int[] {3, 4, 2, 1};

        testMatrix = new DenseMatrix(rawMatrix);
        testL = new DenseMatrix(rawL);
        testU = new DenseMatrix(rawU);
        testP = new DenseMatrix(rawP);
    }

    /** */
    @Test
    public void getL() throws Exception {
        Matrix luDecompositionL = new LUDecomposition(testMatrix).getL();

        assertEquals("Unexpected row size.", testL.rowSize(), luDecompositionL.rowSize());
        assertEquals("Unexpected column size.", testL.columnSize(), luDecompositionL.columnSize());

        for (int i = 0; i < testL.rowSize(); i++)
            for (int j = 0; j < testL.columnSize(); j++)
                assertEquals("Unexpected value at (" + i + "," + j + ").",
                    testL.getX(i, j), luDecompositionL.getX(i, j), 0.0000001d);

        luDecompositionL.destroy();
    }

    /** */
    @Test
    public void getU() throws Exception {
        Matrix luDecompositionU = new LUDecomposition(testMatrix).getU();

        assertEquals("Unexpected row size.", testU.rowSize(), luDecompositionU.rowSize());
        assertEquals("Unexpected column size.", testU.columnSize(), luDecompositionU.columnSize());

        for (int i = 0; i < testU.rowSize(); i++)
            for (int j = 0; j < testU.columnSize(); j++)
                assertEquals("Unexpected value at (" + i + "," + j + ").",
                    testU.getX(i, j), luDecompositionU.getX(i, j), 0.0000001d);

        luDecompositionU.destroy();
    }

    /** */
    @Test
    public void getP() throws Exception {
        Matrix luDecompositionP = new LUDecomposition(testMatrix).getP();

        assertEquals("Unexpected row size.", testP.rowSize(), luDecompositionP.rowSize());
        assertEquals("Unexpected column size.", testP.columnSize(), luDecompositionP.columnSize());

        for (int i = 0; i < testP.rowSize(); i++)
            for (int j = 0; j < testP.columnSize(); j++)
                assertEquals("Unexpected value at (" + i + "," + j + ").",
                    testP.getX(i, j), luDecompositionP.getX(i, j), 0.0000001d);

        luDecompositionP.destroy();
    }

    /** */
    @Test
    public void getPivot() throws Exception {
        Vector pivot = new LUDecomposition(testMatrix).getPivot();

        assertEquals("Unexpected pivot size.", rawPivot.length, pivot.size());

        for (int i = 0; i < testU.rowSize(); i++)
            assertEquals("Unexpected value at " + i, rawPivot[i], (int)pivot.get(i) + 1);
    }

    /**
     * Test for {@link MatrixUtil} features (more specifically, we test matrix which does not have a native like/copy
     * methods support).
     */
    @Test
    public void matrixUtilTest() {
        LUDecomposition dec = new LUDecomposition(testMatrix);
        Matrix luDecompositionL = dec.getL();

        assertEquals("Unexpected L row size.", testL.rowSize(), luDecompositionL.rowSize());
        assertEquals("Unexpected L column size.", testL.columnSize(), luDecompositionL.columnSize());

        for (int i = 0; i < testL.rowSize(); i++)
            for (int j = 0; j < testL.columnSize(); j++)
                assertEquals("Unexpected L value at (" + i + "," + j + ").",
                    testL.getX(i, j), luDecompositionL.getX(i, j), 0.0000001d);

        Matrix luDecompositionU = dec.getU();

        assertEquals("Unexpected U row size.", testU.rowSize(), luDecompositionU.rowSize());
        assertEquals("Unexpected U column size.", testU.columnSize(), luDecompositionU.columnSize());

        for (int i = 0; i < testU.rowSize(); i++)
            for (int j = 0; j < testU.columnSize(); j++)
                assertEquals("Unexpected U value at (" + i + "," + j + ").",
                    testU.getX(i, j), luDecompositionU.getX(i, j), 0.0000001d);

        Matrix luDecompositionP = dec.getP();

        assertEquals("Unexpected P row size.", testP.rowSize(), luDecompositionP.rowSize());
        assertEquals("Unexpected P column size.", testP.columnSize(), luDecompositionP.columnSize());

        for (int i = 0; i < testP.rowSize(); i++)
            for (int j = 0; j < testP.columnSize(); j++)
                assertEquals("Unexpected P value at (" + i + "," + j + ").",
                    testP.getX(i, j), luDecompositionP.getX(i, j), 0.0000001d);

        dec.close();
    }

    /** */
    @Test
    public void singularDeterminant() throws Exception {
        assertEquals("Unexpected determinant for singular matrix decomposition.",
            0d, new LUDecomposition(new DenseMatrix(2, 2)).determinant(), 0d);
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void solveVecWrongSize() throws Exception {
        new LUDecomposition(testMatrix).solve(new DenseVector(testMatrix.rowSize() + 1));
    }

    /** */
    @Test(expected = SingularMatrixException.class)
    public void solveVecSingularMatrix() throws Exception {
        new LUDecomposition(new DenseMatrix(testMatrix.rowSize(), testMatrix.rowSize()))
            .solve(new DenseVector(testMatrix.rowSize()));
    }

    /** */
    @Test
    public void solveVec() throws Exception {
        Vector sol = new LUDecomposition(testMatrix)
            .solve(new DenseVector(testMatrix.rowSize()));

        assertEquals("Wrong solution vector size.", testMatrix.rowSize(), sol.size());

        for (int i = 0; i < sol.size(); i++)
            assertEquals("Unexpected value at index " + i, 0d, sol.getX(i), 0.0000001d);
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void solveMtxWrongSize() throws Exception {
        new LUDecomposition(testMatrix).solve(
            new DenseMatrix(testMatrix.rowSize() + 1, testMatrix.rowSize()));
    }

    /** */
    @Test(expected = SingularMatrixException.class)
    public void solveMtxSingularMatrix() throws Exception {
        new LUDecomposition(new DenseMatrix(testMatrix.rowSize(), testMatrix.rowSize()))
            .solve(new DenseMatrix(testMatrix.rowSize(), testMatrix.rowSize()));
    }

    /** */
    @Test
    public void solveMtx() throws Exception {
        Matrix sol = new LUDecomposition(testMatrix)
            .solve(new DenseMatrix(testMatrix.rowSize(), testMatrix.rowSize()));

        assertEquals("Wrong solution matrix row size.", testMatrix.rowSize(), sol.rowSize());

        assertEquals("Wrong solution matrix column size.", testMatrix.rowSize(), sol.columnSize());

        for (int row = 0; row < sol.rowSize(); row++)
            for (int col = 0; col < sol.columnSize(); col++)
                assertEquals("Unexpected P value at (" + row + "," + col + ").",
                    0d, sol.getX(row, col), 0.0000001d);
    }

    /** */
    @Test(expected = AssertionError.class)
    public void nullMatrixTest() {
        new LUDecomposition(null);
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void nonSquareMatrixTest() {
        new LUDecomposition(new DenseMatrix(2, 3));
    }
}
