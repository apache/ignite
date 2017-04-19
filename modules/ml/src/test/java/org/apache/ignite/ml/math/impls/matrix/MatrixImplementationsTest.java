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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.ignite.ml.math.ExternalizeTest;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.ColumnIndexException;
import org.apache.ignite.ml.math.exceptions.IndexException;
import org.apache.ignite.ml.math.exceptions.RowIndexException;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOffHeapVector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.impls.vector.RandomVector;
import org.apache.ignite.ml.math.impls.vector.SparseLocalVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link Matrix} implementations.
 */
public class MatrixImplementationsTest extends ExternalizeTest<Matrix> {
    /** */
    private static final double DEFAULT_DELTA = 0.000000001d;

    /** */
    private void consumeSampleMatrix(BiConsumer<Matrix, String> consumer) {
        new MatrixImplementationFixtures().consumeSampleMatrix(consumer);
    }

    /** */
    @Test
    public void externalizeTest() {
        consumeSampleMatrix((m, desc) -> externalizeTest(m));
    }

    /** */
    @Test
    public void testLike() {
        consumeSampleMatrix((m, desc) -> {
            Class<? extends Matrix> cls = likeMatrixType(m);

            if (cls != null) {
                Matrix like = m.like(m.rowSize(), m.columnSize());

                assertEquals("Wrong \"like\" matrix for " + desc + "; Unexpected rows.", like.rowSize(), m.rowSize());
                assertEquals("Wrong \"like\" matrix for " + desc + "; Unexpected columns.", like.columnSize(), m.columnSize());

                assertEquals("Wrong \"like\" matrix for " + desc
                        + "; Unexpected class: " + like.getClass().toString(),
                    cls,
                    like.getClass());

                return;
            }

            boolean expECaught = false;

            try {
                m.like(1, 1);
            }
            catch (UnsupportedOperationException uoe) {
                expECaught = true;
            }

            assertTrue("Expected exception was not caught for " + desc, expECaught);
        });
    }

    /** */
    @Test
    public void testCopy() {
        consumeSampleMatrix((m, desc) -> {
            Matrix cp = m.copy();
            assertTrue("Incorrect copy for empty matrix " + desc, cp.equals(m));

            if (!readOnly(m))
                fillMatrix(m);

            cp = m.copy();

            assertTrue("Incorrect copy for matrix " + desc, cp.equals(m));
        });
    }

    /** */
    @Test
    public void testHaveLikeVector() throws InstantiationException, IllegalAccessException {
        for (Class<? extends Matrix> key : likeVectorTypesMap().keySet()) {
            Class<? extends Vector> val = likeVectorTypesMap().get(key);

            if (val == null && !ignore(key))
                System.out.println("Missing test for implementation of likeMatrix for " + key.getSimpleName());
        }
    }

    /** */
    @Test
    public void testLikeVector() {
        consumeSampleMatrix((m, desc) -> {
            if (likeVectorTypesMap().containsKey(m.getClass())) {
                Vector likeVector = m.likeVector(m.columnSize());

                assertNotNull(likeVector);
                assertEquals("Unexpected value for " + desc, likeVector.size(), m.columnSize());

                return;
            }

            boolean expECaught = false;

            try {
                m.likeVector(1);
            }
            catch (UnsupportedOperationException uoe) {
                expECaught = true;
            }

            assertTrue("Expected exception was not caught for " + desc, expECaught);
        });
    }

    /** */
    @Test
    public void testAssignSingleElement() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            final double assignVal = Math.random();

            m.assign(assignVal);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        assignVal, m.get(i, j), 0d);
        });
    }

    /** */
    @Test
    public void testAssignArray() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] data = new double[m.rowSize()][m.columnSize()];

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = Math.random();

            m.assign(data);

            for (int i = 0; i < m.rowSize(); i++) {
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        data[i][j], m.get(i, j), 0d);
            }
        });
    }

    /** */
    @Test
    public void testAssignFunction() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            m.assign((i, j) -> (double)(i * m.columnSize() + j));

            for (int i = 0; i < m.rowSize(); i++) {
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        (double)(i * m.columnSize() + j), m.get(i, j), 0d);
            }
        });
    }

    /** */
    @Test
    public void testPlus() {
        consumeSampleMatrix((m, desc) -> {
            if (readOnly(m))
                return;

            double[][] data = fillAndReturn(m);

            double plusVal = Math.random();

            Matrix plus = m.plus(plusVal);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        data[i][j] + plusVal, plus.get(i, j), 0d);
        });
    }

    /** */
    @Test
    public void testPlusMatrix() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] data = fillAndReturn(m);

            Matrix plus = m.plus(m);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        data[i][j] * 2.0, plus.get(i, j), 0d);
        });
    }

    /** */
    @Test
    public void testMinusMatrix() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Matrix minus = m.minus(m);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        0.0, minus.get(i, j), 0d);
        });
    }

    /** */
    @Test
    public void testTimes() {
        consumeSampleMatrix((m, desc) -> {
            if (readOnly(m))
                return;

            double[][] data = fillAndReturn(m);

            double timeVal = Math.random();
            Matrix times = m.times(timeVal);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        data[i][j] * timeVal, times.get(i, j), 0d);
        });
    }

    /** */
    @Test
    public void testTimesVector() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] data = fillAndReturn(m);

            double[] arr = fillArray(m.columnSize());

            Vector times = m.times(new DenseLocalOnHeapVector(arr));

            assertEquals("Unexpected vector size for " + desc, times.size(), m.rowSize());

            for (int i = 0; i < m.rowSize(); i++) {
                double exp = 0.0;

                for (int j = 0; j < m.columnSize(); j++)
                    exp += arr[j] * data[i][j];

                assertEquals("Unexpected value for " + desc + " at " + i,
                    times.get(i), exp, 0d);
            }

            testInvalidCardinality(() -> m.times(new DenseLocalOnHeapVector(m.columnSize() + 1)), desc);
        });
    }

    /** */
    @Test
    public void testTimesMatrix() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] data = fillAndReturn(m);

            double[] arr = fillArray(m.columnSize());

            Matrix mult = new DenseLocalOnHeapMatrix(m.columnSize(), 1);

            mult.setColumn(0, arr);

            Matrix times = m.times(mult);

            assertEquals("Unexpected rows for " + desc, times.rowSize(), m.rowSize());

            assertEquals("Unexpected cols for " + desc, times.columnSize(), 1);

            for (int i = 0; i < m.rowSize(); i++) {
                double exp = 0.0;

                for (int j = 0; j < m.columnSize(); j++)
                    exp += arr[j] * data[i][j];

                assertEquals("Unexpected value for " + desc + " at " + i,
                    exp, times.get(i, 0), 0d);
            }

            testInvalidCardinality(() -> m.times(new DenseLocalOnHeapMatrix(m.columnSize() + 1, 1)), desc);
        });
    }

    /** */
    @Test
    public void testDivide() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] data = fillAndReturn(m);

            double divVal = Math.random();

            Matrix divide = m.divide(divVal);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        data[i][j] / divVal, divide.get(i, j), 0d);
        });
    }

    /** */
    @Test
    public void testTranspose() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Matrix transpose = m.transpose();

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        m.get(i, j), transpose.get(j, i), 0d);
        });
    }

    /** */
    @Test
    public void testDeterminant() {
        consumeSampleMatrix((m, desc) -> {
            if (m.rowSize() != m.columnSize())
                return;

            if (ignore(m.getClass()))
                return;

            double[][] doubles = fillIntAndReturn(m);

            if (m.rowSize() == 1) {
                assertEquals("Unexpected value " + desc, m.determinant(), doubles[0][0], 0d);

                return;
            }

            if (m.rowSize() == 2) {
                double det = doubles[0][0] * doubles[1][1] - doubles[0][1] * doubles[1][0];
                assertEquals("Unexpected value " + desc, m.determinant(), det, 0d);

                return;
            }

            if (m.rowSize() > 512)
                return; // IMPL NOTE if row size >= 30000 it takes unacceptably long for normal test run.

            Matrix diagMtx = m.like(m.rowSize(), m.columnSize());

            diagMtx.assign(0);
            for (int i = 0; i < m.rowSize(); i++)
                diagMtx.set(i, i, m.get(i, i));

            double det = 1;

            for (int i = 0; i < diagMtx.rowSize(); i++)
                det *= diagMtx.get(i, i);

            try {
                assertEquals("Unexpected value " + desc, det, diagMtx.determinant(), DEFAULT_DELTA);
            }
            catch (Exception e) {
                System.out.println(desc);
                throw e;
            }
        });
    }

    /** */
    @Test
    public void testInverse() {
        consumeSampleMatrix((m, desc) -> {
            if (m.rowSize() != m.columnSize())
                return;

            if (ignore(m.getClass()))
                return;

            if (m.rowSize() > 256)
                return; // IMPL NOTE this is for quicker test run.

            fillNonSingularMatrix(m);

            assertTrue("Unexpected zero determinant " + desc, Math.abs(m.determinant()) > 0d);

            Matrix inverse = m.inverse();

            Matrix mult = m.times(inverse);

            final double delta = 0.001d;

            assertEquals("Unexpected determinant " + desc, 1d, mult.determinant(), delta);

            assertEquals("Unexpected top left value " + desc, 1d, mult.get(0, 0), delta);

            if (m.rowSize() == 1)
                return;

            assertEquals("Unexpected center value " + desc,
                1d, mult.get(m.rowSize() / 2, m.rowSize() / 2), delta);

            assertEquals("Unexpected bottom right value " + desc,
                1d, mult.get(m.rowSize() - 1, m.rowSize() - 1), delta);

            assertEquals("Unexpected top right value " + desc,
                0d, mult.get(0, m.rowSize() - 1), delta);

            assertEquals("Unexpected bottom left value " + desc,
                0d, mult.get(m.rowSize() - 1, 0), delta);
        });
    }

    /** */
    @Test
    public void testMap() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            m.map(x -> 10d);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        10d, m.get(i, j), 0d);
        });
    }

    /** */
    @Test
    public void testMapMatrix() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] doubles = fillAndReturn(m);

            testMapMatrixWrongCardinality(m, desc);

            Matrix cp = m.copy();

            m.map(cp, (m1, m2) -> m1 + m2);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        m.get(i, j), doubles[i][j] * 2, 0d);
        });
    }

    /** */
    @Test
    public void testViewRow() {
        consumeSampleMatrix((m, desc) -> {
            if (!readOnly(m))
                fillMatrix(m);

            for (int i = 0; i < m.rowSize(); i++) {
                Vector vector = m.viewRow(i);
                assert vector != null;

                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        m.get(i, j), vector.get(j), 0d);
            }
        });
    }

    /** */
    @Test
    public void testViewCol() {
        consumeSampleMatrix((m, desc) -> {
            if (!readOnly(m))
                fillMatrix(m);

            for (int i = 0; i < m.columnSize(); i++) {
                Vector vector = m.viewColumn(i);
                assert vector != null;

                for (int j = 0; j < m.rowSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        m.get(j, i), vector.get(j), 0d);
            }
        });
    }

    /** */
    @Test
    public void testFoldRow() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Vector foldRows = m.foldRows(Vector::sum);

            for (int i = 0; i < m.rowSize(); i++) {
                Double locSum = 0d;

                for (int j = 0; j < m.columnSize(); j++)
                    locSum += m.get(i, j);

                assertEquals("Unexpected value for " + desc + " at " + i,
                    foldRows.get(i), locSum, 0d);
            }
        });
    }

    /** */
    @Test
    public void testFoldCol() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Vector foldCols = m.foldColumns(Vector::sum);

            for (int j = 0; j < m.columnSize(); j++) {
                Double locSum = 0d;

                for (int i = 0; i < m.rowSize(); i++)
                    locSum += m.get(i, j);

                assertEquals("Unexpected value for " + desc + " at " + j,
                    foldCols.get(j), locSum, 0d);
            }
        });
    }

    /** */
    @Test
    public void testSum() {
        consumeSampleMatrix((m, desc) -> {
            double[][] data = fillAndReturn(m);

            double sum = m.sum();

            double rawSum = 0;
            for (double[] anArr : data)
                for (int j = 0; j < data[0].length; j++)
                    rawSum += anArr[j];

            assertEquals("Unexpected value for " + desc,
                rawSum, sum, 0d);
        });
    }

    /** */
    @Test
    public void testMax() {
        consumeSampleMatrix((m, desc) -> {
            double[][] doubles = fillAndReturn(m);
            double max = Double.NEGATIVE_INFINITY;

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    max = max < doubles[i][j] ? doubles[i][j] : max;

            assertEquals("Unexpected value for " + desc, m.maxValue(), max, 0d);
        });
    }

    /** */
    @Test
    public void testMin() {
        consumeSampleMatrix((m, desc) -> {
            double[][] doubles = fillAndReturn(m);
            double min = Double.MAX_VALUE;

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    min = min > doubles[i][j] ? doubles[i][j] : min;

            assertEquals("Unexpected value for " + desc, m.minValue(), min, 0d);
        });
    }

    /** */
    @Test
    public void testGetElement() {
        consumeSampleMatrix((m, desc) -> {
            if (!(readOnly(m)))
                fillMatrix(m);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++) {
                    final Matrix.Element e = m.getElement(i, j);

                    final String details = desc + " at [" + i + "," + j + "]";

                    assertEquals("Unexpected element row " + details, i, e.row());
                    assertEquals("Unexpected element col " + details, j, e.column());

                    final double val = m.get(i, j);

                    assertEquals("Unexpected value for " + details, val, e.get(), 0d);

                    boolean expECaught = false;

                    final double newVal = val * 2.0;

                    try {
                        e.set(newVal);
                    }
                    catch (UnsupportedOperationException uoe) {
                        if (!(readOnly(m)))
                            throw uoe;

                        expECaught = true;
                    }

                    if (readOnly(m)) {
                        if (!expECaught)
                            fail("Expected exception was not caught for " + details);

                        continue;
                    }

                    assertEquals("Unexpected value set for " + details, newVal, m.get(i, j), 0d);
                }
        });
    }

    /** */
    @Test
    public void testGetX() {
        consumeSampleMatrix((m, desc) -> {
            if (!(readOnly(m)))
                fillMatrix(m);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at [" + i + "," + j + "]",
                        m.get(i, j), m.getX(i, j), 0d);
        });
    }

    /** */
    @Test
    public void testGetMetaStorage() {
        consumeSampleMatrix((m, desc) -> assertNotNull("Null meta storage in " + desc, m.getMetaStorage()));
    }

    /** */
    @Test
    public void testGuid() {
        consumeSampleMatrix((m, desc) -> assertNotNull("Null guid in " + desc, m.guid()));
    }

    /** */
    @Test
    public void testSwapRows() {
        consumeSampleMatrix((m, desc) -> {
            if (readOnly(m))
                return;

            double[][] doubles = fillAndReturn(m);

            final int swap_i = m.rowSize() == 1 ? 0 : 1;
            final int swap_j = 0;

            Matrix swap = m.swapRows(swap_i, swap_j);

            for (int col = 0; col < m.columnSize(); col++) {
                assertEquals("Unexpected value for " + desc + " at col " + col + ", swap_i " + swap_i,
                    swap.get(swap_i, col), doubles[swap_j][col], 0d);

                assertEquals("Unexpected value for " + desc + " at col " + col + ", swap_j " + swap_j,
                    swap.get(swap_j, col), doubles[swap_i][col], 0d);
            }

            testInvalidRowIndex(() -> m.swapRows(-1, 0), desc + " negative first swap index");
            testInvalidRowIndex(() -> m.swapRows(0, -1), desc + " negative second swap index");
            testInvalidRowIndex(() -> m.swapRows(m.rowSize(), 0), desc + " too large first swap index");
            testInvalidRowIndex(() -> m.swapRows(0, m.rowSize()), desc + " too large second swap index");
        });
    }

    /** */
    @Test
    public void testSwapColumns() {
        consumeSampleMatrix((m, desc) -> {
            if (readOnly(m))
                return;

            double[][] doubles = fillAndReturn(m);

            final int swap_i = m.columnSize() == 1 ? 0 : 1;
            final int swap_j = 0;

            Matrix swap = m.swapColumns(swap_i, swap_j);

            for (int row = 0; row < m.rowSize(); row++) {
                assertEquals("Unexpected value for " + desc + " at row " + row + ", swap_i " + swap_i,
                    swap.get(row, swap_i), doubles[row][swap_j], 0d);

                assertEquals("Unexpected value for " + desc + " at row " + row + ", swap_j " + swap_j,
                    swap.get(row, swap_j), doubles[row][swap_i], 0d);
            }

            testInvalidColIndex(() -> m.swapColumns(-1, 0), desc + " negative first swap index");
            testInvalidColIndex(() -> m.swapColumns(0, -1), desc + " negative second swap index");
            testInvalidColIndex(() -> m.swapColumns(m.columnSize(), 0), desc + " too large first swap index");
            testInvalidColIndex(() -> m.swapColumns(0, m.columnSize()), desc + " too large second swap index");
        });
    }

    /** */
    @Test
    public void testSetRow() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            int rowIdx = m.rowSize() / 2;

            double[] newValues = fillArray(m.columnSize());

            m.setRow(rowIdx, newValues);

            for (int col = 0; col < m.columnSize(); col++)
                assertEquals("Unexpected value for " + desc + " at " + col,
                    newValues[col], m.get(rowIdx, col), 0d);

            testInvalidCardinality(() -> m.setRow(rowIdx, new double[m.columnSize() + 1]), desc);
        });
    }

    /** */
    @Test
    public void testSetColumn() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            int colIdx = m.columnSize() / 2;

            double[] newValues = fillArray(m.rowSize());

            m.setColumn(colIdx, newValues);

            for (int row = 0; row < m.rowSize(); row++)
                assertEquals("Unexpected value for " + desc + " at " + row,
                    newValues[row], m.get(row, colIdx), 0d);

            testInvalidCardinality(() -> m.setColumn(colIdx, new double[m.rowSize() + 1]), desc);
        });
    }

    /** */
    @Test
    public void testViewPart() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            int rowOff = m.rowSize() < 3 ? 0 : 1;
            int rows = m.rowSize() < 3 ? 1 : m.rowSize() - 2;
            int colOff = m.columnSize() < 3 ? 0 : 1;
            int cols = m.columnSize() < 3 ? 1 : m.columnSize() - 2;

            Matrix view1 = m.viewPart(rowOff, rows, colOff, cols);
            Matrix view2 = m.viewPart(new int[] {rowOff, colOff}, new int[] {rows, cols});

            String details = desc + " view [" + rowOff + ", " + rows + ", " + colOff + ", " + cols + "]";

            for (int i = 0; i < rows; i++)
                for (int j = 0; j < cols; j++) {
                    assertEquals("Unexpected view1 value for " + details + " at (" + i + "," + j + ")",
                        m.get(i + rowOff, j + colOff), view1.get(i, j), 0d);

                    assertEquals("Unexpected view2 value for " + details + " at (" + i + "," + j + ")",
                        m.get(i + rowOff, j + colOff), view2.get(i, j), 0d);
                }
        });
    }

    /** */
    @Test
    public void testDensity() {
        consumeSampleMatrix((m, desc) -> {
            if (!readOnly(m))
                fillMatrix(m);

            assertTrue("Unexpected density with threshold 0 for " + desc, m.density(0.0));

            assertFalse("Unexpected density with threshold 1 for " + desc, m.density(1.0));
        });
    }

    /** */
    @Test
    public void testMaxAbsRowSumNorm() {
        consumeSampleMatrix((m, desc) -> {
            if (!readOnly(m))
                fillMatrix(m);

            assertEquals("Unexpected value for " + desc,
                maxAbsRowSumNorm(m), m.maxAbsRowSumNorm(), 0d);
        });
    }

    /** */
    @Test
    public void testAssignRow() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            int rowIdx = m.rowSize() / 2;

            double[] newValues = fillArray(m.columnSize());

            m.assignRow(rowIdx, new DenseLocalOnHeapVector(newValues));

            for (int col = 0; col < m.columnSize(); col++)
                assertEquals("Unexpected value for " + desc + " at " + col,
                    newValues[col], m.get(rowIdx, col), 0d);

            testInvalidCardinality(() -> m.assignRow(rowIdx, new DenseLocalOnHeapVector(m.columnSize() + 1)), desc);
        });
    }

    /** */
    @Test
    public void testAssignColumn() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            int colIdx = m.columnSize() / 2;

            double[] newValues = fillArray(m.rowSize());

            m.assignColumn(colIdx, new DenseLocalOnHeapVector(newValues));

            for (int row = 0; row < m.rowSize(); row++)
                assertEquals("Unexpected value for " + desc + " at " + row,
                    newValues[row], m.get(row, colIdx), 0d);
        });
    }

    /** */
    private double[] fillArray(int len) {
        double[] newValues = new double[len];

        for (int i = 0; i < newValues.length; i++)
            newValues[i] = newValues.length - i;
        return newValues;
    }

    /** */
    private double maxAbsRowSumNorm(Matrix m) {
        double max = 0.0;

        for (int x = 0; x < m.rowSize(); x++) {
            double sum = 0;

            for (int y = 0; y < m.columnSize(); y++)
                sum += Math.abs(m.getX(x, y));

            if (sum > max)
                max = sum;
        }

        return max;
    }

    /** */
    private void testInvalidRowIndex(Supplier<Matrix> supplier, String desc) {
        try {
            supplier.get();
        }
        catch (RowIndexException | IndexException ie) {
            return;
        }

        fail("Expected exception was not caught for " + desc);
    }

    /** */
    private void testInvalidColIndex(Supplier<Matrix> supplier, String desc) {
        try {
            supplier.get();
        }
        catch (ColumnIndexException | IndexException ie) {
            return;
        }

        fail("Expected exception was not caught for " + desc);
    }

    /** */
    private void testMapMatrixWrongCardinality(Matrix m, String desc) {
        for (int rowDelta : new int[] {-1, 0, 1})
            for (int colDelta : new int[] {-1, 0, 1}) {
                if (rowDelta == 0 && colDelta == 0)
                    continue;

                int rowNew = m.rowSize() + rowDelta;
                int colNew = m.columnSize() + colDelta;

                if (rowNew < 1 || colNew < 1)
                    continue;

                testInvalidCardinality(() -> m.map(new DenseLocalOnHeapMatrix(rowNew, colNew), (m1, m2) -> m1 + m2),
                    desc + " wrong cardinality when mapping to size " + rowNew + "x" + colNew);
            }
    }

    /** */
    private void testInvalidCardinality(Supplier<Object> supplier, String desc) {
        try {
            supplier.get();
        }
        catch (CardinalityException ce) {
            return;
        }

        fail("Expected exception was not caught for " + desc);
    }

    /** */
    private boolean readOnly(Matrix m) {
        return m instanceof RandomMatrix;
    }

    /** */
    private double[][] fillIntAndReturn(Matrix m) {
        double[][] data = new double[m.rowSize()][m.columnSize()];

        if (readOnly(m)) {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = m.get(i, j);

        }
        else {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = i * m.rowSize() + j + 1;

            m.assign(data);
        }
        return data;
    }

    /** */
    private double[][] fillAndReturn(Matrix m) {
        double[][] data = new double[m.rowSize()][m.columnSize()];

        if (readOnly(m)) {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = m.get(i, j);

        }
        else {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = -0.5d + Math.random();

            m.assign(data);
        }
        return data;
    }

    /** */
    private void fillNonSingularMatrix(Matrix m) {
        for (int i = 0; i < m.rowSize(); i++) {
            m.set(i, i, 10);

            for (int j = 0; j < m.columnSize(); j++)
                if (j != i)
                    m.set(i, j, 0.01d);
        }
    }

    /** */
    private void fillMatrix(Matrix m) {
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, Math.random());
    }

    /** Ignore test for given matrix type. */
    private boolean ignore(Class<? extends Matrix> clazz) {
        List<Class<? extends Matrix>> ignoredClasses = Arrays.asList(RandomMatrix.class, PivotedMatrixView.class,
            MatrixView.class, FunctionMatrix.class, TransposedMatrixView.class);

        for (Class<? extends Matrix> ignoredClass : ignoredClasses)
            if (ignoredClass.isAssignableFrom(clazz))
                return true;

        return false;
    }

    /** */
    private Class<? extends Matrix> likeMatrixType(Matrix m) {
        for (Class<? extends Matrix> clazz : likeTypesMap().keySet())
            if (clazz.isAssignableFrom(m.getClass()))
                return likeTypesMap().get(clazz);

        return null;
    }

    /** */
    private static Map<Class<? extends Matrix>, Class<? extends Vector>> likeVectorTypesMap() {
        return new LinkedHashMap<Class<? extends Matrix>, Class<? extends Vector>>() {{
            put(DenseLocalOnHeapMatrix.class, DenseLocalOnHeapVector.class);
            put(DenseLocalOffHeapMatrix.class, DenseLocalOffHeapVector.class);
            put(RandomMatrix.class, RandomVector.class);
            put(SparseLocalOnHeapMatrix.class, SparseLocalVector.class);
            put(DenseLocalOnHeapMatrix.class, DenseLocalOnHeapVector.class);
            put(DiagonalMatrix.class, DenseLocalOnHeapVector.class); // IMPL NOTE per fixture
            // IMPL NOTE check for presence of all implementations here will be done in testHaveLikeMatrix via Fixture
        }};
    }

    /** */
    private static Map<Class<? extends Matrix>, Class<? extends Matrix>> likeTypesMap() {
        return new LinkedHashMap<Class<? extends Matrix>, Class<? extends Matrix>>() {{
            put(DenseLocalOnHeapMatrix.class, DenseLocalOnHeapMatrix.class);
            put(DenseLocalOffHeapMatrix.class, DenseLocalOffHeapMatrix.class);
            put(RandomMatrix.class, RandomMatrix.class);
            put(SparseLocalOnHeapMatrix.class, SparseLocalOnHeapMatrix.class);
            put(DenseLocalOnHeapMatrix.class, DenseLocalOnHeapMatrix.class);
            put(DiagonalMatrix.class, DenseLocalOnHeapMatrix.class); // IMPL NOTE per fixture
            put(FunctionMatrix.class, FunctionMatrix.class);
            // IMPL NOTE check for presence of all implementations here will be done in testHaveLikeMatrix via Fixture
        }};
    }
}
