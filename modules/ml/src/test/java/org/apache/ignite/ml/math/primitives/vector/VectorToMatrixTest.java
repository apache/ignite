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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.SparseMatrix;
import org.apache.ignite.ml.math.primitives.vector.impl.DelegatingVector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.VectorizedViewMatrix;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for methods of Vector that involve Matrix. */
public class VectorToMatrixTest {
    /** */
    private static final Map<Class<? extends Vector>, Class<? extends Matrix>> typesMap = typesMap();

    /** */
    @Test
    public void testHaveLikeMatrix() {
        for (Class<? extends Vector> key : typesMap.keySet()) {
            Class<? extends Matrix> val = typesMap.get(key);

            if (val == null)
                System.out.println("Missing test for implementation of likeMatrix for " + key.getSimpleName());
        }
    }

    /** */
    @Test
    public void testLikeMatrix() {
        consumeSampleVectors((v, desc) -> {
            if (!availableForTesting(v))
                return;

            final Matrix matrix = v.likeMatrix(1, 1);

            Class<? extends Vector> key = v.getClass();

            Class<? extends Matrix> expMatrixType = typesMap.get(key);

            assertNotNull("Expect non-null matrix for " + key.getSimpleName() + " in " + desc, matrix);

            Class<? extends Matrix> actualMatrixType = matrix.getClass();

            assertTrue("Expected matrix type " + expMatrixType.getSimpleName()
                    + " should be assignable from actual type " + actualMatrixType.getSimpleName() + " in " + desc,
                expMatrixType.isAssignableFrom(actualMatrixType));

            for (int rows : new int[] {1, 2})
                for (int cols : new int[] {1, 2}) {
                    final Matrix actualMatrix = v.likeMatrix(rows, cols);

                    String details = "rows " + rows + " cols " + cols;

                    assertNotNull("Expect non-null matrix for " + details + " in " + desc,
                        actualMatrix);

                    assertEquals("Unexpected number of rows in " + desc, rows, actualMatrix.rowSize());

                    assertEquals("Unexpected number of cols in " + desc, cols, actualMatrix.columnSize());
                }
        });
    }

    /** */
    @Test
    public void testToMatrix() {
        consumeSampleVectors((v, desc) -> {
            if (!availableForTesting(v))
                return;

            fillWithNonZeroes(v);

            final Matrix matrixRow = v.toMatrix(true);

            final Matrix matrixCol = v.toMatrix(false);

            for (Vector.Element e : v.all())
                assertToMatrixValue(desc, matrixRow, matrixCol, e.get(), e.index());
        });
    }

    /** */
    @Test
    public void testToMatrixPlusOne() {
        consumeSampleVectors((v, desc) -> {
            if (!availableForTesting(v))
                return;

            fillWithNonZeroes(v);

            for (double zeroVal : new double[] {-1, 0, 1, 2}) {
                final Matrix matrixRow = v.toMatrixPlusOne(true, zeroVal);

                final Matrix matrixCol = v.toMatrixPlusOne(false, zeroVal);

                final Metric metricRow0 = new Metric(zeroVal, matrixRow.get(0, 0));

                assertTrue("Not close enough row like " + metricRow0 + " at index 0 in " + desc,
                    metricRow0.closeEnough());

                final Metric metricCol0 = new Metric(zeroVal, matrixCol.get(0, 0));

                assertTrue("Not close enough cols like " + metricCol0 + " at index 0 in " + desc,
                    metricCol0.closeEnough());

                for (Vector.Element e : v.all())
                    assertToMatrixValue(desc, matrixRow, matrixCol, e.get(), e.index() + 1);
            }
        });
    }

    /** */
    @Test
    public void testCross() {
        consumeSampleVectors((v, desc) -> {
            if (!availableForTesting(v))
                return;

            fillWithNonZeroes(v);

            for (int delta : new int[] {-1, 0, 1}) {
                final int size2 = v.size() + delta;

                if (size2 < 1)
                    return;

                final Vector v2 = new DenseVector(size2);

                for (Vector.Element e : v2.all())
                    e.set(size2 - e.index());

                assertCross(v, v2, desc);
            }
        });
    }

    /** */
    private void assertCross(Vector v1, Vector v2, String desc) {
        assertNotNull(v1);
        assertNotNull(v2);

        final Matrix res = v1.cross(v2);

        assertNotNull("Cross matrix is expected to be not null in " + desc, res);

        assertEquals("Unexpected number of rows in cross Matrix in " + desc, v1.size(), res.rowSize());

        assertEquals("Unexpected number of cols in cross Matrix in " + desc, v2.size(), res.columnSize());

        for (int row = 0; row < v1.size(); row++)
            for (int col = 0; col < v2.size(); col++) {
                final Metric metric = new Metric(v1.get(row) * v2.get(col), res.get(row, col));

                assertTrue("Not close enough cross " + metric + " at row " + row + " at col " + col
                    + " in " + desc, metric.closeEnough());
            }
    }

    /** */
    private void assertToMatrixValue(String desc, Matrix matrixRow, Matrix matrixCol, double exp, int idx) {
        final Metric metricRow = new Metric(exp, matrixRow.get(0, idx));

        assertTrue("Not close enough row like " + metricRow + " at index " + idx + " in " + desc,
            metricRow.closeEnough());

        final Metric metricCol = new Metric(exp, matrixCol.get(idx, 0));

        assertTrue("Not close enough cols like " + matrixCol + " at index " + idx + " in " + desc,
            metricCol.closeEnough());
    }

    /** */
    private void fillWithNonZeroes(Vector sample) {
        for (Vector.Element e : sample.all())
            e.set(1 + e.index());
    }

    /** */
    private boolean availableForTesting(Vector v) {
        assertNotNull("Error in test: vector is null", v);

        final boolean availableForTesting = typesMap.get(v.getClass()) != null;

        final Matrix actualLikeMatrix = v.likeMatrix(1, 1);

        assertTrue("Need to enable matrix testing for vector type " + v.getClass().getSimpleName(),
            availableForTesting || actualLikeMatrix == null);

        return availableForTesting;
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Vector, String> consumer) {
        new VectorImplementationsFixtures().consumeSampleVectors(null, consumer);
    }

    /** */
    private static Map<Class<? extends Vector>, Class<? extends Matrix>> typesMap() {
        return new LinkedHashMap<Class<? extends Vector>, Class<? extends Matrix>>() {{
            put(DenseVector.class, DenseMatrix.class);
            put(SparseVector.class, SparseMatrix.class);
            put(VectorizedViewMatrix.class, DenseMatrix.class); // IMPL NOTE per fixture
            put(DelegatingVector.class, DenseMatrix.class); // IMPL NOTE per fixture
            // IMPL NOTE check for presence of all implementations here will be done in testHaveLikeMatrix via Fixture
        }};
    }

    /** */
    private static class Metric { //TODO: IGNITE-5824, consider if softer tolerance (like say 0.1 or 0.01) would make sense here.
        /** */
        private final double exp;

        /** */
        private final double obtained;

        /** **/
        Metric(double exp, double obtained) {
            this.exp = exp;
            this.obtained = obtained;
        }

        /** */
        boolean closeEnough() {
            return new Double(exp).equals(obtained);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Metric{" + "expected=" + exp +
                ", obtained=" + obtained +
                '}';
        }
    }
}
