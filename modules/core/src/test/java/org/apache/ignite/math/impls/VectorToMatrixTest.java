package org.apache.ignite.math.impls;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.Assert.*;

/** Tests for methods of Vector that involve Matrix. */
public class VectorToMatrixTest {
    /** */ private static final Map<Class<? extends Vector>, Class<? extends Matrix>> typesMap = typesMap();

    /** */ @Test
    public void testHaveLikeMatrix() throws InstantiationException, IllegalAccessException {
        for (Class<? extends Vector> key : typesMap.keySet()) {
            Class<? extends Matrix> val = typesMap.get(key);

            if (val == null)
                System.out.println("Missing test for implementation of likeMatrix for " + key.getSimpleName());
        }
    }

    /** */ @Test
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
                expMatrixType.isAssignableFrom(matrix.getClass()));

            for (int rows : new int[] {0, 1, 2})
                for (int cols : new int[] {0, 1, 2}) {
                    final Matrix actualMatrix = v.likeMatrix(rows, cols);

                    String details = "rows " + rows + " cols " + cols;

                    assertNotNull("Expect non-null matrix for " + details + " in " + desc,
                        actualMatrix);

                    assertEquals("Unexpected number of rows in " + desc, rows, actualMatrix.rowSize());

                    assertEquals("Unexpected number of cols in " + desc, cols, actualMatrix.columnSize());
                }
        });
    }

    /** */ @Test
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

    /** */ @Test
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

    /** */ @Test
    @Ignore("not yet implemented test case for cross method")
    public void testCross() {
        //todo write code
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

        return typesMap.get(v.getClass()) != null;
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Vector, String> consumer) {
        new VectorImplementationsFixtures().consumeSampleVectors(null, consumer);
    }

    /** */
    private static Map<Class<? extends Vector>, Class<? extends Matrix>> typesMap() {
        return new LinkedHashMap<Class<? extends Vector>, Class<? extends Matrix>> () {{
            put(DenseLocalOnHeapVector.class, DenseLocalOnHeapMatrix.class);
            put(DenseLocalOffHeapVector.class, null); // todo fill non-nulls for all vectors that are ready to test
            put(RandomVector.class, null);
            put(ConstantVector.class, null);
            put(RandomAccessSparseLocalOnHeapVector.class, null);
            put(SequentialAccessSparseLocalOnHeapVector.class, null);
            put(SingleElementVector.class, null); // todo find out if we need SingleElementMatrix to match, or skip it
            // IMPL NOTE check for presence of all implementations here will be done in testHaveLikeMatrix via Fixture
        }};
    }

    /** */
    private static class Metric { // todo consider if softer tolerance (like say 0.1 or 0.01) would make sense here
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
