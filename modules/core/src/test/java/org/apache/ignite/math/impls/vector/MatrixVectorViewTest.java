package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.Vector;
import org.apache.ignite.math.exceptions.IndexException;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MatrixVectorView}
 */
public class MatrixVectorViewTest {
    /** */ private static final String UNEXPECTED_VALUE = "Unexpected value";
    /** */ private static final int SMALL_SIZE = 3;
    /** */ private static final int IMPOSSIBLE_SIZE = -1;

    /** */ private DenseLocalOnHeapMatrix parent;

    /** */
    @Before
    public void setup(){
        parent = new DenseLocalOnHeapMatrix(SMALL_SIZE, SMALL_SIZE);
        fillMatrix(parent);
    }

    /** */
    @Test
    public void testDiagonal(){
        Vector vector = parent.viewDiagonal();

        for (int i = 0; i < SMALL_SIZE; i++)
            assertView(i, i, vector, i);
    }

    /** */
    @Test
    public void testRow(){
        for (int i = 0; i < SMALL_SIZE; i++) {
            Vector viewRow = parent.viewRow(i);

            for (int j = 0; j < SMALL_SIZE; j++)
                assertView(i, j, viewRow, j);
        }
    }

    /** */
    @Test
    public void testCols(){
        for (int i = 0; i < SMALL_SIZE; i++) {
            Vector viewCol = parent.viewColumn(i);

            for (int j = 0; j < SMALL_SIZE; j++)
                assertView(j, i, viewCol, j);
        }
    }

    /** */ @Test(expected = AssertionError.class)
    public void parentNullTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(null, 1, 1, 1, 1).size());
    }

    /** */ @Test(expected = IndexException.class)
    public void rowNegativeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(parent, -1, 1, 1, 1).size());
    }

    /** */ @Test(expected = IndexException.class)
    public void colNegativeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(parent, 1, -1, 1, 1).size());
    }

    /** */ @Test(expected = IndexException.class)
    public void rowTooLargeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(parent, parent.rowSize() + 1, 1, 1, 1).size());
    }

    /** */ @Test(expected = IndexException.class)
    public void colTooLargeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(parent, 1, parent.columnSize() + 1, 1, 1).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void rowStrideNegativeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(parent, 1, 1, -1, 1).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void colStrideNegativeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(parent, 1, 1, 1, -1).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void rowStrideTooLargeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(parent, 1, 1, parent.rowSize() + 1, 1).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void colStrideTooLargeTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(parent, 1, 1, 1, parent.columnSize() + 1).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void bothStridesZeroTest() {
        //noinspection ConstantConditions
        assertEquals(IMPOSSIBLE_SIZE,
            new MatrixVectorView(parent, 1, 1, 0, 0).size());
    }

    /** */
    private void fillMatrix(DenseLocalOnHeapMatrix parent) {
        for(int i = 0; i < parent.rowSize(); i++)
            for(int j = 0; j < parent.columnSize(); j++)
                parent.set(i, j, i * parent.rowSize() + j);
    }

    /** */
    private void assertView(int row, int col, Vector view, int viewIdx) {
        assertValue(row, col, view, viewIdx);

        parent.set(row, col, parent.get(row, col) + 1);

        assertValue(row, col, view, viewIdx);

        view.set(viewIdx, view.get(viewIdx) + 2);

        assertValue(row, col, view, viewIdx);
    }

    /** */
    private void assertValue(int row, int col, Vector view, int viewIdx) {
        assertEquals(UNEXPECTED_VALUE + " at row " + row + " col " + col, parent.get(row, col), view.get(viewIdx), 0d);
    }
}
