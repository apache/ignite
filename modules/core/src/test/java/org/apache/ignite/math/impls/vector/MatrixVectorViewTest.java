package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.Vector;
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
