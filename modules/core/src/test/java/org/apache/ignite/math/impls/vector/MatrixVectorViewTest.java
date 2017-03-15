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
            assertEquals(UNEXPECTED_VALUE + " at row / col " + i, parent.get(i, i), vector.get(i), 0d);
    }

    /** */
    @Test
    public void testRow(){
        for (int i = 0; i < SMALL_SIZE; i++) {
            Vector viewRow = parent.viewRow(i);

            for (int j = 0; j < SMALL_SIZE; j++)
                assertEquals(UNEXPECTED_VALUE + " at row " + i + " col " + j, parent.get(i, j), viewRow.get(j), 0d);
        }
    }

    /** */
    @Test
    public void testCols(){
        for (int i = 0; i < SMALL_SIZE; i++) {
            Vector viewRow = parent.viewColumn(i);

            for (int j = 0; j < SMALL_SIZE; j++)
                assertEquals(UNEXPECTED_VALUE+ " at row " + i + " col " + j, parent.get(j, i), viewRow.get(j), 0d);
        }
    }

    /** */
    private void fillMatrix(DenseLocalOnHeapMatrix parent) {
        for(int i = 0; i < parent.rowSize(); i++)
            for(int j = 0; j < parent.columnSize(); j++)
                parent.set(i, j, i*parent.rowSize() + j);
    }
}
