package org.apache.ignite.math.impls;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link DenseLocalOffHeapMatrix}.
 */
public class DenseLocalOffHeapMatrixTest {
    DenseLocalOffHeapMatrix denseLocalOffHeapMatrix;

    /** */
    @Before
    public void setUp() throws Exception {
        denseLocalOffHeapMatrix = new DenseLocalOffHeapMatrix(STORAGE_SIZE, STORAGE_SIZE);
    }

    /** */
    @After
    public void tearDown() throws Exception {
        denseLocalOffHeapMatrix.destroy();
    }

    /** */
    @Test
    public void copy() throws Exception {
        int rows = STORAGE_SIZE;
        int cols = STORAGE_SIZE;

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                denseLocalOffHeapMatrix.set(i, j, Math.random());
            }
        }

        Matrix copy = denseLocalOffHeapMatrix.copy();

        try{
            assertTrue(UNEXPECTED_VALUE, copy.getClass() == DenseLocalOffHeapMatrix.class);

            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    assertTrue(UNEXPECTED_VALUE, Double.compare(denseLocalOffHeapMatrix.get(i, j), copy.get(i, j)) == 0);
                }
            }

            double randomValue = Math.random();
            denseLocalOffHeapMatrix.set(0, 0, randomValue);

            assertTrue(UNEXPECTED_VALUE, Double.compare(denseLocalOffHeapMatrix.get(0, 0), copy.get(0, 0)) != 0);
        } finally {
            copy.destroy();
        }
    }

    /** */
    @Test
    public void like() throws Exception {
        Matrix like = denseLocalOffHeapMatrix.like(STORAGE_SIZE, STORAGE_SIZE);

        try{
            assertTrue(UNEXPECTED_VALUE, like.getClass() == DenseLocalOffHeapMatrix.class);

            assertTrue(UNEXPECTED_VALUE, like.rowSize() == STORAGE_SIZE && like.columnSize() == STORAGE_SIZE);
        } finally {
            like.destroy();
        }
    }

    /** */
    @Test
    public void likeVector() throws Exception {
        Vector vector = denseLocalOffHeapMatrix.likeVector(STORAGE_SIZE);

        try{
            assertTrue(UNEXPECTED_VALUE, vector.getClass() == DenseLocalOffHeapVector.class);
            assertTrue(UNEXPECTED_VALUE, vector.getStorage().size() == STORAGE_SIZE);
        } finally {
            vector.destroy();
        }
    }

}