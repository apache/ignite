package org.apache.ignite.math.impls.storage;

import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.NULL_VALUE;
import static org.apache.ignite.math.impls.MathTestConstants.STORAGE_SIZE;
import static org.apache.ignite.math.impls.MathTestConstants.UNEXPECTED_VALUE;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link MatrixArrayStorage}.
 */
public class MatrixArrayStorageTest extends MatrixBaseStorageTest<MatrixArrayStorage>{

    @Override public void setUp() {
        storage = new MatrixArrayStorage(STORAGE_SIZE, STORAGE_SIZE);
    }

    /** */
    @Test
    public void isSequentialAccess() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.isSequentialAccess());
    }

    /** */
    @Test
    public void isDense() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.isDense());
    }

    /** */
    @Test
    public void getLookupCost() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.getLookupCost() == 0);
    }

    /** */
    @Test
    public void isAddConstantTime() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.isAddConstantTime());
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.isArrayBased());
    }

    /** */
    @Test
    public void data() throws Exception {
        double[][] data = storage.data();
        assertNotNull(NULL_VALUE, data);
        assertTrue(UNEXPECTED_VALUE, data.length == STORAGE_SIZE);
        assertTrue(UNEXPECTED_VALUE, data[0].length == STORAGE_SIZE);
    }

}