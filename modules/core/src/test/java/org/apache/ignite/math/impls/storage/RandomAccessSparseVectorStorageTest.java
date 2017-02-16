package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.impls.MathTestConstants;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.NULL_VALUE;
import static org.apache.ignite.math.impls.MathTestConstants.STORAGE_SIZE;
import static org.apache.ignite.math.impls.MathTestConstants.UNEXPECTED_VALUE;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link RandomAccessSparseVectorStorage}.
 * TODO wip
 */
public class RandomAccessSparseVectorStorageTest extends VectorBaseStorageTest<RandomAccessSparseVectorStorage> {
    /** */
    @Before
    public void setUp(){
        storage = new RandomAccessSparseVectorStorage(STORAGE_SIZE);
    }

    /** */
    @Test
    public void data() throws Exception {
        double[] data = storage.data();
        assertNotNull(NULL_VALUE, data);
        assertEquals(UNEXPECTED_VALUE, data.length, STORAGE_SIZE);
    }

    /** */
    @Test
    public void isSequentialAccess() throws Exception {
        assertFalse(UNEXPECTED_VALUE, storage.isSequentialAccess());
    }

    /** */
    @Test
    public void isDense() throws Exception {
        assertFalse(UNEXPECTED_VALUE, storage.isDense());
    }

    /** */
    @Test
    public void getLookupCost() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.getLookupCost() == 1);
    }

    /** */
    @Test
    public void isAddConstantTime() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.isAddConstantTime());
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertFalse(UNEXPECTED_VALUE, storage.isArrayBased());
    }

}