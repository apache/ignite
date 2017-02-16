package org.apache.ignite.math.impls.storage;

import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.UNEXPECTED_VALUE;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link SequentialAccessSparseVectorStorage}.
 * TODO wip
 */
public class SequentialAccessSparseVectorStorageTest extends VectorBaseStorageTest<SequentialAccessSparseVectorStorage> {

    @Override public void setUp() {
        storage = new SequentialAccessSparseVectorStorage();
    }

    /** */
    @Test
    public void data() throws Exception {

    }

    /** */
    @Test
    public void isSequentialAccess() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.isSequentialAccess());
    }

    /** */
    @Test
    public void isDense() throws Exception {
        assertFalse(UNEXPECTED_VALUE, storage.isDense());
    }

    /** */
    @Test
    public void getLookupCost() throws Exception {

    }

    /** */
    @Test
    public void isAddConstantTime() throws Exception {

    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertFalse(UNEXPECTED_VALUE, storage.isArrayBased());
    }

}