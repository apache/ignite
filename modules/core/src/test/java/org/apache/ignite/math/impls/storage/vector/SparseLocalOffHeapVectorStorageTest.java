package org.apache.ignite.math.impls.storage.vector;

import org.apache.ignite.math.impls.MathTestConstants;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.NULL_VALUE;
import static org.apache.ignite.math.impls.MathTestConstants.UNEXPECTED_VALUE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SparseOffHeapVectorStorage}.
 */
public class SparseLocalOffHeapVectorStorageTest extends VectorBaseStorageTest<SparseOffHeapVectorStorage> {
    @Override public void setUp() {
        storage = new SparseOffHeapVectorStorage(MathTestConstants.STORAGE_SIZE);
    }

    /** */
    @Test
    public void data() throws Exception {
        assertNull(NULL_VALUE, storage.data());
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

    /** */
    @Test
    public void isDense() throws Exception {
        assertFalse(UNEXPECTED_VALUE, storage.isDense());
    }
}
