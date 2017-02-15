package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.impls.MathTestConstants;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.UNEXPECTED_VALUE;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link MatrixNullStorage}.
 */
public class MatrixNullStorageTest extends MatrixBaseStorageTest<MatrixNullStorage> {
    /** {@inheritDoc} */
    @Override public void setUp() {
        storage = new MatrixNullStorage();
    }

    @Test (expected = UnsupportedOperationException.class)
    @Override public void getSet() throws Exception {
        super.getSet();
    }

    /** */
    @Test (expected = UnsupportedOperationException.class)
    public void isSequentialAccess() throws Exception {
        storage.isSequentialAccess();
    }

    /** */
    @Test (expected = UnsupportedOperationException.class)
    public void isDense() throws Exception {
        storage.isDense();
    }

    /** */
    @Test (expected = UnsupportedOperationException.class)
    public void getLookupCost() throws Exception {
        storage.getLookupCost();
    }

    /** */
    @Test (expected = UnsupportedOperationException.class)
    public void isAddConstantTime() throws Exception {
        storage.isAddConstantTime();
    }

    /** */
    @Test (expected = UnsupportedOperationException.class)
    public void isArrayBased() throws Exception {
        storage.isArrayBased();
    }

    /** */
    @Test (expected = UnsupportedOperationException.class)
    public void data() throws Exception {
        storage.data();
    }

    /** */
    @Override public void rowSize() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.rowSize() == 0);
    }

    /** */
    @Override public void columnSize() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.columnSize() == 0);
    }
}