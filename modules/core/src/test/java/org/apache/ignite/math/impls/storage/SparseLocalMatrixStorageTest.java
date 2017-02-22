package org.apache.ignite.math.impls.storage;


import static org.apache.ignite.math.impls.MathTestConstants.*;

/**
 * Unit tests for {@link SparseLocalMatrixStorage}.
 */
public class SparseLocalMatrixStorageTest extends MatrixBaseStorageTest<SparseLocalMatrixStorage> {
    @Override public void setUp() {
        storage = new SparseLocalMatrixStorage(STORAGE_SIZE, STORAGE_SIZE);
    }
}
