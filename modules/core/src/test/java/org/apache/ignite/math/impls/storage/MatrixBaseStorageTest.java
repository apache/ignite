package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.MatrixStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.assertEquals;

/**
 * Abstract class with base tests for each matrix storage.
 */
public abstract class MatrixBaseStorageTest<T extends MatrixStorage> extends ExternalizeTest<T> {
    /** */
    protected T storage;

    /** */
    @Before
    public abstract void setUp();

    /** */
    @After
    public void tearDown() throws Exception {
        storage.destroy();
    }

    /** */
    @Test
    public void getSet() throws Exception {
        int rows = STORAGE_SIZE;
        int cols = STORAGE_SIZE;

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double data = Math.random();

                storage.set(i, j, data);

                Assert.assertEquals(VALUE_NOT_EQUALS, storage.get(i, j), data, NIL_DELTA);
            }
        }
    }

    /** */
    @Test
    public void columnSize() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, storage.columnSize(), STORAGE_SIZE);
    }

    /** */
    @Test
    public void rowSize() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, storage.rowSize(), STORAGE_SIZE);
    }

    /** */
    @Override public void externalizeTest() {
        super.externalizeTest(storage);
    }
}
