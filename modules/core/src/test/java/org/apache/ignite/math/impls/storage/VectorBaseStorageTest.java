package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.VectorStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Abstract class with base tests for each vector storage.
 */
public abstract class VectorBaseStorageTest<T extends VectorStorage> extends ExternalizeTest<T> {
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
        for (int i = 0; i < STORAGE_SIZE; i++) {
            double random = Math.random();

            storage.set(i, random);

            assertEquals(WRONG_DATA_ELEMENT, storage.get(i), random, NIL_DELTA);
        }
    }

    /** */
    @Test
    public void size(){
        assertTrue(UNEXPECTED_VALUE, storage.size() == STORAGE_SIZE);
    }

    /** */
    @Override public void externalizeTest() {
        super.externalizeTest(storage);
    }

    /**
     * Fill storage by random doubles.
     *
     * @param size Storage size.
     */
    private void fillStorage(int size) {
        for (int i = 0; i < size; i++)
            storage.set(i, Math.random());
    }
}
