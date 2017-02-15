package org.apache.ignite.math.impls.storage;

import org.junit.Test;

import java.util.Arrays;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit test for {@link VectorArrayStorage}.
 */
public class VectorArrayStorageTest extends VectorBaseStorageTest<VectorArrayStorage> {
    /** */
    @Override public void setUp() {
        storage = new VectorArrayStorage(STORAGE_SIZE);
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertTrue(WRONG_ATTRIBUTE_VALUE, storage.isArrayBased());

        assertTrue(WRONG_ATTRIBUTE_VALUE, new VectorArrayStorage().isArrayBased());
    }

    /** */
    @Test
    public void data() throws Exception {
        assertNotNull(NULL_DATA_STORAGE, storage.data());

        assertEquals(WRONG_DATA_SIZE, storage.data().length, STORAGE_SIZE);

        assertTrue(UNEXPECTED_DATA_VALUE, Arrays.equals(storage.data(), new double[STORAGE_SIZE]));

        assertNull(UNEXPECTED_DATA_VALUE, new VectorArrayStorage().data());
    }

}
