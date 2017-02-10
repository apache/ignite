package org.apache.ignite.math.impls.storage;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit test for {@link VectorArrayStorage}.
 */
public class VectorArrayStorageTest {

    /** */
    private VectorArrayStorage testStorage;

    /** */
    @Before
    public void setup() {
        testStorage = new VectorArrayStorage(STORAGE_SIZE);
    }

    /** */
    @Test
    public void get() throws Exception {
        fillStorage(STORAGE_SIZE);

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertNotNull(NULL_DATA_ELEMENT, testStorage.get(i));
    }

    /** */
    @Test
    public void set() throws Exception {
        for (int i = 0; i < STORAGE_SIZE; i++) {
            double random = Math.random();

            testStorage.set(i, random);

            assertEquals(WRONG_DATA_ELEMENT, testStorage.get(i), random, NIL_DELTA);
        }
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertTrue(WRONG_ATTRIBUTE_VALUE, testStorage.isArrayBased());

        assertTrue(WRONG_ATTRIBUTE_VALUE, new VectorArrayStorage().isArrayBased());
    }

    /** */
    @Test
    public void data() throws Exception {
        assertNotNull(NULL_DATA_STORAGE, testStorage.data());

        assertEquals(WRONG_DATA_SIZE, testStorage.data().length, STORAGE_SIZE);

        assertTrue(UNEXPECTED_DATA_VALUE, Arrays.equals(testStorage.data(), new double[STORAGE_SIZE]));

        assertNull(UNEXPECTED_DATA_VALUE, new VectorArrayStorage().data());
    }

    /**
     * Fill storage by random doubles.
     *
     * @param size Storage size.
     */
    private void fillStorage(int size) {
        for (int i = 0; i < size; i++)
            testStorage.set(i, Math.random());
    }

}
