package org.apache.ignite.math.impls;

import org.apache.ignite.math.impls.storage.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Unit test for {@link VectorArrayStorage}.
 */
public class VectorArrayStorageTest {
    /** */
    static final int STORAGE_SIZE = 100;

    /** */
    private static final String WRONG_ATTRIBUTE_VALUE = "Wrong attribute value.";

    /** */
    private static final String NULL_DATA_ELEMENT = "Null data element.";

    /** */
    private static final String WRONG_DATA_ELEMENT = "Wrong data element.";

    /** */
    static final double NIL_DELTA = 0d;

    /** */
    static final String NULL_DATA_STORAGE = "Null data storage.";

    /** */
    private static final String WRONG_DATA_SIZE = "Wrong data size.";

    /** */
    private static final String UNEXPECTED_DATA_VALUE = "Unexpected data value.";

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
