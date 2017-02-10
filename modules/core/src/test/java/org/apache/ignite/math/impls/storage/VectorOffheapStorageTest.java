package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.impls.MathTestConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link VectorOffheapStorage}.
 */
public class VectorOffheapStorageTest {

    private VectorOffheapStorage offheapVector;

    /** */
    @Before
    public void setUp() throws Exception {
        offheapVector = new VectorOffheapStorage(STORAGE_SIZE);
    }

    /** */
    @After
    public void tearDown() throws Exception {
        offheapVector.destroy();
    }

    /** */
    @Test
    public void size() throws Exception {
        assertEquals(MathTestConstants.VALUE_NOT_EQUALS,offheapVector.size(), STORAGE_SIZE);
    }

    /** */
    @Test
    public void get() throws Exception {

    }

    /** */
    @Test
    public void set() throws Exception {

    }

    /** */
    @Test
    public void isArrayBased() throws Exception {

    }

    /** */
    @Test
    public void data() throws Exception {
        assertNull(MathTestConstants.NULL_VALUE, offheapVector.data());
    }

    /** */
    @Test
    public void isSequentialAccess() throws Exception {

    }

    /** */
    @Test
    public void isDense() throws Exception {

    }

    /** */
    @Test
    public void getLookupCost() throws Exception {

    }

    /** */
    @Test
    public void isAddConstantTime() throws Exception {

    }

}