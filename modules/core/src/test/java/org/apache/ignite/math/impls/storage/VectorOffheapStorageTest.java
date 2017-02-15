package org.apache.ignite.math.impls.storage;

import static org.apache.ignite.math.impls.MathTestConstants.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.DoubleStream;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link VectorOffheapStorage}.
 */
public class VectorOffheapStorageTest extends VectorBaseStorageTest<VectorOffheapStorage> {
    /** */
    private static final double DOUBLE_ZERO = 0d;

    /** */
    @Before
    public void setUp() {
        storage = new VectorOffheapStorage(STORAGE_SIZE);
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertFalse(UNEXPECTED_VALUE, storage.isArrayBased());
    }

    /** */
    @Test
    public void data() throws Exception {
        assertNull(NULL_VALUE, storage.data());
    }

    /** */
    @Test
    public void isSequentialAccess() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.isSequentialAccess());
    }

    /** */
    @Test
    public void isDense() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.isDense());
    }

    /** */
    @Test
    public void getLookupCost() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, storage.getLookupCost(), DOUBLE_ZERO, NIL_DELTA);
    }

    /** */
    @Test
    public void isAddConstantTime() throws Exception {
        assertTrue(UNEXPECTED_VALUE, storage.isAddConstantTime());
    }

    /** */
    @Test
    public void equalsTest(){
        assertTrue(VALUE_NOT_EQUALS, storage.equals(storage));

        assertFalse(VALUES_SHOULD_BE_NOT_EQUALS, storage.equals(new VectorArrayStorage(STORAGE_SIZE)));
    }

}