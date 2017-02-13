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
public class VectorOffheapStorageTest {

    /** */
    private static final double DOUBLE_ZERO = 0d;

    /** */
    private static final String EXTERNALIZE_TEST_FILE_NAME = "externalizeTest";

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
    @AfterClass
    public static void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get(EXTERNALIZE_TEST_FILE_NAME));
    }

    /** */
    @Test
    public void size() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, offheapVector.size(), STORAGE_SIZE);
    }

    /** */
    @Test
    public void getSetTest() {
        double[] data = DoubleStream.generate(() -> Math.random()).limit(STORAGE_SIZE).toArray();

        for (int i = 0; i < data.length; i++) {
            double refVal = data[i];

            offheapVector.set(i, refVal);

            assertEquals(VALUE_NOT_EQUALS, offheapVector.get(i), refVal, NIL_DELTA);
        }
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertFalse(UNEXPECTED_VALUE, offheapVector.isArrayBased());
    }

    /** */
    @Test
    public void data() throws Exception {
        assertNull(NULL_VALUE, offheapVector.data());
    }

    /** */
    @Test
    public void isSequentialAccess() throws Exception {
        assertTrue(UNEXPECTED_VALUE, offheapVector.isSequentialAccess());
    }

    /** */
    @Test
    public void isDense() throws Exception {
        assertTrue(UNEXPECTED_VALUE, offheapVector.isDense());
    }

    /** */
    @Test
    public void getLookupCost() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, offheapVector.getLookupCost(), DOUBLE_ZERO, NIL_DELTA);
    }

    /** */
    @Test
    public void isAddConstantTime() throws Exception {
        assertTrue(UNEXPECTED_VALUE, offheapVector.isAddConstantTime());
    }

    /** */
    @Test
    public void equalsTest(){
        assertEquals(VALUE_NOT_EQUALS, offheapVector, offheapVector);

        assertNotEquals(VALUES_SHOULD_BE_NOT_EQUALS, offheapVector, new VectorArrayStorage(STORAGE_SIZE));
    }

    /** */
    @Test
    public void externalizeTest() {
        File f = new File(EXTERNALIZE_TEST_FILE_NAME);

        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(f));

            objectOutputStream.writeObject(offheapVector);

            objectOutputStream.close();

            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(f));

            VectorOffheapStorage vectorOffheapStorage = (VectorOffheapStorage) objectInputStream.readObject();

            objectInputStream.close();

            assertEquals(VALUE_NOT_EQUALS, offheapVector, vectorOffheapStorage);
        } catch (ClassNotFoundException | IOException e) {
            fail(e.getMessage());
        }
    }

}