package org.apache.ignite.math.impls.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.ignite.math.VectorStorage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.NIL_DELTA;
import static org.apache.ignite.math.impls.MathTestConstants.STORAGE_SIZE;
import static org.apache.ignite.math.impls.MathTestConstants.UNEXPECTED_VALUE;
import static org.apache.ignite.math.impls.MathTestConstants.VALUE_NOT_EQUALS;
import static org.apache.ignite.math.impls.MathTestConstants.WRONG_DATA_ELEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract class with base tests for each vector storage.
 */
public abstract class VectorBaseStorageTest<T extends VectorStorage> {

    protected T storage;

    /** */
    private static final String EXTERNALIZE_TEST_FILE_NAME = "externalizeTest";

    @Before
    public abstract void setUp();

    @After
    public void tearDown() throws Exception {
        storage.destroy();
    }

    /** */
    @AfterClass
    public static void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get(EXTERNALIZE_TEST_FILE_NAME));
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
    @Test
    public void externalizeTest() {
        File f = new File(EXTERNALIZE_TEST_FILE_NAME);

        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(f));

            objectOutputStream.writeObject(storage);

            objectOutputStream.close();

            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(f));

            T vectorRestored = (T) objectInputStream.readObject();

            objectInputStream.close();

            assertTrue(VALUE_NOT_EQUALS, storage.equals(vectorRestored));
        } catch (ClassNotFoundException | IOException e) {
            fail(e.getMessage());
        }
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
