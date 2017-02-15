package org.apache.ignite.math.impls.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.ignite.math.MatrixStorage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.NIL_DELTA;
import static org.apache.ignite.math.impls.MathTestConstants.STORAGE_SIZE;
import static org.apache.ignite.math.impls.MathTestConstants.VALUE_NOT_EQUALS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract class with base tests for each matrix storage.
 */
public abstract class MatrixBaseStorageTest<T extends MatrixStorage> {

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

    @Test
    public void columnSize() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, storage.columnSize(), STORAGE_SIZE);
    }

    @Test
    public void rowSize() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, storage.rowSize(), STORAGE_SIZE);
    }

    @Test
    public void writeReadExternal() throws Exception {
        File f = new File(EXTERNALIZE_TEST_FILE_NAME);

        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(f));

            objectOutputStream.writeObject(storage);

            objectOutputStream.close();

            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(f));

            T storageRestored = (T) objectInputStream.readObject();

            objectInputStream.close();

            assertTrue(VALUE_NOT_EQUALS, storage.equals(storageRestored));
        } catch (ClassNotFoundException | IOException e) {
            fail(e.getMessage());
        }
    }
}
