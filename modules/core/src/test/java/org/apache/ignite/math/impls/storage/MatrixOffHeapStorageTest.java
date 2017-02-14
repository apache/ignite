/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.math.impls.storage;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link MatrixOffHeapStorage}.
 */
public class MatrixOffHeapStorageTest {

    /** */
    private MatrixOffHeapStorage matrixOffHeapStorage;

    /** */
    private static final String EXTERNALIZE_TEST_FILE_NAME = "externalizeTest";

    @Before
    public void setUp() throws Exception {
        matrixOffHeapStorage = new MatrixOffHeapStorage(STORAGE_SIZE, STORAGE_SIZE);
    }

    @After
    public void tearDown() throws Exception {
        matrixOffHeapStorage.destroy();
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

                matrixOffHeapStorage.set(i, j, data);

                assertEquals(VALUE_NOT_EQUALS, matrixOffHeapStorage.get(i, j), data, NIL_DELTA);
            }
        }
    }

    @Test
    public void columnSize() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, matrixOffHeapStorage.columnSize(), STORAGE_SIZE);
    }

    @Test
    public void rowSize() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, matrixOffHeapStorage.rowSize(), STORAGE_SIZE);
    }

    @Test
    public void isArrayBased() throws Exception {
        assertFalse(UNEXPECTED_VALUE, matrixOffHeapStorage.isArrayBased());
    }

    @Test
    public void isSequentialAccess() throws Exception {
        assertTrue(UNEXPECTED_VALUE, matrixOffHeapStorage.isSequentialAccess());
    }

    @Test
    public void isDense() throws Exception {
        assertTrue(UNEXPECTED_VALUE, matrixOffHeapStorage.isDense());
    }

    @Test
    public void getLookupCost() throws Exception {
        assertTrue(UNEXPECTED_VALUE, matrixOffHeapStorage.getLookupCost() == 0d);
    }

    @Test
    public void isAddConstantTime() throws Exception {
        assertTrue(UNEXPECTED_VALUE, matrixOffHeapStorage.isAddConstantTime());
    }

    @Test
    public void data() throws Exception {
        assertNull(UNEXPECTED_VALUE, matrixOffHeapStorage.data());
    }

    @Test
    public void writeReadExternal() throws Exception {
        File f = new File(EXTERNALIZE_TEST_FILE_NAME);

        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(f));

            objectOutputStream.writeObject(matrixOffHeapStorage);

            objectOutputStream.close();

            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(f));

            MatrixOffHeapStorage mohsRestored = (MatrixOffHeapStorage) objectInputStream.readObject();

            objectInputStream.close();

            assertTrue(VALUE_NOT_EQUALS, matrixOffHeapStorage.equals(mohsRestored));
        } catch (ClassNotFoundException | IOException e) {
            fail(e.getMessage());
        }
    }

}