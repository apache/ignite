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
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link MatrixOffHeapStorage}.
 */
public class MatrixOffHeapStorageTest {

    /** */
    private MatrixOffHeapStorage matrixOffHeapStorage;

    @Before
    public void setUp() throws Exception {
        matrixOffHeapStorage = new MatrixOffHeapStorage(STORAGE_SIZE, STORAGE_SIZE);
    }

    @After
    public void tearDown() throws Exception {
        matrixOffHeapStorage.destroy();
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
    public void data() throws Exception {
        assertNull(UNEXPECTED_VALUE, matrixOffHeapStorage.data());
    }

    @Test
    public void writeReadExternal() throws Exception {
        // TODO
    }

}