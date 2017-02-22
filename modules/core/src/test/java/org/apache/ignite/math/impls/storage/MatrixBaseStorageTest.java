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

import org.apache.ignite.math.MatrixStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.assertEquals;

/**
 * Abstract class with base tests for each matrix storage.
 */
public abstract class MatrixBaseStorageTest<T extends MatrixStorage> extends ExternalizeTest<T> {
    /** */
    protected T storage;

    /** */
    @Before
    public abstract void setUp();

    /** */
    @After
    public void tearDown() throws Exception {
        storage.destroy();
    }

    /** */
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

    /** */
    @Test
    public void columnSize() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, storage.columnSize(), STORAGE_SIZE);
    }

    /** */
    @Test
    public void rowSize() throws Exception {
        assertEquals(VALUE_NOT_EQUALS, storage.rowSize(), STORAGE_SIZE);
    }

    /** */
    @Override public void externalizeTest() {
        fillMatrix();
        super.externalizeTest(storage);
    }

    protected void fillMatrix(){
        for (int i = 0; i < storage.rowSize(); i++) {
            for (int j = 0; j < storage.columnSize(); j++)
                storage.set(i, j, Math.random());
        }
    }
}
