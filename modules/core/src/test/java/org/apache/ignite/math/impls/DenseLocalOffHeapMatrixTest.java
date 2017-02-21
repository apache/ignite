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

package org.apache.ignite.math.impls;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link DenseLocalOffHeapMatrix}.
 */
public class DenseLocalOffHeapMatrixTest {
    /** */ private DenseLocalOffHeapMatrix denseLocalOffHeapMatrix;

    /** */
    @Before
    public void setUp() throws Exception {
        denseLocalOffHeapMatrix = new DenseLocalOffHeapMatrix(STORAGE_SIZE, STORAGE_SIZE);
    }

    /** */
    @After
    public void tearDown() throws Exception {
        denseLocalOffHeapMatrix.destroy();
    }

    /** */
    @Test
    public void copy() throws Exception {
        int rows = STORAGE_SIZE;
        int cols = STORAGE_SIZE;

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                denseLocalOffHeapMatrix.set(i, j, Math.random());

        Matrix cp = denseLocalOffHeapMatrix.copy();

        try{
            assertTrue(UNEXPECTED_VALUE, cp.getClass() == DenseLocalOffHeapMatrix.class);

            for (int i = 0; i < rows; i++)
                for (int j = 0; j < cols; j++)
                    assertTrue(UNEXPECTED_VALUE, Double.compare(denseLocalOffHeapMatrix.get(i, j), cp.get(i, j)) == 0);

            double randomVal = Math.random();
            denseLocalOffHeapMatrix.set(0, 0, randomVal);

            assertTrue(UNEXPECTED_VALUE, Double.compare(denseLocalOffHeapMatrix.get(0, 0), cp.get(0, 0)) != 0);
        } finally {
            cp.destroy();
        }
    }

    /** */
    @Test
    public void like() throws Exception {
        Matrix like = denseLocalOffHeapMatrix.like(STORAGE_SIZE, STORAGE_SIZE);

        try{
            assertTrue(UNEXPECTED_VALUE, like.getClass() == DenseLocalOffHeapMatrix.class);

            assertTrue(UNEXPECTED_VALUE, like.rowSize() == STORAGE_SIZE && like.columnSize() == STORAGE_SIZE);
        } finally {
            like.destroy();
        }
    }

    /** */
    @Test
    public void likeVector() throws Exception {
        Vector vector = denseLocalOffHeapMatrix.likeVector(STORAGE_SIZE);

        try{
            assertTrue(UNEXPECTED_VALUE, vector.getClass() == DenseLocalOffHeapVector.class);
            assertTrue(UNEXPECTED_VALUE, vector.getStorage().size() == STORAGE_SIZE);
        } finally {
            vector.destroy();
        }
    }

}
