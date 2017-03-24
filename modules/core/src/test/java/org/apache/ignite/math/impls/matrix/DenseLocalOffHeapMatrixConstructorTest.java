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

package org.apache.ignite.math.impls.matrix;

import org.junit.Test;

import static org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError;
import static org.junit.Assert.*;

/** */
public class DenseLocalOffHeapMatrixConstructorTest {
    /** */ @Test
    public void mapInvalidArgsTest() {
        verifyAssertionError(() -> new DenseLocalOffHeapMatrix(0, 1), "invalid row parameter");

        verifyAssertionError(() -> new DenseLocalOffHeapMatrix(1, 0), "invalid col parameter");

        //noinspection ConstantConditions
        verifyAssertionError(() -> new DenseLocalOffHeapMatrix(null), "null matrix parameter");

        verifyAssertionError(() -> new DenseLocalOffHeapMatrix(new double[][] {null, new double[1]}),
            "null row in matrix");
    }

    /** */ @Test
    public void basicTest() {
        assertEquals("Expected number of rows, int parameters.", 1,
            new DenseLocalOffHeapMatrix(1, 2).rowSize());

        assertEquals("Expected number of rows, double[][] parameter.", 1,
            new DenseLocalOffHeapMatrix(new double[][] {new double[2]}).rowSize()); // todo find what's wrong

        assertEquals("Expected number of cols, int parameters.", 1,
            new DenseLocalOffHeapMatrix(2, 1).columnSize());

        assertEquals("Expected number of cols, double[][] parameter.", 1,
            new DenseLocalOffHeapMatrix(new double[][] {new double[1], new double[1]}).columnSize());
    }
}
