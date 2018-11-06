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

package org.apache.ignite.ml.math.primitives.matrix;

import org.apache.ignite.ml.math.primitives.matrix.impl.SparseMatrix;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** */
public class SparseMatrixConstructorTest {
    /** */
    @Test
    public void invalidArgsTest() {
        DenseMatrixConstructorTest.verifyAssertionError(() -> new SparseMatrix(0, 1),
            "invalid row parameter");

        DenseMatrixConstructorTest.verifyAssertionError(() -> new SparseMatrix(1, 0),
            "invalid col parameter");
    }

    /** */
    @Test
    public void basicTest() {
        assertEquals("Expected number of rows.", 1,
            new SparseMatrix(1, 2).rowSize());

        assertEquals("Expected number of cols, int parameters.", 1,
            new SparseMatrix(2, 1).columnSize());

        SparseMatrix m = new SparseMatrix(1, 1);
        //noinspection EqualsWithItself
        assertTrue("Matrix is expected to be equal to self.", m.equals(m));
        //noinspection ObjectEqualsNull
        assertFalse("Matrix is expected to be not equal to null.", m.equals(null));
    }
}
