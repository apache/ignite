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

import java.util.function.Supplier;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/** */
public class DenseMatrixConstructorTest {
    /** */
    @Test
    public void invalidArgsTest() {
        verifyAssertionError(() -> new DenseMatrix(0, 1), "invalid row parameter");

        verifyAssertionError(() -> new DenseMatrix(1, 0), "invalid col parameter");

        //noinspection ConstantConditions
        verifyAssertionError(() -> new DenseMatrix(null), "null matrix parameter");

        verifyAssertionError(() -> new DenseMatrix(new double[][] {null, new double[1]}),
            "null row in matrix");
    }

    /** */
    @Test
    public void basicTest() {
        assertEquals("Expected number of rows, int parameters.", 1,
            new DenseMatrix(1, 2).rowSize());

        assertEquals("Expected number of rows, double[][] parameter.", 1,
            new DenseMatrix(new double[][] {new double[2]}).rowSize());

        assertEquals("Expected number of cols, int parameters.", 1,
            new DenseMatrix(2, 1).columnSize());

        assertEquals("Expected number of cols, double[][] parameter.", 1,
            new DenseMatrix(new double[][] {new double[1], new double[1]}).columnSize());
    }

    /** */
    static void verifyAssertionError(Supplier<Matrix> ctor, String desc) {
        try {
            assertNotNull("Unexpected null matrix in " + desc, ctor.get());
        }
        catch (AssertionError ae) {
            return;
        }

        fail("Expected error not caught in " + desc);
    }
}
