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

import org.apache.ignite.math.ExternalizeTest;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.MathTestConstants;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DiagonalMatrix}.
 */
public class DiagonalMatrixTest extends ExternalizeTest<DiagonalMatrix> {
    /** */ public static final String UNEXPECTED_VALUE = "Unexpected value.";

    /** */ private DiagonalMatrix testMatrix;

    /** */
    @Before
    public void setup(){
        DenseLocalOnHeapMatrix parent = new DenseLocalOnHeapMatrix(MathTestConstants.STORAGE_SIZE, MathTestConstants.STORAGE_SIZE);
        fillMatrix(parent);
        testMatrix = new DiagonalMatrix(parent);
    }

    /** {@inheritDoc} */
    @Override public void externalizeTest() {
        externalizeTest(testMatrix);
    }

    /** */
    @Test
    public void testSetGet() {
        double testVal = 42;
        for (int i = 0; i < MathTestConstants.STORAGE_SIZE; i++) {
            testMatrix.set(i, i, testVal);

            assertEquals(UNEXPECTED_VALUE, testMatrix.get(i, i), testVal, 0d);
        }
    }

    /** */
    @Test
    public void testSetGetNegative(){
        double testVal = 42;

        for(int i = 0; i < MathTestConstants.STORAGE_SIZE; i++)
            for(int j = 0; j < MathTestConstants.STORAGE_SIZE; j++)
                try {
                    testMatrix.set(i, j, testVal);

                    if (i != j)
                        fail("UnsupportedOperationException expected.");

                    assertEquals(UNEXPECTED_VALUE, testMatrix.get(i, i), testVal, 0d);

                }
                catch (UnsupportedOperationException e) {
                    // No-op.
                }
    }

    /** */
    @Test
    public void testAttributes(){
        assertTrue(UNEXPECTED_VALUE, testMatrix.rowSize() == MathTestConstants.STORAGE_SIZE);
        assertTrue(UNEXPECTED_VALUE, testMatrix.columnSize() == MathTestConstants.STORAGE_SIZE);

        assertFalse(UNEXPECTED_VALUE, testMatrix.isArrayBased());
        assertTrue(UNEXPECTED_VALUE, testMatrix.isDense());
        assertFalse(UNEXPECTED_VALUE, testMatrix.isDistributed());

        assertEquals(UNEXPECTED_VALUE, testMatrix.isRandomAccess(), !testMatrix.isSequentialAccess());
        assertTrue(UNEXPECTED_VALUE, testMatrix.isRandomAccess());
    }

    /** */
    @Test
    public void testNullParams() {
        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new DiagonalMatrix((Matrix)null), "Null Matrix parameter");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new DiagonalMatrix((Vector)null), "Null Vector parameter");

        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new DiagonalMatrix((double[])null), "Null double[] parameter");
    }

    /** */
    private void fillMatrix(Matrix m){
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, Math.random());
    }

}
