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

package org.apache.ignite.ml.math.impls.matrix;

import org.apache.ignite.ml.math.ExternalizeTest;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.MathTestConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TransposedMatrixView}.
 */
public class TransposedMatrixViewTest extends ExternalizeTest<TransposedMatrixView> {
    /** */
    private static final String UNEXPECTED_VALUE = "Unexpected value";
    /** */
    private TransposedMatrixView testMatrix;
    /** */
    private DenseLocalOnHeapMatrix parent;

    /** */
    @Before
    public void setup() {
        parent = new DenseLocalOnHeapMatrix(MathTestConstants.STORAGE_SIZE, MathTestConstants.STORAGE_SIZE);
        fillMatrix(parent);
        testMatrix = new TransposedMatrixView(parent);
    }

    /** {@inheritDoc} */
    @Override public void externalizeTest() {
        externalizeTest(testMatrix);
    }

    /** */
    @Test
    public void testView() {
        assertEquals(UNEXPECTED_VALUE, parent.rowSize(), testMatrix.columnSize());
        assertEquals(UNEXPECTED_VALUE, parent.columnSize(), testMatrix.rowSize());

        for (int i = 0; i < parent.rowSize(); i++)
            for (int j = 0; j < parent.columnSize(); j++)
                assertEquals(UNEXPECTED_VALUE, parent.get(i, j), testMatrix.get(j, i), 0d);
    }

    /** */
    @Test
    public void testNullParams() {
        DenseLocalOnHeapMatrixConstructorTest.verifyAssertionError(() -> new TransposedMatrixView(null), "Null Matrix parameter");
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void testLike() {
        testMatrix.like(0, 0);
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void testLikeVector() {
        testMatrix.likeVector(0);
    }

    /** */
    private void fillMatrix(DenseLocalOnHeapMatrix mtx) {
        for (int i = 0; i < mtx.rowSize(); i++)
            for (int j = 0; j < mtx.columnSize(); j++)
                mtx.setX(i, j, i * mtx.rowSize() + j);
    }
}
