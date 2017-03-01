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

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.ExternalizeTest;
import org.junit.Test;

import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Matrix} implementations.
 */
public class MatrixImplementationsTest extends ExternalizeTest<Matrix> {
    /** */
    private void consumeSampleVectors(BiConsumer<Integer, Integer> paramsConsumer, BiConsumer<Matrix, String> consumer) {
        new MatrixImplementationFixtures().consumeSampleMatrix(paramsConsumer, consumer);
    }

    /** */
    @Override
    public void externalizeTest() {
        consumeSampleVectors(null, (m, desc) -> {
            externalizeTest(m);
        });
    }

    /** */
    @Test
    public void likeTest(){
        consumeSampleVectors(null, (m, desc) -> {
            Matrix like = m.like(m.rowSize(), m.columnSize());

            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected class: " + like.getClass().toString(),
                    like.getClass(),
                    m.getClass());
            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected rows.", like.rowSize(), m.rowSize());
            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected columns.", like.columnSize(), m.columnSize());
            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected storage class: " + like.getStorage().getClass().toString(),
                    like.getStorage().getClass(),
                    m.getStorage().getClass());
        });
    }

    /** */
    @Test
    public void copyTest(){
        consumeSampleVectors(null, (m, desc) -> {
            Matrix copy = m.copy();
            assertTrue("Incorrect copy for empty matrix " + desc, copy.equals(m));

            fillMatrix(m);
            copy = m.copy();
            assertTrue("Incorrect copy for matrix " + desc, copy.equals(m));
        });
    }

    /** */
    private void fillMatrix(Matrix m){
        for (int i = 0; i < m.rowSize(); i++) {
            for (int j = 0; j < m.columnSize(); j++) {
                m.set(i, j, Math.random());
            }
        }
    }
}
