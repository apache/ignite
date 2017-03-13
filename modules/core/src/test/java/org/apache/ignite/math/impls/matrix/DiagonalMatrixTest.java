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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.BiConsumer;
import org.apache.ignite.math.ExternalizeTest;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.StorageConstants;
import org.apache.ignite.math.impls.MathTestConstants;
import org.junit.AfterClass;
import org.junit.Before;

/**
 * Tests for {@link DiagonalMatrix}.
 */
public class DiagonalMatrixTest extends ExternalizeTest<DiagonalMatrix>{
    private DiagonalMatrix testMatrix;

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
    private void fillMatrix(Matrix m){
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, Math.random());
    }

}
