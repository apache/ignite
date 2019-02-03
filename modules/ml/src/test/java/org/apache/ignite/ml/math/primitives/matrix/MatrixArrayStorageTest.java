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

import org.apache.ignite.ml.math.primitives.MathTestConstants;
import org.apache.ignite.ml.math.primitives.matrix.storage.DenseMatrixStorage;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DenseMatrixStorage}.
 */
public class MatrixArrayStorageTest extends MatrixBaseStorageTest<DenseMatrixStorage> {
    /** {@inheritDoc} */
    @Override public void setUp() {
        storage = new DenseMatrixStorage(MathTestConstants.STORAGE_SIZE, MathTestConstants.STORAGE_SIZE);
    }

    /** */
    @Test
    public void isSequentialAccess() throws Exception {
        assertFalse(MathTestConstants.UNEXPECTED_VAL, storage.isSequentialAccess());
    }

    /** */
    @Test
    public void isDense() throws Exception {
        assertTrue(MathTestConstants.UNEXPECTED_VAL, storage.isDense());
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertTrue(MathTestConstants.UNEXPECTED_VAL, storage.isArrayBased());
    }

    /** */
    @Test
    public void data() throws Exception {
        double[] data = storage.data();
        assertNotNull(MathTestConstants.NULL_VAL, data);
        assertTrue(MathTestConstants.UNEXPECTED_VAL, data.length == MathTestConstants.STORAGE_SIZE *
            MathTestConstants.STORAGE_SIZE);
    }

}
