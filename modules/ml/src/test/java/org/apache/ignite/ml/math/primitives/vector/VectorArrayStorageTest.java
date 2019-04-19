/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.math.primitives.vector;

import java.util.Arrays;
import org.apache.ignite.ml.math.primitives.MathTestConstants;
import org.apache.ignite.ml.math.primitives.vector.storage.DenseVectorStorage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link DenseVectorStorage}.
 */
public class VectorArrayStorageTest extends VectorBaseStorageTest<DenseVectorStorage> {
    /** */
    @Override public void setUp() {
        storage = new DenseVectorStorage(MathTestConstants.STORAGE_SIZE);
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertTrue(MathTestConstants.WRONG_ATTRIBUTE_VAL, storage.isArrayBased());

        assertTrue(MathTestConstants.WRONG_ATTRIBUTE_VAL, new DenseVectorStorage().isArrayBased());
    }

    /** */
    @Test
    public void data() throws Exception {
        assertNotNull(MathTestConstants.NULL_DATA_STORAGE, storage.data());

        assertEquals(MathTestConstants.WRONG_DATA_SIZE, storage.data().length, MathTestConstants.STORAGE_SIZE);

        assertTrue(MathTestConstants.UNEXPECTED_DATA_VAL, Arrays.equals(storage.data(), new double[MathTestConstants.STORAGE_SIZE]));

        assertNull(MathTestConstants.UNEXPECTED_DATA_VAL, new DenseVectorStorage().data());
    }

}
