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

package org.apache.ignite.ml.math.impls.storage.vector;

import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.impls.MathTestConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link SparseLocalOnHeapVectorStorage}.
 */
public class RandomAccessSparseVectorStorageTest extends VectorBaseStorageTest<SparseLocalOnHeapVectorStorage> {
    /** */
    @Override public void setUp() {
        storage = new SparseLocalOnHeapVectorStorage(MathTestConstants.STORAGE_SIZE, StorageConstants.RANDOM_ACCESS_MODE);
    }

    /** */
    @Test
    public void data() throws Exception {
        assertNull(MathTestConstants.NULL_VAL, storage.data());
    }

    /** */
    @Test
    public void isSequentialAccess() throws Exception {
        assertFalse(MathTestConstants.UNEXPECTED_VAL, storage.isSequentialAccess());
    }

    /** */
    @Test
    public void isDense() throws Exception {
        assertFalse(MathTestConstants.UNEXPECTED_VAL, storage.isDense());
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertFalse(MathTestConstants.UNEXPECTED_VAL, storage.isArrayBased());
    }

}
