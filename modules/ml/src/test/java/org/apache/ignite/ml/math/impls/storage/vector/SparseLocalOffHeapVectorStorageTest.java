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

import org.apache.ignite.ml.math.ExternalizeTest;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.ml.math.impls.MathTestConstants.STORAGE_SIZE;
import static org.apache.ignite.ml.math.impls.MathTestConstants.UNEXPECTED_VAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SparseLocalOffHeapVectorStorage}.
 */
public class SparseLocalOffHeapVectorStorageTest extends ExternalizeTest<SparseLocalOffHeapVectorStorage> {
    /** */
    private SparseLocalOffHeapVectorStorage testVectorStorage;

    /** */
    @Before
    public void setup() {
        testVectorStorage = new SparseLocalOffHeapVectorStorage(STORAGE_SIZE);
    }

    /** */
    @After
    public void teardown() {
        testVectorStorage.destroy();
        testVectorStorage = null;
    }

    /** */
    @Test
    public void testBasic() {
        for (int i = 0; i < STORAGE_SIZE; i++) {
            double testVal = Math.random();
            testVectorStorage.set(i, testVal);
            assertEquals(UNEXPECTED_VAL, testVal, testVectorStorage.get(i), 0d);
        }
    }

    /** {@inheritDoc} */
    @Test(expected = UnsupportedOperationException.class)
    @Override public void externalizeTest() {
        super.externalizeTest(new SparseLocalOffHeapVectorStorage(STORAGE_SIZE));
    }

    /** */
    @Test
    public void testAttributes() {
        SparseLocalOffHeapVectorStorage testVectorStorage = new SparseLocalOffHeapVectorStorage(STORAGE_SIZE);

        assertTrue(UNEXPECTED_VAL, testVectorStorage.isRandomAccess());
        assertFalse(UNEXPECTED_VAL, testVectorStorage.isSequentialAccess());
        assertFalse(UNEXPECTED_VAL, testVectorStorage.isDense());
        assertFalse(UNEXPECTED_VAL, testVectorStorage.isArrayBased());
        assertFalse(UNEXPECTED_VAL, testVectorStorage.isDistributed());
    }
}
