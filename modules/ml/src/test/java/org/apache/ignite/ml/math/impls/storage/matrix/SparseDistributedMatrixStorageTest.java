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

package org.apache.ignite.ml.math.impls.storage.matrix;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.impls.MathTestConstants;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests for {@link SparseDistributedMatrixStorage}.
 */
@GridCommonTest(group = "Distributed Models")
public class SparseDistributedMatrixStorageTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;
    /** Cache name. */
    private static final String CACHE_NAME = "test-cache";
    /** */
    private static final String UNEXPECTED_ATTRIBUTE_VALUE = "Unexpected attribute value.";
    /** Grid instance. */
    private Ignite ignite;

    /**
     * Default constructor.
     */
    public SparseDistributedMatrixStorageTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);

        ignite.configuration().setPeerClassLoadingEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.destroyCache(CACHE_NAME);
    }

    /** */
    public void testCacheCreation() throws Exception {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        final int rows = MathTestConstants.STORAGE_SIZE;
        final int cols = MathTestConstants.STORAGE_SIZE;

        SparseDistributedMatrixStorage storage = new SparseDistributedMatrixStorage(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        assertNotNull("SparseDistributedMatrixStorage cache is null.", storage.cache());
    }

    /** */
    public void testSetGet() throws Exception {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        final int rows = MathTestConstants.STORAGE_SIZE;
        final int cols = MathTestConstants.STORAGE_SIZE;

        SparseDistributedMatrixStorage storage = new SparseDistributedMatrixStorage(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double v = Math.random();
                storage.set(i, j, v);

                assert Double.compare(v, storage.get(i, j)) == 0;
                assert Double.compare(v, storage.get(i, j)) == 0;
            }
        }
    }

    /** */
    public void testAttributes() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        final int rows = MathTestConstants.STORAGE_SIZE;
        final int cols = MathTestConstants.STORAGE_SIZE;

        SparseDistributedMatrixStorage storage = new SparseDistributedMatrixStorage(rows, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        assertEquals(UNEXPECTED_ATTRIBUTE_VALUE, storage.rowSize(), rows);
        assertEquals(UNEXPECTED_ATTRIBUTE_VALUE, storage.columnSize(), cols);

        assertFalse(UNEXPECTED_ATTRIBUTE_VALUE, storage.isArrayBased());
        assertFalse(UNEXPECTED_ATTRIBUTE_VALUE, storage.isDense());
        assertTrue(UNEXPECTED_ATTRIBUTE_VALUE, storage.isDistributed());

        assertEquals(UNEXPECTED_ATTRIBUTE_VALUE, storage.isRandomAccess(), !storage.isSequentialAccess());
        assertTrue(UNEXPECTED_ATTRIBUTE_VALUE, storage.isRandomAccess());

    }

}
