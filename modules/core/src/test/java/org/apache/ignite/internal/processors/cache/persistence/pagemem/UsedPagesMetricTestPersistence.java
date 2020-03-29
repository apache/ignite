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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * TotalUsedPages metric persistence tests.
 */
public class UsedPagesMetricTestPersistence extends UsedPagesMetricAbstractTest {
    /** */
    public static final int NODES = 1;

    /** */
    public static final int ITERATIONS = 1;

    /** */
    public static final int STORED_ENTRIES_COUNT = 50000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(100 * 1024L * 1024L)
                        .setMaxSize(500 * 1024L * 1024L)
                        .setMetricsEnabled(true)
                ));
    }

    /**
     *
     */
    @Before
    public void cleanBeforeStart() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void stopAndClean() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that totalUsedPages metric for data region with enabled persistence
     * and pages being rotated to disk behaves correctly.
     *
     * @throws Exception if failed
     */
    @Test
    public void testFillAndRemovePagesRotation() throws Exception {
        testFillAndRemove(NODES, ITERATIONS, STORED_ENTRIES_COUNT, 8192);
    }

    /**
     * Tests that totalUsedPages metric for data region with enabled persistence
     * and pages that are not being rotated to disk behaves correctly.
     *
     * @throws Exception if failed
     */
    @Test
    public void testFillAndRemoveWithoutPagesRotation() throws Exception {
        testFillAndRemove(NODES, ITERATIONS, STORED_ENTRIES_COUNT, 256);
    }
}
