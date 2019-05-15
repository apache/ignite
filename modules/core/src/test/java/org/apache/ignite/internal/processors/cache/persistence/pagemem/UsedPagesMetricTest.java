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
import org.junit.Test;

/**
 * TotalUsedPages metric in-memory tests.
 */
public class UsedPagesMetricTest extends UsedPagesMetricAbstractTest {
    /** */
    public static final int NODES = 2;

    /** */
    public static final int ITERATIONS = 3;

    /** */
    public static final int STORED_ENTRIES_COUNT = 50000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setInitialSize(100 * 1024L * 1024L)
                        .setMaxSize(500 * 1024L * 1024L)
                        .setMetricsEnabled(true)
                ));
    }

    /**
     * Tests that totalUsedPages metric for in-memory data region behaves correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFillAndRemoveInMemory() throws Exception {
        testFillAndRemove(NODES, ITERATIONS, STORED_ENTRIES_COUNT, 256);
    }
}
