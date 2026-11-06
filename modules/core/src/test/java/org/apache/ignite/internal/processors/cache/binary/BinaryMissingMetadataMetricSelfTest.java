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

package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.BINARY_ERRORS_METRICS;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.MISSING_METADATA_CNT;

/**
 * Tests that the {@code binary.errors.MissingMetadataCount} metric is incremented when binary metadata
 * for an object with compact footer is missing during reading (e.g. after the binary metadata directory
 * has been cleared between node restarts).
 */
public class BinaryMissingMetadataMetricSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "testCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void testMissingMetadataMetric() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        Object binObj = ignite.binary().builder("TestType").setField("f1", 1).setField("f2", "v2").build();

        ignite.cache(CACHE_NAME).put(1, binObj);

        stopGrid(0);

        // Delete the binary metadata directory to simulate metadata loss after a restart.
        U.delete(ignite.context().pdsFolderResolver().fileTree().binaryMeta());

        ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        LongMetric metric = ignite.context().metric().registry(BINARY_ERRORS_METRICS).findMetric(MISSING_METADATA_CNT);

        assertEquals(0, metric.value());

        IgniteEx finalIgnite = ignite;

        GridTestUtils.assertThrowsWithCause(() -> finalIgnite.cache(CACHE_NAME).get(1), Exception.class);

        assertEquals(1, metric.value());
    }
}
