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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCachePartitionsStateValidationTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
        ));

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        return cfg;
    }

    /** {@inheritDoc */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Test that partitions state validation works correctly.
     *
     * @throws Exception If failed.
     */
    public void testValidationIfPartitionCountersAreInconsistent() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrids(2);
        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        // Modify update counter for some partition.
        for (GridDhtLocalPartition partition : ignite.cachex(CACHE_NAME).context().topology().localPartitions()) {
            partition.updateCounter(100500L);
            break;
        }

        // Trigger exchange.
        startGrid(2);

        awaitPartitionMapExchange();

        // Nothing should happen (just log error message) and we're still able to put data to corrupted cache.
        ignite.cache(CACHE_NAME).put(0, 0);

        stopAllGrids();
    }
}
