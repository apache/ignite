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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

/**
 * Tests rebalance of huge cache with large entries.
 * OOM should not occur on supplier during handling demand message.
 */
public class RebalanceIteratorLargeEntriesOOMTest extends GridCommonAbstractTest {
    /** */
    private static final String REPLICATED_CACHE_NAME = "repl-cache";

    /** */
    private static final int KB = 1 << 10;

    /** */
    private static final int GB = 1 << 30;

    /** */
    private static final long MAX_REGION_SIZE = GB;

    /** */
    private static final int PAYLOAD_SIZE = 200 * KB;

    /** */
    private static final int NUM_LOAD_THREADS = 4;

    /**
     * Get number of items per loader in order to fill 90% of memory region.
     */
    private static final long INTERVAL = MAX_REGION_SIZE * 9 / (10 * NUM_LOAD_THREADS * PAYLOAD_SIZE);

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Arrays.asList("-Xmx512m", "-Xms512m", "-XX:+HeapDumpOnOutOfMemoryError");
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return igniteInstanceName.startsWith("supplier");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setInitialSize(GB)
                .setMaxSize(GB)
            )
        );
        cfg.setCacheConfiguration(new CacheConfiguration<>(REPLICATED_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        IgniteProcessProxy.killAll();
    }

    /** */
    @Test
    public void testRebalance() throws Exception {
        startSupplier();
        IgniteEx client = startClientGrid("client");

        GridTestUtils.runMultiThreaded((Integer idx) -> {
            try (IgniteDataStreamer<Object, Object> streamer = client.dataStreamer(REPLICATED_CACHE_NAME)) {
                streamer.timeout(TimeUnit.MINUTES.toMillis(1));

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                byte[] buf = new byte[PAYLOAD_SIZE];

                for (long i = idx * INTERVAL; i < (idx + 1) * INTERVAL; i++) {
                    rnd.nextBytes(buf);

                    streamer.addData(i, buf);
                }

                streamer.flush();
            }
        }, NUM_LOAD_THREADS, "loader-");

        startDemander();
        awaitPartitionMapExchange(true, true, null);
    }

    /**
     * Start remote supplier.
     */
    private void startSupplier() throws Exception {
        startGrid(0);

        startGrid("supplier");

        stopGrid(0);
    }

    /** */
    private void startDemander() throws Exception {
        startGrid("demander");
    }
}
