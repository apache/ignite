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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteCacheClearDuringRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        if(MvccFeatureChecker.forcedMvcc())
            fail("https://issues.apache.org/jira/browse/IGNITE-7952");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
            .setCacheMode(PARTITIONED));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClearAll() throws Exception {
        final IgniteEx node = startGrid(0);

        for (int i = 0; i < 5; i++) {
            populate(node);

            try {
                startGrid(1).cache(CACHE_NAME).clear();
            }
            finally {
                stopGrid(1);
            }
        }

        populate(node);

        startGrid(1).cache(CACHE_NAME).clear();

        startGrid(2);
    }

    /**
     * @param node Ignite node;
     * @throws Exception If failed.
     */
    private void populate(final Ignite node) throws Exception {
        final AtomicInteger id = new AtomicInteger();

        final int tCnt = Runtime.getRuntime().availableProcessors();

        final byte[] data = new byte[1024];

        ThreadLocalRandom.current().nextBytes(data);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try (IgniteDataStreamer<Object, Object> str = node.dataStreamer(CACHE_NAME)) {
                    int idx = id.getAndIncrement();

                    str.autoFlushFrequency(0);

                    for (int i = idx; i < 500_000; i += tCnt) {
                        str.addData(i, data);

                        if (i % (100 * tCnt) == idx)
                            str.flush();
                    }

                    str.flush();
                }
            }
        }, tCnt, "ldr");

        assertEquals(500_000, node.cache(CACHE_NAME).size());
    }
}
