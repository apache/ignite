/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.datastreamer;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for DataStreamer.
 */
public class DataStreamerMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean dynamicCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        if (!dynamicCache)
            cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStopIgnites() throws Exception {
        startStopIgnites();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-1602")
    @Test
    public void testStartStopIgnitesDynamicCache() throws Exception {
        dynamicCache = true;

        startStopIgnites();
    }

    /**
     * @throws Exception If failed.
     */
    private void startStopIgnites() throws Exception {
        for (int attempt = 0; attempt < 3; ++attempt) {
            log.info("Iteration: " + attempt);

            final Ignite ignite = startGrid(0);

            Set<IgniteFuture> futs = new HashSet<>();

            final AtomicInteger igniteId = new AtomicInteger(1);

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 1; i < 5; ++i)
                        startGrid(igniteId.incrementAndGet());

                    return true;
                }
            }, 2, "start-node-thread");

            if (dynamicCache)
                ignite.getOrCreateCache(cacheConfiguration());

            try (final DataStreamerImpl dataLdr = (DataStreamerImpl)ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
                Random rnd = new Random();

                long endTime = U.currentTimeMillis() + 15_000;

                while (!fut.isDone() && U.currentTimeMillis() < endTime)
                    futs.add(dataLdr.addData(rnd.nextInt(100_000), String.valueOf(rnd.nextInt(100_000))));
            }

            for (IgniteFuture f : futs)
                f.get();

            fut.get();

            stopAllGrids();
        }
    }
}
