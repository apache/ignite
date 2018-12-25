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

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteCacheCreateRestartSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** */
    private static final int NODES = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopOriginatingNode() throws Exception {
        startGrids(NODES);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 50; i++) {
            int node = rnd.nextInt(0, NODES);

            final Ignite ignite = ignite(node);

            info("Running iteration on the node [idx=" + node + ", nodeId=" + ignite.cluster().localNode().id() + ']');

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Thread.currentThread().setName("create-thread");

                    try {
                        ignite.createCache(new CacheConfiguration<>(CACHE_NAME));
                    }
                    catch (CacheException | IllegalStateException e) {
                        log.info("Expected error: " + e);
                    }

                    return null;
                }
            });

            ignite.close();

            fut.get();

            Ignite ignite0 = startGrid(node);

            ignite0.destroyCache(CACHE_NAME);
        }
    }
}
