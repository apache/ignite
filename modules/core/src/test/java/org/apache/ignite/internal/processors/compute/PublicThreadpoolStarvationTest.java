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

package org.apache.ignite.internal.processors.compute;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test to validate https://issues.apache.org/jira/browse/IGNITE-4239
 * Jobs hang when a lot of jobs calculate cache.
 */
public class PublicThreadpoolStarvationTest extends GridCacheAbstractSelfTest {
    /** Cache size. */
    private static final int CACHE_SIZE = 10;

    /** Cache size. */
    private static final String CACHE_NAME = "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPublicThreadPoolSize(1);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected Class<?>[] indexedTypes() {
        return new Class<?>[] {
            Integer.class, String.class,
        };
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        info("Fill caches begin...");

        fillCaches();

        info("Caches are filled.");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        grid(0).destroyCache(CACHE_NAME);

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    private void fillCaches() throws Exception {
        grid(0).createCache(CACHE_NAME);

        try (
            IgniteDataStreamer<Integer, String> streamer =
                grid(0).dataStreamer(CACHE_NAME)) {

            for (int i = 0; i < CACHE_SIZE; ++i)
                streamer.addData(i, "Data " + i);
        }

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheSizeOnPublicThreadpoolStarvation() throws Exception {
        grid(0).compute().run(new IgniteRunnable() {
            @Override public void run() {
                grid(0).cache(CACHE_NAME).size();
            }
        });
    }
}
