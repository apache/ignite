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

package org.apache.ignite.internal.processors.cache.ttl;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.stream.IntStream;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for cache.size() with ttl enabled.
 */
public class CacheSizeTtlTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "TestCache";

    /** Entry expiry duration. */
    private static final Duration ENTRY_EXPIRY_DURATION = new Duration(SECONDS, 1);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests that cache.size() works correctly for massive amount of puts and ttl.
     */
    @Test
    public void testCacheSizeWorksCorrectlyWithTtl() throws IgniteInterruptedCheckedException {
        startIgniteServer();

        Ignite client = startIgniteClient();

        try (IgniteDataStreamer dataStreamer = client.dataStreamer(CACHE_NAME)) {
            IntStream.range(0, 100_000)
                .forEach(i -> dataStreamer.addData(1, LocalDateTime.now()));
        }

        assertTrue(GridTestUtils.waitForCondition(() -> client.cache(CACHE_NAME).size(CachePeekMode.PRIMARY) == 0,
            getTestTimeout()));
    }

    /**
     * Tests that cache.size() works correctly for massive amount of puts and ttl.
     */
    @Test
    public void testCachePutWorksCorrectlyWithTtl() throws Exception {
        startIgniteServer();

        Ignite client = startIgniteClient();

        multithreaded(() -> IntStream.range(0, 20_000)
            .forEach(i -> client.cache(CACHE_NAME).put(1, LocalDateTime.now())), 8);

        assertTrue(GridTestUtils.waitForCondition(() -> client.cache(CACHE_NAME).size(CachePeekMode.PRIMARY) == 0,
            getTestTimeout()));
    }

    private static Ignite startIgniteServer() {
        IgniteConfiguration configuration = new IgniteConfiguration()
            .setClientMode(false)
            .setIgniteInstanceName(UUID.randomUUID().toString())
            .setCacheConfiguration(cacheConfiguration())
            .setDiscoverySpi(discoveryConfiguration());
        return Ignition.start(configuration);
    }

    private static Ignite startIgniteClient() {
        IgniteConfiguration configuration = new IgniteConfiguration()
            .setClientMode(true)
            .setIgniteInstanceName(UUID.randomUUID().toString())
            .setDiscoverySpi(discoveryConfiguration());
        return Ignition.start(configuration);
    }

    @NotNull
    private static CacheConfiguration<String, LocalDateTime> cacheConfiguration() {
        return new CacheConfiguration<String, LocalDateTime>()
            .setName(CACHE_NAME)
            .setCacheMode(REPLICATED)
            .setEagerTtl(true)
            .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(ENTRY_EXPIRY_DURATION));
    }

    private static TcpDiscoverySpi discoveryConfiguration() {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(singleton("127.0.0.1:48550..48551"));
        TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        tcpDiscoverySpi.setIpFinder(ipFinder);
        tcpDiscoverySpi.setLocalPort(48550);
        tcpDiscoverySpi.setLocalPortRange(1);

        return tcpDiscoverySpi;
    }
}
