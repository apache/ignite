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

package org.apache.ignite.internal;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture.PARTITION_STATE_FAILED_MSG;

/**
 * Checks diagnostic messages at PME.
 */
public class DiagnosticLogForPartitionStatesTest extends GridCommonAbstractTest {
    /** Cache 1. */
    private static final String CACHE_1 = "cache-1";

    /** Any message. */
    private static final String ANY_MSG = "";

    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(log)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(200L * 1024 * 1024)));
    }

    /**
     * @throws Exception if stopAllGrid fails.
     */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * Tests that partitions validation is triggered and the corresponding message is logged.
     *
     * @throws Exception If failed.
     */
    @Test
    public void shouldPrintMessageIfPartitionHasOtherCounter() throws Exception {
        doTest(new CacheConfiguration<Integer, Integer>(CACHE_1).setBackups(1), true);
    }

    /**
     * Tests that partitions validation is not triggered when custom expiry policy is explicitly used.
     *
     * @throws Exception If failed.
     */
    @Test
    public void shouldNotPrintMessageIfPartitionHasOtherCounterButHasCustomExpiryPolicy() throws Exception {
        doTest(
            new CacheConfiguration<Integer, Integer>(CACHE_1)
                .setBackups(1)
                .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 1))),
            false
        );
    }

    /**
     * Tests that partitions validation is triggered and the corresponding message is logged
     * when eternal policy is explicitly used.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShouldPrintMessageWhenEternalExpiryPolicyIsExplicitlyUsed() throws Exception {
        doTest(
            new CacheConfiguration<Integer, Integer>(CACHE_1)
                .setBackups(1)
                .setExpiryPolicyFactory(EternalExpiryPolicy.factoryOf()),
            true
        );
    }

    /**
     * Tests that partitions validation is not triggered when a cache in the cache group (at least one of them)
     * uses custom expiry policy.
     */
    @Test
    public void testShouldNotPrintMessageIfPartitionHasOtherCounterButHasCustomExpiryPolicyCacheGroup() throws Exception {
        String grpName = "test-group";

        CacheConfiguration<Integer, Integer> cfg1 = new CacheConfiguration<Integer, Integer>("cache-1")
            .setBackups(1)
            .setGroupName(grpName);

        CacheConfiguration<Integer, Integer> cfg2 = new CacheConfiguration<Integer, Integer>("cache-2")
            .setBackups(1)
            .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 1)))
            .setGroupName(grpName);

        doTest(Arrays.asList(cfg1, cfg2), false);
    }

    /**
     * @param cacheCfg Cache configuration.
     * @param msgExp {@code true} if warning message is expected.
     * @throws Exception If failed.
     */
    private void doTest(CacheConfiguration<Integer, Integer> cacheCfg, boolean msgExp) throws Exception {
        doTest(Collections.singletonList(cacheCfg), msgExp);
    }

    /**
     * @param cfgs Cache configurations.
     * @param msgExp {@code true} if warning message is expected.
     * @throws Exception If failed.
     */
    private void doTest(List<CacheConfiguration<Integer, Integer>> cfgs, boolean msgExp) throws Exception {
        String name = (cfgs.size() == 1) ? cfgs.get(0).getName() : cfgs.get(0).getGroupName();

        LogListener lsnr = LogListener
            .matches(s -> s.startsWith(String.format(PARTITION_STATE_FAILED_MSG, name, ANY_MSG)))
            .times(msgExp ? 1 : 0)
            .build();

        log.registerListener(lsnr);

        IgniteEx node1 = startGrid(0);

        IgniteEx node2 = startGrid(1);

        node1.cluster().active(true);

        List<IgniteCache<Integer, Integer>> caches = cfgs.stream()
            .map(cfg -> node1.getOrCreateCache(cfg)).collect(Collectors.toList());

        Map<String, Set<Integer>> clearKeys = new HashMap<>();

        for (IgniteCache<Integer, Integer> cache : caches) {
            String cacheName = cache.getName();

            Set<Integer> clr = new HashSet<>();

            for (int i = 0; i < 100; i++) {
                cache.put(i, i);

                if (node2.affinity(cacheName).isPrimary(node2.localNode(), i))
                    clr.add(i);
            }

            clearKeys.put(cacheName, clr);
        }

        for (IgniteCache<Integer, Integer> c : caches) {
            node2.context().cache().cache(c.getName())
                .clearLocallyAll(clearKeys.get(c.getName()), true, true, true);
        }

        // Trigger partition map exchange and therefore trigger partitions validation.
        startGrid(2);

        awaitPartitionMapExchange();

        assertTrue(lsnr.check());
    }
}
