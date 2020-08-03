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

package org.apache.ignite.p2p;

import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * P2P test.
 */
@GridCommonTest(group = "P2P")
public class GridP2PComputeWithNestedEntryProcessorTest extends GridCommonAbstractTest {
    /** URL of classes. */
    private static final ClassLoader TEST_CLASS_LOADER;

    /** Deleted filter name. */
    private static final String FIRST_FILTER_NAME = "org.apache.ignite.tests.p2p.pedicates.FirstConsideredPredicate";

    /** Deleted filter name. */
    private static final String SECOND_FILTER_NAME = "org.apache.ignite.tests.p2p.pedicates.SecondConsideredPredicate";

    /** Composite filter name. */
    private static final String COMPOSITE_FILTER_NAME = "org.apache.ignite.tests.p2p.pedicates.CompositePredicate";

    /** Cache entry processor. */
    private static final String COMPUTE_WITH_NESTED_PROCESSOR = "org.apache.ignite.tests.p2p.CacheDeploymenComputeWithNestedEntryProcessor";

    /** Entries. */
    public static final int ENTRIES = 100;

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private DeploymentMode depMode;

    /** */
    private static final TcpDiscoveryIpFinder FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Initialize ClassLoader. */
    static {
        try {
            TEST_CLASS_LOADER = new URLClassLoader(
                new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))},
                GridP2PComputeWithNestedEntryProcessorTest.class.getClassLoader());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME))
            .setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        processTest();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        processTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void processTest() throws Exception {
        try {
            Ignite ignite = startGrids(2);

            createAndLoadCache(ignite);

            awaitPartitionMapExchange();

            for (int i = 0; i < 10; i++) {
                try (Ignite client = startClientGrid("client")) {

                    IgniteCache cache = client.cache(DEFAULT_CACHE_NAME).withKeepBinary();

                    Integer key = primaryKey(ignite(1).cache(DEFAULT_CACHE_NAME));

                    for (Boolean res : runJob(client, 10_000, DEFAULT_CACHE_NAME, key)) {
                        assertTrue(key >= ENTRIES || res);
                    }

                    scanCacheData(cache);
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Ignite.
     */
    @NotNull private void createAndLoadCache(Ignite ignite) {
        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME).withKeepBinary();

        for (int i = 0; i < ENTRIES; i++) {
            if (i % 2 == 0) {
                cache.put(i, ignite.binary()
                    .builder("boType")
                    .build());
            }
            else
                cache.put(i, ignite.binary()
                    .builder("boType")
                    .setField("isDeleted", true)
                    .build());
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param timeout Timeout.
     * @param param Parameter.
     * @throws Exception If failed.
     */
    private Collection<Boolean> runJob(Ignite ignite, long timeout,
        String cacheName, int param) throws Exception {
        Constructor ctor = TEST_CLASS_LOADER.loadClass(COMPUTE_WITH_NESTED_PROCESSOR)
            .getConstructor(String.class, int.class);

        return ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<Boolean>)ctor.newInstance(cacheName, param));
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void scanCacheData(IgniteCache cache) throws Exception {
        scanByCopositeFirstPredicate(cache);
        scanByCopositeSecondPredicate(cache);
        scanByCopositeFirstSecondPredicate(cache);
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void scanByCopositeFirstPredicate(IgniteCache cache) throws Exception {
        IgniteBiPredicate firstFilter = (IgniteBiPredicate)TEST_CLASS_LOADER.loadClass(FIRST_FILTER_NAME)
            .getConstructor().newInstance();
        IgniteBiPredicate compositeFilter = (IgniteBiPredicate)TEST_CLASS_LOADER.loadClass(COMPOSITE_FILTER_NAME)
            .getConstructor().newInstance();

        U.invoke(TEST_CLASS_LOADER.loadClass(COMPOSITE_FILTER_NAME), compositeFilter, "addPredicate", firstFilter);

        List list = cache.query(new ScanQuery().setFilter(compositeFilter)).getAll();

        assertEquals(list.size(), ENTRIES / 2);
    }

    /**
     * @param cache Ignite chache.
     * @throws Exception If failed.
     */
    private void scanByCopositeSecondPredicate(IgniteCache cache) throws Exception {
        IgniteBiPredicate secondFilter = (IgniteBiPredicate)TEST_CLASS_LOADER.loadClass(SECOND_FILTER_NAME)
            .getConstructor().newInstance();
        IgniteBiPredicate compositeFilter = (IgniteBiPredicate)TEST_CLASS_LOADER.loadClass(COMPOSITE_FILTER_NAME)
            .getConstructor().newInstance();

        U.invoke(TEST_CLASS_LOADER.loadClass(COMPOSITE_FILTER_NAME), compositeFilter, "addPredicate", secondFilter);

        List list = cache.query(new ScanQuery().setFilter(compositeFilter)).getAll();

        assertEquals(list.size(), ENTRIES / 2);
    }

    /**
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void scanByCopositeFirstSecondPredicate(IgniteCache cache) throws Exception {
        IgniteBiPredicate firstFilter = (IgniteBiPredicate)TEST_CLASS_LOADER.loadClass(FIRST_FILTER_NAME)
            .getConstructor().newInstance();
        IgniteBiPredicate secondFilter = (IgniteBiPredicate)TEST_CLASS_LOADER.loadClass(SECOND_FILTER_NAME)
            .getConstructor().newInstance();
        IgniteBiPredicate compositeFilter = (IgniteBiPredicate)TEST_CLASS_LOADER.loadClass(COMPOSITE_FILTER_NAME)
            .getConstructor().newInstance();

        U.invoke(TEST_CLASS_LOADER.loadClass(COMPOSITE_FILTER_NAME), compositeFilter, "addPredicate", firstFilter);
        U.invoke(TEST_CLASS_LOADER.loadClass(COMPOSITE_FILTER_NAME), compositeFilter, "addPredicate", secondFilter);

        List list = cache.query(new ScanQuery().setPageSize(10).setFilter(compositeFilter)).getAll();

        assertEquals(list.size(), 0);
    }
}
