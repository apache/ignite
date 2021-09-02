/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.metric;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.Person;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Base class for testing index pages metrics.
 */
@RunWith(Parameterized.class)
public abstract class AbstractIndexPageMetricsTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameters(name = "numCaches = {0}")
    public static Object[] data() {
        return new Object[] { 1, 3 };
    }

    /** */
    private IgniteEx grid;

    /** */
    private List<IgniteCache<Integer, Person>> caches;

    /** */
    IndexPageCounter indexPageCounter;

    /**
     * Number of caches that will be created on the test Ignite node.
     */
    @Parameterized.Parameter
    public int numCaches;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid = startGrid();
        grid.cluster().state(ClusterState.ACTIVE);

        caches = IntStream.range(0, numCaches)
            .mapToObj(i -> "cache " + i)
            .map(this::createCache)
            .collect(Collectors.toList());

        indexPageCounter = new IndexPageCounter(grid, isPersistenceEnabled());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid.close();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(isPersistenceEnabled())
                    )
            );
    }

    /**
     * Overriding classes should return {@code true} if the test Ignite node should be launched with enabled
     * persistence.
     */
    abstract boolean isPersistenceEnabled();

    /**
     * Validates that index page metrics correctly represent the number of in-memory index pages.
     * <p>
     * Implementation should use the {@link #indexPageCounter} to obtain the actual number of index pages.
     */
    abstract void validateIdxPagesCnt() throws IgniteCheckedException;

    /**
     * Inserts some data into the test caches and then removes all data.
     */
    @Test
    public void testStoreAndDeleteEntries() throws Exception {
        validateIdxPagesCnt();

        fillData(1);

        validateIdxPagesCnt();

        fillData(100);

        validateIdxPagesCnt();

        clearData();

        validateIdxPagesCnt();
    }

    /**
     * Populates the test caches and then destroys a cache.
     */
    @Test
    public void testStoreAndDeleteCacheGrp() throws Exception {
        fillData(100);

        validateIdxPagesCnt();

        caches.get(0).destroy();

        validateIdxPagesCnt();
    }

    /**
     * Populates the test caches and then removes <i>some</i> data from them.
     * <p>
     * This test case verifies that index metrics are still correct even if some index pages get merged.
     */
    @Test
    public void testStoreAndRemoveSomeEntries() throws Exception {
        fillData(100);

        validateIdxPagesCnt();

        IntStream.range(0, 50)
            .map(i -> (int) (Math.random() * 100))
            .forEach(caches.get(0)::remove);

        validateIdxPagesCnt();
    }

    /**
     * Returns the {@link GridCacheProcessor} of the test Ignite node.
     */
    GridCacheProcessor gridCacheProcessor() {
        return grid.context().cache();
    }

    /**
     * Returns the default Data Region of the test Ignite node.
     */
    DataRegion defaultDataRegion() throws IgniteCheckedException {
        return Objects.requireNonNull(
            gridCacheProcessor().context().database().dataRegion(null)
        );
    }

    /**
     * Creates a test cache with the given name.
     */
    private IgniteCache<Integer, Person> createCache(String cacheName) {
        CacheConfiguration<Integer, Person> cacheConfiguration = new CacheConfiguration<Integer, Person>(cacheName)
            .setIndexedTypes(Integer.class, Person.class);
        return grid.createCache(cacheConfiguration);
    }

    /**
     * Inserts {@code numEntries} objects into every test cache.
     */
    private void fillData(int numEntries) {
        IntStream.range(0, numEntries)
            .mapToObj(i -> new Person(i, "foobar"))
            .forEach(person -> caches.forEach(cache -> cache.put(person.getId(), person)));
    }

    /**
     * Removes all data from all test caches.
     */
    private void clearData() {
        caches.forEach(IgniteCache::clear);
    }
}
