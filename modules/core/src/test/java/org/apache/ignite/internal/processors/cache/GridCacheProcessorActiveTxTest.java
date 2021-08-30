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

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * Tests that verify the execution of GridCacheProcessor operations
 * in active transactions.
 */
public class GridCacheProcessorActiveTxTest extends GridCommonAbstractTest {
    /**
     * Format for displaying the cache name(s) and operation.
     *
     * @see GridCacheProcessor#CHECK_EMPTY_TRANSACTIONS_ERROR_MSG_FORMAT
     */
    private static final String CHECK_EMPTY_TRANSACTIONS_ERROR_MSG =
        getFieldValue(GridCacheProcessor.class, "CHECK_EMPTY_TRANSACTIONS_ERROR_MSG_FORMAT");

    /** Node. */
    private static IgniteEx NODE;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        NODE = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        NODE.destroyCaches(NODE.cacheNames());

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Checking the throw exception during the operation "dynamicStartCache".
     */
    @Test
    public void testDynamicSingleCacheStart() {
        String cacheName = DEFAULT_CACHE_NAME;

        opInActiveTx(
            () -> NODE.createCache(new CacheConfiguration<>(cacheName)),
            cacheName,
            "dynamicStartCache"
        );
    }

    /**
     * Checking the throw exception during the operation
     * "dynamicStartCachesByStoredConf".
     */
    @Test
    public void testDynamicStartMultipleCaches() {
        List<String> cacheNames = cacheNames();

        List<CacheConfiguration> cacheCfgs = cacheConfigurations(cacheNames);

        opInActiveTx(
            () -> NODE.createCaches(cacheCfgs),
            cacheNames.toString(),
            "dynamicStartCachesByStoredConf"
        );
    }

    /**
     * Checking the throw exception during the operation "dynamicDestroyCache".
     */
    @Test
    public void testDynamicCacheDestroy() {
        String cacheName = DEFAULT_CACHE_NAME;

        NODE.createCache(new CacheConfiguration<>(cacheName));

        opInActiveTx(
            () -> NODE.destroyCache(cacheName),
            cacheName,
            "dynamicDestroyCache"
        );
    }

    /**
     * Checking the throw exception during the operation
     * "dynamicDestroyCaches".
     */
    @Test
    public void testDynamicDestroyMultipleCaches() {
        List<String> cacheNames = cacheNames();

        List<CacheConfiguration> cacheCfgs = cacheConfigurations(cacheNames);

        NODE.createCaches(cacheCfgs);

        opInActiveTx(
            () -> NODE.destroyCaches(cacheNames),
            cacheNames.toString(),
            "dynamicDestroyCaches"
        );
    }

    /**
     * Checking the throw exception during the operation "dynamicCloseCache".
     */
    @Test
    public void testDynamicCacheClose() {
        GridCacheProcessor cacheProcessor = NODE.context().cache();

        String cacheName = DEFAULT_CACHE_NAME;

        NODE.getOrCreateCache(new CacheConfiguration<>(cacheName));

        opInActiveTx(
            () -> cacheProcessor.dynamicCloseCache(cacheName),
            cacheName,
            "dynamicCloseCache"
        );

        NODE.destroyCache(cacheName);
    }

    /**
     * Checking the throw exception during the operation "resetCacheState".
     */
    @Test
    public void testResetCacheState() {
        List<String> cacheNames = cacheNames();

        opInActiveTx(
            () -> NODE.context().cache().resetCacheState(cacheNames),
            cacheNames.toString(),
            "resetCacheState"
        );
    }

    /**
     * Create cache names.
     *
     * @return Cache names.
     */
    private List<String> cacheNames() {
        return range(0, 2).mapToObj(i -> DEFAULT_CACHE_NAME + i).collect(toList());
    }

    /**
     * Creating cache configurations in which only cache name is set.
     *
     * @param cacheNames The names of caches.
     * @return Cache configurations.
     */
    private List<CacheConfiguration> cacheConfigurations(List<String> cacheNames) {
        assert nonNull(cacheNames);

        return cacheNames.stream().map(CacheConfiguration::new).collect(toList());
    }

    /**
     * Performing an operation in an active transaction with a check that an
     * exception will be thrown with a format
     * {@link #CHECK_EMPTY_TRANSACTIONS_ERROR_MSG}.
     *
     * @param runnableX Operation in an active transaction.
     * @param cacheName Cache name for the exception message.
     * @param operation Operation for the exception message.
     */
    private void opInActiveTx(RunnableX runnableX, String cacheName, String operation) {
        assert nonNull(runnableX);
        assert nonNull(cacheName);
        assert nonNull(operation);

        try (Transaction tx = NODE.transactions().txStart()) {
            assertThrows(
                log,
                () -> {
                    runnableX.run();
                    return null;
                },
                IgniteException.class,
                format(CHECK_EMPTY_TRANSACTIONS_ERROR_MSG, cacheName, operation)
            );
        }

        runnableX.run();
    }
}
