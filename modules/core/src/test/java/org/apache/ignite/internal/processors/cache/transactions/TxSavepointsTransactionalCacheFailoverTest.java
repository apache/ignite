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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnlockRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** */
public class TxSavepointsTransactionalCacheFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    private int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(cacheConfiguration());

        BanningCommunicationSpi commSpi = new BanningCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>();

        cfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheMode(PARTITIONED);

        cfg.setName(DEFAULT_CACHE_NAME);

        cfg.setBackups(1);

        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(gridCount());

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureInRollbackToSavepoint() throws Exception {
        checkSavepoint(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupNodeFailureInRollbackToSavepoint() throws Exception {
        checkSavepoint(false);
    }

    /**
     * Check the situation, when primary or backup will not response on requests during rollback to savepoint.
     *
     * @param failOnPrimary {@code True} if timeout should happen because of primary node,
     * {@code false} - because of backup.
     * @throws Exception If failed.
     */
    private void checkSavepoint(boolean failOnPrimary) throws Exception {
        Ignite ignite = ignite(0);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int key1 = generateKey(ignite, 0);
        int key2 = generateKey(ignite, key1 + 1);

        if (failOnPrimary)
            communication(0).bannedClasses(Collections.singletonList(GridRollbackToSavepointRequest.class));
        else
            communication(1).bannedClasses(Collections.singletonList(GridDhtUnlockRequest.class));

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(1_000);

            cache.put(key1, key1);

            tx.savepoint("sp");

            cache.put(key2, key2);

            tx.rollbackToSavepoint("sp");

            if (failOnPrimary)
                fail("Transaction proceed to commit: " + tx);

            tx.commit();
        }
        catch (IgniteException e) {
            if (failOnPrimary)
                assertTrue(e.getMessage().contains("Failed unlock for keys"));
            else {
                error(e.getMessage(), e);

                fail("Unexpected exception: " + e);
            }
        }

        stopGrid(failOnPrimary ? 1 : 2);

        ((IgniteKernal)ignite(0)).context().discovery().topologyFuture(gridCount() + 1).get(1_000);

        awaitPartitionMapExchange();

        // Check there are no hanging transactions.
        G.allGrids().forEach((node) -> {
            ((IgniteEx)node).context().cache().context().tm().idMapSize();

            assertTrue(((IgniteEx)node).context().cache().context().mvcc().lockedKeys().isEmpty());
        });

        for (ClusterNode node : grid(0).affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key1))
            assertEquals(failOnPrimary ? null : key1, grid(node).cache(DEFAULT_CACHE_NAME).localPeek(key1));

        // Check that keys are free and not locked.
        cache.putAsync(key1, key1 + 1).get(1_000);

        cache.putAsync(key2, key2 + 1).get(1_000);

        cache.removeAsync(key1).get(1_000);

        cache.removeAsync(key2).get(1_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeStopInRollbackToSavepoint() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        int key1 = generateKey(grid(0), 0);
        int key2 = generateKey(grid(0), key1 + 1);

        GridTestUtils.assertThrowsWithCause((Callable<Void>)() -> {
                try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(key1, key1);

                    tx.savepoint("sp");

                    cache.put(key2, key2);

                    G.stop(grid(1).name(), false);

                    tx.rollbackToSavepoint("sp");
                }

                return null;
            },
            ClusterTopologyCheckedException.class
        );
    }

    /**
     * @param ignite Ignite instance to generate key.
     * @param beginingIdx Index to start generating.
     * @return Generated key that is primary for {@code ignite(1)} and backup for {@code ignite(2)}.
     */
    private int generateKey(Ignite ignite, int beginingIdx) {
        Affinity<Object> aff = ignite.affinity(DEFAULT_CACHE_NAME);

        for (int key = beginingIdx;;key++) {
            if (aff.isPrimary(ignite(1).cluster().localNode(), key)
                && aff.isBackup(ignite(2).cluster().localNode(), key))
                return key;
        }
    }

    /**
     * @param idx Index.
     * @return Communication SPI.
     */
    private BanningCommunicationSpi communication(int idx) {
        return (BanningCommunicationSpi)ignite(idx).configuration().getCommunicationSpi();
    }
}
