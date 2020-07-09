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

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;

/**
 * Tests an ability to rollback transactions on invalid mapping during topology change.
 */
public class TxRollbackOnMapOnInvalidTopologyTest extends GridCommonAbstractTest {
    /** */
    private static final int GRIDS = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(2);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(GRIDS);

        startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests rollbacks when mapped to invalid topology.
     */
    @Test
    public void testRollbackOnMapToInvalidTopology_1() throws Exception {
        doTestRollback(grid("client"), grid(0));
    }

    /**
     * Tests rollbacks when mapped to invalid topology.
     */
    @Test
    public void testRollbackOnMapToInvalidTopology_2() throws Exception {
        doTestRollback(grid("client"), grid(1));
    }

    /**
     * Tests rollbacks when mapped to invalid topology.
     */
    @Test
    public void testRollbackOnMapToInvalidTopology_3() throws Exception {
        doTestRollback(grid("client"), grid(2));
    }

    /**
     * Tests rollbacks when mapped to invalid topology.
     */
    @Test
    public void testRollbackOnMapToInvalidTopology_4() throws Exception {
        doTestRollback(grid(0), grid(0));
    }

    /**
     * Tests rollbacks when mapped to invalid topology.
     */
    @Test
    public void testRollbackOnMapToInvalidTopology_5() throws Exception {
        doTestRollback(grid(0), grid(1));
    }

    /**
     * Tests rollbacks when mapped to invalid topology.
     */
    @Test
    public void testRollbackOnMapToInvalidTopology_6() throws Exception {
        doTestRollback(grid(0), grid(2));
    }

    /**
     * Test scenario: mock partition to fail check, start new node.
     * Expected result: Transaction is rolled back.
     *
     * @param near Near mode.
     * @param node Owner.
     */
    private void doTestRollback(Ignite near, IgniteEx node) throws Exception {
        List<Integer> primKeys = primaryKeys(node.cache(DEFAULT_CACHE_NAME), 100);
        List<Integer> movingKeys = movingKeysAfterJoin(node, DEFAULT_CACHE_NAME, 100);

        primKeys.removeAll(movingKeys); /** {@code primKeys} contains stable partitions. */

        int part = primKeys.get(0);

        IgniteEx grid = (IgniteEx)grid(node.affinity(DEFAULT_CACHE_NAME).mapPartitionToNode(part));

        GridDhtPartitionTopologyImpl top =
            (GridDhtPartitionTopologyImpl)grid.cachex(DEFAULT_CACHE_NAME).context().topology();

        AffinityTopologyVersion failCheckVer = new AffinityTopologyVersion(GRIDS + 2, 1);

        top.partitionFactory((ctx, grp, id) -> new GridDhtLocalPartition(ctx, grp, id, false) {
            @Override public boolean primary(AffinityTopologyVersion topVer) {
                return !(id == part && topVer.equals(failCheckVer)) && super.primary(topVer);
            }
        });

        // Re-create mocked part.
        GridDhtLocalPartition p0 = top.localPartition(part);
        p0.rent(false).get();
        assertTrue(p0.state() == EVICTED);

        ReadWriteLock lock = U.field(top, "lock");
        lock.writeLock().lock();
        p0 = top.getOrCreatePartition(part);
        p0.own();
        lock.writeLock().unlock();

        startGrid(GRIDS);
        awaitPartitionMapExchange();

        try (Transaction tx = near.transactions().txStart()) {
            near.cache(DEFAULT_CACHE_NAME).put(part, part);

            tx.commit();

            fail();
        }
        catch (TransactionRollbackException ignore) {
            // Expected.
        }
        catch (Exception e) {
            fail(X.getFullStackTrace(e));
        }
    }
}
