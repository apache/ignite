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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.GET;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SCAN;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.PUT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Coordinator failover test for partitioned caches.
 */
public class CacheMvccPartitionedCoordinatorFailoverTest extends CacheMvccAbstractCoordinatorFailoverTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxGet_ClientServer_Backups2_CoordinatorFails_Persistence() throws Exception {
        persistence = true;

        accountsTxReadAll(4, 2, 2, DFLT_PARTITION_COUNT,
            null, true, GET, PUT, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxGet_Server_Backups1_CoordinatorFails() throws Exception {
        accountsTxReadAll(2, 0, 1, DFLT_PARTITION_COUNT,
            null, true, GET, PUT, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxScan_ClientServer_Backups2_CoordinatorFails() throws Exception {
        accountsTxReadAll(4, 2, 2, DFLT_PARTITION_COUNT,
            null, true, SCAN, PUT, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAccountsTxScan_Server_Backups1_CoordinatorFails_Persistence() throws Exception {
        persistence = true;

        accountsTxReadAll(2, 0, 1, DFLT_PARTITION_COUNT,
            null, true, SCAN, PUT, DFLT_TEST_TIME, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllGetAll_ClientServer_Backups2_RestartCoordinator_GetPut() throws Exception {
        putAllGetAll(RestartMode.RESTART_CRD, 4, 2, 2, DFLT_PARTITION_COUNT,
            null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllGetAll_ClientServer_Backups1_RestartCoordinator_GetPut_Persistence() throws Exception {
        persistence = true;

        putAllGetAll(RestartMode.RESTART_CRD, 2, 1, 1, 64,
            null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate_N_Objects_ClientServer_Backups1_PutGet_CoordinatorFails_Persistence() throws Exception {
        persistence = true;

        updateNObjectsTest(3, 5, 3, 1, DFLT_PARTITION_COUNT, DFLT_TEST_TIME,
            null, GET, PUT, RestartMode.RESTART_CRD);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate_N_Objects_ClientServer_Backups1__PutGet_CoordinatorFails() throws Exception {
        updateNObjectsTest(10, 3, 2, 1, DFLT_PARTITION_COUNT, DFLT_TEST_TIME,
            null, GET, PUT, RestartMode.RESTART_CRD);
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetReadInProgressCoordinatorFails() throws Exception {
        readInProgressCoordinatorFails(false, false, PESSIMISTIC, REPEATABLE_READ, GET, PUT, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetReadInsideTxInProgressCoordinatorFails() throws Exception {
        readInProgressCoordinatorFails(false, true, PESSIMISTIC, REPEATABLE_READ, GET, PUT, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetReadInProgressCoordinatorFails_ReadDelay() throws Exception {
        readInProgressCoordinatorFails(true, false, PESSIMISTIC, REPEATABLE_READ, GET, PUT, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetReadInsideTxInProgressCoordinatorFails_ReadDelay() throws Exception {
        readInProgressCoordinatorFails(true, true, PESSIMISTIC, REPEATABLE_READ, GET, PUT, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadInProgressCoordinatorFailsSimple_FromServerPutGet() throws Exception {
        readInProgressCoordinatorFailsSimple(false, null, GET, PUT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateDeactivateCLuster() throws Exception {
        disableScheduledVacuum = true;
        persistence = true;

        final int DATA_NODES = 3;

        // Do not use startMultithreaded here.
        startGrids(DATA_NODES);

        Ignite near = grid(DATA_NODES - 1);

        CacheConfiguration ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, DATA_NODES - 1, DFLT_PARTITION_COUNT);

        near.cluster().active(true);

        IgniteCache cache = near.createCache(ccfg);

        cache.put(1, 1);

        near.cluster().active(false);

        stopGrid(0);

        near.cluster().active(true);

        assertEquals(1, cache.get(1));
    }
}
