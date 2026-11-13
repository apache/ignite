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

package org.apache.ignite.internal.processors.tx;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.query.calcite.GridCommonAbstractWrapperTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
public class SqlTransactionsTopologyChangeTest extends GridCommonAbstractWrapperTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());
        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);

        return cfg;
    }

    /** */
    @AfterEach
    public void stopNodes() {
        stopAllGrids();
    }

    /** */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSelectDuringTopologyChange(boolean client) throws Exception {
        IgniteEx server = startGrid(0);
        IgniteEx node = client ? startClientGrid(1) : server;

        IgniteCache<Integer, Integer> cache = node.createCache(new CacheConfiguration<Integer, Integer>("test-cache")
            .setAtomicityMode(TRANSACTIONAL)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(List.of(new QueryEntity(Integer.class, Integer.class).setTableName("TEST"))));

        node.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        GridDhtPartitionTopology top = server.cachex(cache.getName()).context().topology();
        AffinityTopologyVersion topVer = top.readyTopologyVersion();
        IgniteInternalFuture<?> destroyFut;
        String qry = "SELECT _KEY, _VAL FROM test";

        // Bound the transaction timeout so a regression cannot hang the test indefinitely during planning.
        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 10_000, 1)) {
            // Hold the exchange without planning a query and warming the table statistics.
            cache.put(1, 42);
            destroyFut = runAsync(() -> node.destroyCache(DEFAULT_CACHE_NAME));

            assertTrue("Cache stop exchange did not start", waitForCondition(() -> {
                GridDhtTopologyFuture fut = top.topologyVersionFuture();

                return fut.initialVersion().after(topVer) && !fut.isDone();
            }, 5_000));

            // The query must reach the scan while the exchange is still blocked by the transaction.
            assertThrowsWithCause(() -> sql(node, qry), ClusterTopologyException.class);

            assertFalse(destroyFut.isDone());

            tx.commit();
        }

        destroyFut.get(5_000);

        assertEquals(List.of(List.of(1, 42)), sql(node, qry));
    }

    /** */
    private static List<List<?>> sql(IgniteEx node, String sql) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC").setTimeout(5, SECONDS);

        try (FieldsQueryCursor<List<?>> cur = node.context().query().querySqlFields(qry, false)) {
            return cur.getAll();
        }
    }
}
