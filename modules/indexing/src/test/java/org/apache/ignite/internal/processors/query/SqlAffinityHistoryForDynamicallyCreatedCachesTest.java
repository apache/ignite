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

package org.apache.ignite.internal.processors.query;

import java.util.concurrent.CountDownLatch;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class SqlAffinityHistoryForDynamicallyCreatedCachesTest extends AbstractThinClientTest {
    /** */
    private static final String CACHE_NAME = "SQL_TABLE";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests that the correct meaningful error is thrown in case of race between
     * concurrent cache creation and SQL query. In particular the error is thrown
     * when partitions exchange is not yet completed after the cache used in query
     * just dynamically created.
     * <p>
     * Before the misleading {@code Getting affinity for too old topology version that is
     * already out of history (try to increase 'IGNITE_AFFINITY_HISTORY_SIZE' system property)}
     * error was thrown.
     */
    @Test
    public void testConcurrentCacheCreateAndSqlQuery() throws Exception {
        startGrids(3);

        GridCachePartitionExchangeManager<Object, Object> exchangeMgr = ignite(1).context().cache().context().exchange();

        CountDownLatch exchangeStarted = new CountDownLatch(1);
        CountDownLatch proceedWithExchange = new CountDownLatch(1);

        PartitionsExchangeAware listener = new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                if (fut.hasCachesToStart()) {
                    exchangeStarted.countDown();

                    try {
                        proceedWithExchange.await();
                    }
                    catch (InterruptedException e) {
                        // No-op.
                    }
                }
            }
        };

        exchangeMgr.registerExchangeAwareComponent(listener);

        runAsync(() -> {
            try (IgniteClient cli = startClient(0)) {
                cli.createCache(getCacheConfiguration());
            }
        }, "create-cache-thread");

        try (IgniteClient cli = startClient(0)) {
            exchangeStarted.await();

            assertThrows(
                null,
                () -> cli.query(new SqlFieldsQuery("select * from " + CACHE_NAME)).getAll(),
                ClientException.class,
                "partitions exchange wasn't yet completed after cache creation");
        }

        exchangeMgr.unregisterExchangeAwareComponent(listener);

        proceedWithExchange.countDown();
    }

    /** */
    private ClientCacheConfiguration getCacheConfiguration() {
        return new ClientCacheConfiguration()
            .setName(CACHE_NAME)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(new QueryEntity(Integer.class, Integer.class).setTableName(CACHE_NAME));
    }
}
