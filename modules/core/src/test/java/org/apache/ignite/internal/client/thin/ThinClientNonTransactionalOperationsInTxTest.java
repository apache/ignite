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

package org.apache.ignite.internal.client.thin;

import java.util.Collections;
import java.util.function.Consumer;
import javax.cache.CacheException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.client.thin.TcpClientCache.NON_TRANSACTIONAL_CLIENT_CACHE_CLEAR_IN_TX_ERROR_MESSAGE;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** Checks that non-transactional cache operations fail within a transaction. */
public class ThinClientNonTransactionalOperationsInTxTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName)
            .setCacheConfiguration(defaultCacheConfiguration().setAtomicityMode(TRANSACTIONAL));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testThinClientCacheClear() throws Exception {
        startGrid(0);

        IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER));

        checkThinClientCacheOperation(client, cache -> cache.clear());

        checkThinClientCacheOperation(client, cache -> cache.clear(2));

        checkThinClientCacheOperation(client, cache -> cache.clear(Collections.singleton(2)));

        checkThinClientCacheOperation(client, cache -> cache.clearAll(Collections.singleton(2)));

        checkThinClientCacheOperation(client, cache -> cache.clearAsync());

        checkThinClientCacheOperation(client, cache -> cache.clearAsync(2));

        checkThinClientCacheOperation(client, cache -> cache.clearAllAsync(Collections.singleton(2)));
    }

    /**
     * It should throw exception.
     *
     * @param client IgniteClient.
     */
    private void checkThinClientCacheOperation(IgniteClient client, Consumer<ClientCache<Object, Object>> op) {
        ClientCache<Object, Object> cache = client.getOrCreateCache(new ClientCacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
        );

        cache.put(1, 1);

        CacheException err = null;

        try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            op.accept(cache);
        }
        catch (CacheException e) {
            err = e;
        }

        assertTrue(err != null && err.getMessage().startsWith(NON_TRANSACTIONAL_CLIENT_CACHE_CLEAR_IN_TX_ERROR_MESSAGE));

        assertTrue(cache.containsKey(1));
        assertFalse(cache.containsKey(2));
    }
}
