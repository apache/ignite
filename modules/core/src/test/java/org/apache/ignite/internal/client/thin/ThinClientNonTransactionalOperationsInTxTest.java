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
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import javax.cache.CacheException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.client.thin.TcpClientCache.NON_TRANSACTIONAL_CLIENT_CACHE_IN_TX_ERROR_MESSAGE;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** Checks that non-transactional cache operations fail within a transaction. */
public class ThinClientNonTransactionalOperationsInTxTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName).setCacheConfiguration(defaultCacheConfiguration()
            .setAtomicityMode(TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testThinClientCacheClear() throws Exception {
        startGrid(0);

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {

            checkThinClientCacheClearOperation(client, cache -> cache.clear());

            checkThinClientCacheClearOperation(client, cache -> cache.clear(2));

            checkThinClientCacheClearOperation(client, cache -> cache.clear(Collections.singleton(2)));

            checkThinClientCacheClearOperation(client, cache -> cache.clearAll(Collections.singleton(2)));

            checkThinClientCacheClearOperation(client, cache -> cache.clearAsync());

            checkThinClientCacheClearOperation(client, cache -> cache.clearAsync(2));

            checkThinClientCacheClearOperation(client, cache -> cache.clearAllAsync(Collections.singleton(2)));
        }
    }

    /**
     * It should throw exception.
     *
     * @param client IgniteClient.
     * @param op Operation.
     */
    private void checkThinClientCacheClearOperation(IgniteClient client, Consumer<ClientCache<Object, Object>> op) {
        ClientCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        GridTestUtils.assertThrows(log, (Callable<Void>)() -> {
            try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                cache.put(2, 2);

                op.accept(cache);
            }

            return null;
        }, CacheException.class, String.format(NON_TRANSACTIONAL_CLIENT_CACHE_IN_TX_ERROR_MESSAGE, "clear"));

        assertTrue(cache.containsKey(1));
        assertFalse(cache.containsKey(2));
    }

    /** */
    @Test
    public void testThinClientCacheRemove() throws Exception {
        startGrid(0);

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {

            checkThinClientCacheRemoveOperation(client, cache -> cache.removeAll());

            checkThinClientCacheRemoveOperation(client, cache -> cache.removeAllAsync());
        }
    }

    /**
     * It should throw exception.
     *
     * @param client IgniteClient.
     * @param op Operation.
     */
    private void checkThinClientCacheRemoveOperation(IgniteClient client, Consumer<ClientCache<Object, Object>> op) {
        ClientCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        GridTestUtils.assertThrows(log, (Callable<Void>)() -> {
            try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                cache.put(2, 2);

                op.accept(cache);
            }

            return null;
        }, CacheException.class, String.format(NON_TRANSACTIONAL_CLIENT_CACHE_IN_TX_ERROR_MESSAGE, "removeAll"));

        assertTrue(cache.containsKey(1));
        assertFalse(cache.containsKey(2));
    }
}
