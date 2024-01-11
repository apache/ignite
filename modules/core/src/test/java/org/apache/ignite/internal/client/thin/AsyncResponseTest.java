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

import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Thin client async response tests.
 */
public class AsyncResponseTest extends AbstractThinClientTest {
    /** Default timeout value. */
    private static final long TIMEOUT = 1_000L;

    /** */
    private static final int THREADS_CNT = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getClientConnectorConfiguration().setThreadPoolSize(THREADS_CNT - 1);

        return cfg;
    }

    /** */
    @Test
    public void testBlockingOps() throws Exception {
        startGrid(0);
        IgniteClient client = startClient(0);
        client.getOrCreateCache(new ClientCacheConfiguration().setName("test").setAtomicityMode(TRANSACTIONAL));

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 100; i++) {
                try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TIMEOUT)) {
                    client.cache("test").put(0, 0);

                    tx.commit();
                }
            }
        }, THREADS_CNT, "put-thread");

        assertEquals(0, client.cache("test").get(0));
    }
}
