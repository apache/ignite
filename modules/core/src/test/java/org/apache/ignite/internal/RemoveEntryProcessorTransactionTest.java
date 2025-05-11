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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest.RemoveAndReturnNullEntryProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/** */
public class RemoveEntryProcessorTransactionTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testDelete() throws Exception {
        IgniteCache<String, Integer> c = startGrid(0).createCache(new CacheConfiguration<String, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation txIsolation : TransactionIsolation.values()) {
                c.put("key", 1);

                try (Transaction tx = grid(0).transactions().txStart(txConcurrency, txIsolation)) {
                    c.invoke("key", new RemoveAndReturnNullEntryProcessor());

                    assertNull(c.get("key"));
                }

            }
        }

    }
}
