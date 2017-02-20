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

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assert;

/**
 *
 */
public class IgniteCacheEntryUpdateFailedTest extends IgniteCacheAbstractTest {

    /**
     *
     */
    public void testCacheEntryUpdateFailedTest() {
        Ignite ignite = grid(0);
        IgniteTransactions txs = ignite.transactions();

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("Test Cache");

        try (Transaction tx =
                 txs.txStart(TransactionConcurrency.PESSIMISTIC,
                     TransactionIsolation.REPEATABLE_READ)) {
            String testKey = "testKey";
            final String testValue = "testValue";
            // invoking entry processor upon entry with new key
            cache.invoke(testKey, new CacheEntryProcessor<Object, Object, Object>() {
                @Override
                public Object process(MutableEntry<Object, Object> entry,
                    Object... arguments) throws EntryProcessorException {
                    entry.setValue(testValue);
                    return null;
                }
            });

            Assert.assertEquals(testValue, cache.get(testKey));
        }
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

}
