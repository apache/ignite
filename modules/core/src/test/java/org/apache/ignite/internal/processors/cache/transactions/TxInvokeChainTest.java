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

import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Tests a chain of invokes under a tx.
 */
public class TxInvokeChainTest extends GridCacheAbstractSelfTest {
    /** */
    private static final AtomicInteger cnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(igniteInstanceName.equals("client"));

        return cfg;
    }

    /**
     *
     */
    public void testInvokeChain() throws Exception {
        int invokeCnt = 100;

        int expCnt = invokeCnt * 3; // get() + 1 primary + 1 backup.

        Ignite client = startGrid("client");

        int k = 0;

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                doTestInvokeChain(client, k++, invokeCnt, concurrency, isolation,
                    new IgniteOutClosure<CacheEntryProcessor<Integer, Integer, ?>>() {
                        @Override public CacheEntryProcessor<Integer, Integer, ?> apply() {
                            return new CacheEntryProcessor<Integer, Integer, Void>() {
                                @Override public Void process(MutableEntry<Integer, Integer> entry,
                                    Object... arguments) throws EntryProcessorException {
                                    cnt.incrementAndGet();

                                    entry.setValue(entry.getValue() + 1);

                                    return null;
                                }
                            };
                        }
                    });

                assertEquals(expCnt, cnt.get());

                doTestInvokeChain(client, k++, invokeCnt, concurrency, isolation,
                    new IgniteOutClosure<CacheEntryProcessor<Integer, Integer, ?>>() {
                        @Override public CacheEntryProcessor<Integer, Integer, ?> apply() {
                            return new CacheEntryProcessor<Integer, Integer, Integer>() {
                                @Override public Integer process(MutableEntry<Integer, Integer> entry,
                                    Object... arguments) throws EntryProcessorException {
                                    cnt.incrementAndGet();

                                    entry.setValue(entry.getValue() + 1);

                                    return entry.getValue();
                                }
                            };
                        }
                    });

                assertTrue(expCnt < cnt.get());
            }
        }
    }

    /**
     * Ensure EntryProcessor is called proper number of times.
     *
     * @throws Exception
     */
    private void doTestInvokeChain(Ignite client, int key, int invokeCnt, TransactionConcurrency conc, TransactionIsolation isolation,
        IgniteOutClosure<CacheEntryProcessor<Integer, Integer, ?>> factoryClo) throws Exception {
        cnt.set(0);

        client.cache(DEFAULT_CACHE_NAME).put(key, 0);

        try (Transaction tx = client.transactions().txStart(conc, isolation)) {
            IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < invokeCnt; i++)
                cache.invoke(key, factoryClo.apply());

            assertEquals(invokeCnt, cache.get(key).intValue());

            tx.commit();
        }

        assertEquals(invokeCnt, client.cache(DEFAULT_CACHE_NAME).get(key));
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void initStoreStrategy() throws IgniteCheckedException {
        storeStgy = null;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }
}
