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

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/** */
public class TxAsyncOpsSemaphorePermitsExceededTest extends GridCommonAbstractTest {
    /** Failed flag. */
    private final AtomicBoolean failed = new AtomicBoolean(false);

    /** */
    private static final int MAX_NUM_OP_BEFORE_EXCEED = 2;

    /** */
    private final Random rnd = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(2)
            .setMaxConcurrentAsyncOperations(Integer.MAX_VALUE - MAX_NUM_OP_BEFORE_EXCEED));

        cfg.setFailureHandler((ignite, ctx) -> {
            failed.set(true);

            return true;
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testImplicitAsyncPutOps() throws Exception {
        runOps(() ->
            grid(0).cache(DEFAULT_CACHE_NAME).put(rnd.nextInt(), rnd.nextInt())
        );
    }

    /** */
    @Test
    public void testSyncPutOps() throws Exception {
        runOps(() -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                grid(0).cache(DEFAULT_CACHE_NAME).put(rnd.nextInt(), rnd.nextInt());

                tx.commit();
            }
        });
    }

    /** */
    @Test
    public void testAsyncPutOps() throws Exception {
        runOps(() -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                grid(0).cache(DEFAULT_CACHE_NAME).putAsync(rnd.nextInt(), rnd.nextInt()).get();

                tx.commit();
            }
        });
    }

    /** */
    private void runOps(Runnable tx) throws Exception {
        AtomicInteger cnt = new AtomicInteger();

        multithreadedAsync(() -> {
            while (!Thread.interrupted() && cnt.incrementAndGet() < MAX_NUM_OP_BEFORE_EXCEED * 100)
                tx.run();
        }, MAX_NUM_OP_BEFORE_EXCEED).get();

        assertFalse("Critical failure occurred", failed.get());
    }
}
