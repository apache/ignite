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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;

/**
 *
 */
public abstract class IgniteCachePutRetryAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /**
     * @return Keys count for the test.
     */
    protected abstract int keysCount();

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setAtomicWriteOrderMode(writeOrderMode());
        cfg.setBackups(1);
        cfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        AtomicConfiguration acfg = new AtomicConfiguration();

        acfg.setBackups(1);

        cfg.setAtomicConfiguration(acfg);

        return cfg;
    }

    /**
     * @return Write order mode.
     */
    protected CacheAtomicWriteOrderMode writeOrderMode() {
        return CLOCK;
    }
    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        checkPut(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsync() throws Exception {
        checkPut(true);
    }

    /**
     * @param async If {@code true} tests asynchronous put.
     * @throws Exception If failed.
     */
    private void checkPut(boolean async) throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();

        int keysCnt = keysCount();

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    stopGrid(3);

                    U.sleep(300);

                    startGrid(3);
                }

                return null;
            }
        });


        IgniteCache<Object, Object> cache = ignite(0).cache(null);

        if (atomicityMode() == ATOMIC)
            assertEquals(writeOrderMode(), cache.getConfiguration(CacheConfiguration.class).getAtomicWriteOrderMode());

        int iter = 0;

        long stopTime = System.currentTimeMillis() + 60_000;

        if (async) {
            IgniteCache<Object, Object> cache0 = cache.withAsync();

            while (System.currentTimeMillis() < stopTime) {
                Integer val = ++iter;

                for (int i = 0; i < keysCnt; i++) {
                    cache0.put(i, val);

                    cache0.future().get();
                }

                for (int i = 0; i < keysCnt; i++) {
                    cache0.get(i);

                    assertEquals(val, cache0.future().get());
                }
            }
        }
        else {
            while (System.currentTimeMillis() < stopTime) {
                Integer val = ++iter;

                for (int i = 0; i < keysCnt; i++)
                    cache.put(i, val);

                for (int i = 0; i < keysCnt; i++)
                    assertEquals(val, cache.get(i));
            }
        }

        finished.set(true);
        fut.get();

        for (int i = 0; i < keysCnt; i++)
            assertEquals(iter, cache.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailsWithNoRetries() throws Exception {
        checkFailsWithNoRetries(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailsWithNoRetriesAsync() throws Exception {
        checkFailsWithNoRetries(true);
    }

    /**
     * @param async If {@code true} tests asynchronous put.
     * @throws Exception If failed.
     */
    private void checkFailsWithNoRetries(boolean async) throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    stopGrid(3);

                    U.sleep(300);

                    startGrid(3);
                }

                return null;
            }
        });

        try {
            int keysCnt = keysCount();

        boolean eThrown = false;

        IgniteCache<Object, Object> cache = ignite(0).cache(null).withNoRetries();

        if (async)
            cache = cache.withAsync();

        for (int i = 0; i < keysCnt; i++) {
            try {
                if (async) {
                    cache.put(i, i);

                    cache.future().get();
                }
                else
                    cache.put(i, i);
            }
            catch (Exception e) {
                assertTrue("Invalid exception: " + e,
                    X.hasCause(e, ClusterTopologyCheckedException.class, CachePartialUpdateException.class));

                eThrown = true;

                    break;
                }
            }

        assertTrue(eThrown);

            finished.set(true);

            fut.get();
        }
        finally {
            finished.set(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60 * 1000;
    }
}
