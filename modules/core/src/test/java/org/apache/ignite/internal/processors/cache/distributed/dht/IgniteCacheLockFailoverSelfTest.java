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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheLockFailoverSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setNearConfiguration(null);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockFailover() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        Integer key = backupKey(cache);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> restartFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                while (!stop.get()) {
                    stopGrid(1);

                    U.sleep(500);

                    startGrid(1);
                }
                return null;
            }
        });

        try {
            long end = System.currentTimeMillis() + 60_000;

            long iter = 0;

            while (System.currentTimeMillis() < end) {
                if (iter % 100 == 0)
                    log.info("Iteration: " + iter);

                iter++;

                GridCacheAdapter<Object, Object> adapter = ((IgniteKernal)grid(0)).internalCache(null);

                IgniteInternalFuture<Boolean> fut = adapter.lockAsync(key, 0);

                try {
                    fut.get(30_000);

                    U.sleep(1);
                }
                catch (IgniteFutureTimeoutException e) {
                    info("Entry: " + adapter.peekEx(key));

                    fail("Lock timeout [fut=" + fut + ", err=" + e + ']');
                }
                catch (Exception e) {
                    log.error("Error: " + e);
                }
                finally {
                    adapter.unlock(key);
                }
            }
        }
        finally {
            stop.set(true);

            restartFut.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnlockPrimaryLeft() throws Exception {
        GridCacheAdapter<Integer, Integer> cache = ((IgniteKernal)grid(0)).internalCache(null);

        Integer key = backupKey(grid(0).cache(null));

        cache.lock(key, 0);

        stopGrid(1);

        cache.unlock(key);

        GridCacheEntryEx entry = cache.peekEx(key);

        assertTrue("Remote MVCC is not empty: " + entry, entry == null || entry.remoteMvccSnapshot().isEmpty());

        startGrid(1);
    }
}