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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Abstract restart test.
 */
public abstract class CacheAbstractRestartSelfTest extends IgniteCacheAbstractTest {
    /** */
    private IgniteCache cache;

    /** */
    private volatile CountDownLatch cacheCheckedLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.equals(getTestGridName(gridCount() - 1)))
            cfg.setClientMode(true);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 15 * 60_000;
    }

    /**
     * @return Number of updaters threads.
     */
    protected int updatersNumber() {
        return 64;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestart() throws Exception {
        final int clientGrid = gridCount() - 1;

        assertTrue(ignite(clientGrid).configuration().isClientMode());

        cache = jcache(clientGrid);

        updateCache(cache);

        final AtomicBoolean stop = new AtomicBoolean();

        ArrayList<IgniteInternalFuture> updaterFuts = new ArrayList<>();

        for (int i = 0; i < updatersNumber(); i++) {
            final int finalI = i;
            IgniteInternalFuture<?> updateFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Thread.currentThread().setName("update-thread-" + finalI);

                    assertTrue(cacheCheckedLatch.await(30_000, TimeUnit.MILLISECONDS));

                    int iter = 0;

                    while (!stop.get()) {
                        log.info("Start update: " + iter);

                        updateCache(cache);

                        log.info("End update: " + iter++);
                    }

                    log.info("Update iterations: " + iter);

                    return null;
                }
            });

            updaterFuts.add(updateFut);
        }

        IgniteInternalFuture<?> restartFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Thread.currentThread().setName("restart-thread");

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    assertTrue(cacheCheckedLatch.await(30_000, TimeUnit.MILLISECONDS));

                    int node = rnd.nextInt(0, gridCount() - 1);

                    log.info("Stop node: " + node);

                    stopGrid(node);

                    U.sleep(restartSleep());

                    log.info("Start node: " + node);

                    startGrid(node);

                    cacheCheckedLatch = new CountDownLatch(1);

                    U.sleep(restartDelay());

                    awaitPartitionMapExchange();
                }

                return null;
            }
        });

        long endTime = System.currentTimeMillis() + 10 * 60_000;

        try {
            int iter = 0;

            while (System.currentTimeMillis() < endTime && !isDone(updaterFuts) && !restartFut.isDone()) {
                try {
                    log.info("Start of cache checking: " + iter);

                    checkCache(cache);

                    log.info("End of cache checking: " + iter++);
                }
                finally {
                    cacheCheckedLatch.countDown();
                }
            }

            log.info("Checking iteration: " + iter);
        }
        finally {
            cacheCheckedLatch.countDown();

            stop.set(true);
        }

        for (IgniteInternalFuture fut : updaterFuts)
            fut.get();

        restartFut.get();

        checkCache(cache);
    }

    /**
     * @return Restart sleep in milliseconds.
     */
    private int restartSleep() {
        return 100;
    }

    /**
     * @return Restart delay in milliseconds.
     */
    private int restartDelay() {
        return 100;
    }

    /**
     * Checks cache.
     *
     * @param cache Cache.
     */
    protected abstract void checkCache(IgniteCache cache);

    /**
     * Updates cache.
     *
     * @param cache Cache.
     */
    protected abstract void updateCache(IgniteCache cache);

    /**
     * @param futs Futers.
     * @return {@code True} if all futures are done.
     */
    private static boolean isDone(ArrayList<IgniteInternalFuture> futs) {
        for (IgniteInternalFuture fut : futs) {
            if (!fut.isDone())
                return false;
        }

        return true;
    }
}
