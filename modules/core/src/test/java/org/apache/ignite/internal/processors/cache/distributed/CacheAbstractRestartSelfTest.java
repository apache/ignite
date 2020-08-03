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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Abstract restart test.
 */
public abstract class CacheAbstractRestartSelfTest extends IgniteCacheAbstractTest {
    /** */
    private volatile CountDownLatch cacheCheckedLatch = new CountDownLatch(1);

    /** */
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals(getTestIgniteInstanceName(gridCount() - 1)))
            cfg.setClientMode(true);

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 8 * 60_000;
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
    @Test
    public void testRestart() throws Exception {
        final int clientGrid = gridCount() - 1;

        assertTrue(ignite(clientGrid).configuration().isClientMode());

        final IgniteEx grid = grid(clientGrid);

        final IgniteCache cache = jcache(clientGrid);

        updateCache(grid, cache);

        final AtomicBoolean stop = new AtomicBoolean();

        ArrayList<IgniteInternalFuture> updaterFuts = new ArrayList<>();

        for (int i = 0; i < updatersNumber(); i++) {
            final int threadIdx = i;

            IgniteInternalFuture<?> updateFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Thread.currentThread().setName("update-thread-" + threadIdx);

                    assertTrue(cacheCheckedLatch.await(30_000, TimeUnit.MILLISECONDS));

                    int iter = 0;

                    while (!stop.get()) {
                        log.info("Start update: " + iter);

                        rwl.readLock().lock();

                        try {
                            updateCache(grid, cache);
                        }
                        finally {
                            rwl.readLock().unlock();
                        }

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

        long endTime = System.currentTimeMillis() + getTestDuration();

        try {
            int iter = 0;

            while (System.currentTimeMillis() < endTime && !isAnyDone(updaterFuts) && !restartFut.isDone()) {
                try {
                    log.info("Start of cache checking: " + iter);

                    rwl.writeLock().lock();

                    try {
                        checkCache(grid, cache);
                    }
                    finally {
                        rwl.writeLock().unlock();
                    }

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

        checkCache(grid, cache);
    }

    /**
     * @return Test duration.
     * @see #getTestTimeout()
     */
    protected int getTestDuration() {
        return 60_000;
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
     * Checks cache in one thread. All update operations are not executed.
     *
     * @param cache Cache.
     */
    protected abstract void checkCache(IgniteEx grid, IgniteCache cache) throws Exception;

    /**
     * Updates cache in many threads.
     *
     * @param grid Grid.
     * @param cache Cache.
     */
    protected abstract void updateCache(IgniteEx grid, IgniteCache cache) throws Exception;

    /**
     * @param futs Futers.
     * @return {@code True} if all futures are done.
     */
    private static boolean isAnyDone(ArrayList<IgniteInternalFuture> futs) {
        for (IgniteInternalFuture fut : futs) {
            if (fut.isDone())
                return true;
        }

        return false;
    }
}
