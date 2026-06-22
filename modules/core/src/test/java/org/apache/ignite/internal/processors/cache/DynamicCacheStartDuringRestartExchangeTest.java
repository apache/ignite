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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class DynamicCacheStartDuringRestartExchangeTest extends GridCommonAbstractTest {
    /** */
    private static final String ERR_MSG = "Invalid exchange futures state";

    /** */
    private static final int SRV_NODES = 3;

    /** */
    private static final int CLIENT_THREADS = 32;

    /** */
    private static final int RESTART_CNT = 10;

    /** */
    private static final int FIRST_CLIENT_PORT = 10800;

    /** */
    private static final int CACHE_CFG_CNT = 30;

    /** */
    private final AtomicBoolean stopClients = new AtomicBoolean();

    /** */
    private final AtomicInteger clientCreateSuccesses = new AtomicInteger();

    /** */
    private final AtomicReference<Throwable> caughtErr = new AtomicReference<>();

    /** */
    private final CountDownLatch caughtLatch = new CountDownLatch(1);

    /** */
    private final Map<Integer, ClientCacheConfiguration> cacheConfs = new HashMap<>();

    /** */
    private Thread.UncaughtExceptionHandler oldUncaughtHnd;

    /** */
    private IgniteInternalFuture<?> startFut;

    /** */
    private IgniteInternalFuture<?> clientFut;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        cfg.setConsistentId("node-" + idx);

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setPort(FIRST_CLIENT_PORT + idx)
            .setPortRange(0));

        cfg.setCacheConfiguration(new CacheConfiguration<>("static-cache")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(2));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            stopClients.set(true);

            if (oldUncaughtHnd != null)
                Thread.setDefaultUncaughtExceptionHandler(oldUncaughtHnd);

            cancelFuture(startFut);
            cancelFuture(clientFut);

            waitFuture(startFut, 1_000);
            waitFuture(clientFut, 1_000);

            stopAllGrids(true);
        }
        finally {
            super.afterTest();
        }
    }

    /** */
    @Test
    public void testThinClientsCreateDynamicCachesDuringClusterRestart() throws Exception {
        prepareCacheConfigs();

        prepareExceptionHandler();

        startGrids(3);

        awaitPartitionMapExchange();

        startThinClientLoad();

        assertTrue(waitForCondition(() -> clientCreateSuccesses.get() >= 50, 5_000));

        for (int i = 0; i < RESTART_CNT; i++) {
            stopAllGrids();

            waitForTopology(0);

            startGridsConcurrently();

            if (isCaught())
                break;

            awaitPartitionMapExchange();

            if (isCaught())
                break;
        }

        assertNull("Caught failure: " + ERR_MSG, caughtErr.get());
    }

    /** */
    private void prepareExceptionHandler() {
        oldUncaughtHnd = Thread.getDefaultUncaughtExceptionHandler();

        Thread.setDefaultUncaughtExceptionHandler((thread, err) -> {
            if (isExpectedFailure(err)) {
                markCaught(err);

                return;
            }

            if (oldUncaughtHnd != null)
                oldUncaughtHnd.uncaughtException(thread, err);
        });
    }

    /** */
    private void startGridsConcurrently() throws Exception {
        AtomicInteger nodeIdx = new AtomicInteger();

        CountDownLatch startLatch = new CountDownLatch(1);

        startFut = GridTestUtils.runMultiThreadedAsync(() -> {
            int idx = nodeIdx.getAndIncrement();

            try {
                startLatch.await();

                if (!isCaught())
                    startGrid(idx);
            }
            catch (Throwable e) {
                if (isExpectedFailure(e))
                    markCaught(e);
                else
                    throw new RuntimeException(e);
            }
        }, SRV_NODES, "start-node");

        startLatch.countDown();

        assertTrue("Timed out while starting grids concurrently",
            waitForCondition(() -> isCaught() || startFut.isDone(), 30_000));

        if (isCaught())
            return;

        try {
            startFut.get();
        }
        catch (Throwable e) {
            if (isExpectedFailure(e)) {
                markCaught(e);

                return;
            }

            throw e;
        }
    }

    /** */
    private void prepareCacheConfigs() {
        for (int i = 0; i < CACHE_CFG_CNT; i++) {
            ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration()
                .setName("thin-cache-" + i)
                .setGroupName("thin-group-" + i)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(2);

            cacheConfs.put(i, cacheCfg);
        }
    }

    /** */
    private void startThinClientLoad() throws Exception {
        CountDownLatch started = new CountDownLatch(CLIENT_THREADS);

        clientFut = GridTestUtils.runMultiThreadedAsync(() -> {
            started.countDown();

            while (!stopClients.get() && !isCaught()) {
                try {
                    createCacheFromNewThinClientConnection();
                }
                catch (Throwable e) {
                    if (isExpectedFailure(e)) {
                        markCaught(e);

                        break;
                    }

                    doSleep(100);
                }
            }
        }, CLIENT_THREADS, "thin-client-load");

        started.await();
    }

    /** */
    private void createCacheFromNewThinClientConnection() {
        ClientConfiguration clientCfg = new ClientConfiguration()
            .setAddresses(clientAddresses())
            .setTimeout(1_000)
            .setReconnectThrottlingRetries(0);

        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            ClientCacheConfiguration cacheCfg = cacheConfs.get(ThreadLocalRandom.current().nextInt(CACHE_CFG_CNT));

            client.getOrCreateCache(cacheCfg);

            clientCreateSuccesses.incrementAndGet();
        }
    }

    /** */
    private String[] clientAddresses() {
        String[] addrs = new String[SRV_NODES];

        for (int i = 0; i < SRV_NODES; i++)
            addrs[i] = "127.0.0.1:" + (FIRST_CLIENT_PORT + i);

        return addrs;
    }

    /** */
    private boolean isExpectedFailure(Throwable err) {
        if (err == null)
            return false;

        String msg = err.getMessage();

        if (msg != null && msg.contains(ERR_MSG))
            return true;

        if (isExpectedFailure(err.getCause()))
            return true;

        for (Throwable suppressed : err.getSuppressed()) {
            if (isExpectedFailure(suppressed))
                return true;
        }

        return false;
    }

    /** */
    private boolean isCaught() {
        return caughtErr.get() != null;
    }

    /** */
    private void markCaught(Throwable err) {
        if (caughtErr.compareAndSet(null, err)) {
            stopClients.set(true);

            caughtLatch.countDown();
        }
    }

    /** */
    private void cancelFuture(IgniteInternalFuture<?> fut) {
        if (fut == null)
            return;

        try {
            fut.cancel();
        }
        catch (Throwable ignored) {
            // No-op
        }
    }

    /** */
    private void waitFuture(IgniteInternalFuture<?> fut, long timeout) {
        if (fut == null)
            return;

        try {
            fut.get(timeout);
        }
        catch (Throwable ignored) {
            // No-op
        }
    }
}
