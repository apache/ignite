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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test for getting values on unstable topology with read from backup enabled.
 */
public class CacheGetReadFromBackupFailoverTest extends GridCommonAbstractTest {
    /** Tx cache name. */
    private static final String TX_CACHE = "txCache";

    /** Atomic cache name. */
    private static final String ATOMIC_CACHE = "atomicCache";

    /** Keys count. */
    private static final int KEYS_CNT = 50000;

    /** Stop load flag. */
    private static final AtomicBoolean stop = new AtomicBoolean();

    /** Error. */
    private static final AtomicReference<Throwable> err = new AtomicReference<>();

    /**
     * @return Grid count.
     */
    public int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new AbstractFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                err.compareAndSet(null, failureCtx.error());
                stop.set(true);
                return false;
            }
        });

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration<Long, Long> txCcfg = new CacheConfiguration<Long, Long>(TX_CACHE)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setReadFromBackup(true);

        CacheConfiguration<Long, Long> atomicCcfg = new CacheConfiguration<Long, Long>(ATOMIC_CACHE)
            .setAtomicityMode(ATOMIC)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setReadFromBackup(true);

        cfg.setCacheConfiguration(txCcfg, atomicCcfg);

        // Enforce different mac adresses to emulate distributed environment by default.
        cfg.setUserAttributes(Collections.singletonMap(
            IgniteNodeAttributes.ATTR_MACS_OVERRIDE, UUID.randomUUID().toString()));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stop.set(false);

        err.set(null);

        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailover() throws Exception {
        Ignite ignite = ignite(0);

        ignite.cluster().active(true);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        try (IgniteDataStreamer<Object, Object> stmr = ignite.dataStreamer(TX_CACHE)) {
            for (int i = 0; i < KEYS_CNT; i++)
                stmr.addData(i, rnd.nextLong());
        }

        try (IgniteDataStreamer<Object, Object> stmr = ignite.dataStreamer(ATOMIC_CACHE)) {
            for (int i = 0; i < KEYS_CNT; i++)
                stmr.addData(i, rnd.nextLong());
        }

        AtomicInteger idx = new AtomicInteger(-1);

        AtomicInteger successGet = new AtomicInteger();

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            ThreadLocalRandom rnd0 = ThreadLocalRandom.current();

            while (!stop.get()) {
                Ignite ig = null;

                while (ig == null) {
                    int n = rnd0.nextInt(gridCount());

                    if (idx.get() != n) {
                        try {
                            ig = ignite(n);
                        }
                        catch (IgniteIllegalStateException e) {
                            // No-op.
                        }
                    }
                }

                try {
                    if (rnd.nextBoolean()) {
                        ig.cache(TX_CACHE).get(rnd0.nextLong(KEYS_CNT));
                        ig.cache(ATOMIC_CACHE).get(rnd0.nextLong(KEYS_CNT));
                    }
                    else {
                        ig.cache(TX_CACHE).getAll(rnd.longs(16, 0, KEYS_CNT).boxed().collect(Collectors.toSet()));
                        ig.cache(ATOMIC_CACHE).getAll(rnd.longs(16, 0, KEYS_CNT).boxed().collect(Collectors.toSet()));
                    }

                    successGet.incrementAndGet();
                }
                catch (CacheException e) {
                    if (!X.hasCause(e, NodeStoppingException.class))
                        throw e;
                }

            }
        }, "load-thread");

        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < 30 * 1000L) {
            int idx0 = idx.get();

            if (idx0 >= 0)
                startGrid(idx0);

            U.sleep(500);

            int next = rnd.nextInt(gridCount());

            idx.set(next);

            stopGrid(next);

            U.sleep(500);
        }

        stop.set(true);

        while (true) {
            try {
                fut.get(10_000);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                for (Ignite i : G.allGrids()) {
                    IgniteEx ex = (IgniteEx)i;

                    log.info(">>>> " + ex.context().localNodeId());

                    GridCacheMvccManager mvcc = ex.context().cache().context().mvcc();

                    for (GridCacheFuture<?> fut0 : mvcc.activeFutures()) {
                        log.info("activeFut - " + fut0);
                    }

                    for (GridCacheFuture<?> fut0 : mvcc.atomicFutures()) {
                        log.info("atomicFut - " + fut0);
                    }
                }
            }
        }

        Assert.assertTrue(String.valueOf(successGet.get()), successGet.get() > 50);

        Throwable e = err.get();

        if (e != null) {
            log.error("Test failed", e);

            fail("Test failed");
        }
    }
}
