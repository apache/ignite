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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;

/**
 * Test for distributed queries with node restarts.
 */
@WithSystemProperty(key = IGNITE_SQL_RETRY_TIMEOUT, value = "1000000")
public class IgniteCacheQueryNodeRestartDistributedJoinSelfTest extends IgniteCacheQueryAbstractDistributedJoinSelfTest {
    /** Total nodes. */
    private int totalNodes = 6;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        if (totalNodes > GRID_CNT) {
            for (int i = GRID_CNT; i < totalNodes; i++)
                startGrid(i);
        }
        else
            totalNodes = GRID_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestarts() throws Exception {
        restarts(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRestartsBroadcast() throws Exception {
        restarts(true);
    }

    /**
     * @param broadcastQry If {@code true} tests broadcast query.
     * @throws Exception If failed.
     */
    private void restarts(final boolean broadcastQry) throws Exception {
        int duration = 90 * 1000;
        int qryThreadNum = 4;
        int restartThreadsNum = 2; // 4 + 2 = 6 nodes
        final int nodeLifeTime = 4000;
        final int logFreq = 100;

        final AtomicIntegerArray locks = new AtomicIntegerArray(totalNodes);

        SqlFieldsQuery qry0;

        if (broadcastQry)
            qry0 = new SqlFieldsQuery(QRY_0_BROADCAST).setDistributedJoins(true).setEnforceJoinOrder(true);
        else
            qry0 = new SqlFieldsQuery(QRY_0).setDistributedJoins(true);

        String plan = queryPlan(grid(0).cache("pu"), qry0);

        X.println("Plan1: " + plan);

        assertEquals(broadcastQry, plan.contains("batched:broadcast"));

        final List<List<?>> goldenRes = grid(0).cache("pu").query(qry0).getAll();

        Thread.sleep(3000);

        assertEquals(goldenRes, grid(0).cache("pu").query(qry0).getAll());

        final SqlFieldsQuery qry1;

        if (broadcastQry)
            qry1 = new SqlFieldsQuery(QRY_1_BROADCAST).setDistributedJoins(true).setEnforceJoinOrder(true);
        else
            qry1 = new SqlFieldsQuery(QRY_1).setDistributedJoins(true);

        plan = queryPlan(grid(0).cache("co"), qry1);

        X.println("Plan2: " + plan);

        assertEquals(broadcastQry, plan.contains("batched:broadcast"));

        final List<List<?>> rRes = grid(0).cache("co").query(qry1).getAll();

        assertFalse(goldenRes.isEmpty());
        assertFalse(rRes.isEmpty());

        final AtomicInteger qryCnt = new AtomicInteger();
        final AtomicBoolean qrysDone = new AtomicBoolean();

        final AtomicBoolean fail = new AtomicBoolean();

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                GridRandom rnd = new GridRandom();

                try {
                    while (!qrysDone.get()) {
                        int g;

                        do {
                            g = rnd.nextInt(locks.length());

                            if (fail.get())
                                return;
                        }
                        while (!locks.compareAndSet(g, 0, 1));

                        if (rnd.nextBoolean()) {
                            IgniteCache<?, ?> cache = grid(g).cache("pu");

                            SqlFieldsQuery qry;

                            if (broadcastQry)
                                qry = new SqlFieldsQuery(QRY_0_BROADCAST).setDistributedJoins(true).setEnforceJoinOrder(true);
                            else
                                qry = new SqlFieldsQuery(QRY_0).setDistributedJoins(true);

                            boolean smallPageSize = rnd.nextBoolean();

                            qry.setPageSize(smallPageSize ? 30 : 1000);

                            try {
                                assertEquals(goldenRes, cache.query(qry).getAll());
                            }
                            catch (CacheException e) {
                                if (!smallPageSize)
                                    log.error("Unexpected exception at the test", e);

                                assertTrue("On large page size must retry.", smallPageSize);

                                boolean failedOnRemoteFetch = false;

                                for (Throwable th = e; th != null; th = th.getCause()) {
                                    if (!(th instanceof CacheException))
                                        continue;

                                    if (th.getMessage() != null &&
                                        th.getMessage().startsWith("Failed to fetch data from node:")) {
                                        failedOnRemoteFetch = true;

                                        break;
                                    }
                                }

                                if (!failedOnRemoteFetch) {
                                    e.printStackTrace();

                                    fail("Must fail inside of GridResultPage.fetchNextPage or subclass.");
                                }
                            }
                        }
                        else {
                            IgniteCache<?, ?> cache = grid(g).cache("co");

                            assertEquals(rRes, cache.query(qry1).getAll());
                        }

                        locks.set(g, 0);

                        int c = qryCnt.incrementAndGet();

                        if (c % logFreq == 0)
                            info("Executed queries: " + c);
                    }
                }
                catch (Throwable e) {
                    e.printStackTrace();

                    error("Got exception: " + e.getMessage());

                    fail.set(true);
                }

            }
        }, qryThreadNum, "query-thread");

        final AtomicInteger restartCnt = new AtomicInteger();

        final AtomicBoolean restartsDone = new AtomicBoolean();

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable<Object>() {
            @SuppressWarnings({"BusyWait"})
            @Override public Object call() throws Exception {
                try {
                    GridRandom rnd = new GridRandom();

                    while (!restartsDone.get()) {
                        int g;

                        do {
                            g = rnd.nextInt(locks.length());

                            if (fail.get())
                                return null;
                        }
                        while (!locks.compareAndSet(g, 0, -1));

                        log.info("Stop node: " + g);

                        stopGrid(g);

                        Thread.sleep(rnd.nextInt(nodeLifeTime));

                        log.info("Start node: " + g);

                        startGrid(g);

                        Thread.sleep(rnd.nextInt(nodeLifeTime));

                        locks.set(g, 0);

                        int c = restartCnt.incrementAndGet();

                        if (c % logFreq == 0)
                            info("Node restarts: " + c);
                    }

                    return true;
                }
                catch (Throwable e) {
                    e.printStackTrace();

                    return true;
                }
            }
        }, restartThreadsNum, "restart-thread");

        GridTestUtils.waitForCondition(() -> fail.get(), duration);

        info("Stopping...");

        restartsDone.set(true);
        qrysDone.set(true);

        fut2.get();
        fut1.get();

        if (fail.get())
            fail("See message above");

        info("Stopped.");
    }
}
