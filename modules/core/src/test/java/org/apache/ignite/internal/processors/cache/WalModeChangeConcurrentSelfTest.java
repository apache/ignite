/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import com.mchange.v2.c3p0.util.TestUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Concurrent test for WAL state change.
 */
public class WalModeChangeConcurrentSelfTest extends WalModeChangeCommonAbstractSelfTest {
    /**
     * Constructor.
     */
    public WalModeChangeConcurrentSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /**
     * Test that concurrent enable/disable events doesn't leave to hangs.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testConcurrentOperations() throws Exception {
        final Ignite srv1 = startGrid(config(SRV_1, false, false));
        final Ignite srv2 = startGrid(config(SRV_2, false, false));
        final Ignite srv3 = startGrid(config(SRV_3, false, true));

        final Ignite cli = startGrid(config(CLI, true, false));

        final Ignite cacheCli = startGrid(config(CLI_2, true, false));

        cacheCli.active(true);

        final IgniteCache cache = cacheCli.getOrCreateCache(cacheConfig(CacheMode.PARTITIONED));

        for (int i = 0; i < 3; i++) {
            // Start pushing requests.
            Collection<Ignite> walNodes = new ArrayList<>();

            walNodes.add(srv1);
            walNodes.add(srv2);
            walNodes.add(srv3);
            walNodes.add(cli);

            final AtomicBoolean done = new AtomicBoolean();

            final CountDownLatch latch = new CountDownLatch(5);

            for (Ignite node : walNodes) {
                final Ignite node0 = node;

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        checkConcurrentOperations(done, node0);

                        latch.countDown();
                    }
                });

                t.setName("wal-load-" + node0.name());

                t.start();
            }

            // Do some cache loading in the mean time.
            Thread t = new Thread(new Runnable() {
                @Override public void run() {
                    int i = 0;

                    while (!done.get())
                        cache.put(i, i++);

                    latch.countDown();
                }
            });

            t.setName("cache-load");

            t.start();

            Thread.sleep(20_000);

            done.set(true);

            latch.await();

            X.println(">>> Iteration finished: " + i);
        }
    }

    /**
     * Check concurrent operations.
     *
     * @param done Done flag.
     * @param node Node.
     */
    private static void checkConcurrentOperations(AtomicBoolean done, Ignite node) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean state = rnd.nextBoolean();

        while (!done.get()) {
            if (state)
                node.cluster().walEnable(CACHE_NAME);
            else
                node.cluster().walDisable(CACHE_NAME);

            state = !state;
        }

        try {
            Thread.sleep(rnd.nextLong(200, 1000));
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
