/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class GridCacheConcurrentGetCacheOnClientTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void test() throws Exception {
        IgniteConfiguration node1cfg = getConfiguration("node1");
        IgniteConfiguration node2cfg = getConfiguration("node2");

        Ignite node1 = startGrid("node1", node1cfg);
        Ignite node2 = startGrid("node2", node2cfg);

        final IgniteEx client1 = startClientGrid("client1", getConfiguration("client"));
        final IgniteEx client2 = startClientGrid("client2", getConfiguration("client"));

        final CountDownLatch startLatch = new CountDownLatch(1);

        final CountDownLatch stopLatch = new CountDownLatch(2);

        final AtomicInteger countFails = new AtomicInteger();

        final AtomicInteger exceptionFails = new AtomicInteger();

        final String cacheName = "TEST_CACHE";

        runAsync(new Runnable() {
            @Override public void run() {
                try {
                    startLatch.await();

                    IgniteCache<Object, Object> cache = client2.cache(cacheName);

                    if (cache == null)
                        countFails.incrementAndGet();

                    stopLatch.countDown();
                }
                catch (Exception e) {
                    exceptionFails.incrementAndGet();
                }
            }
        });

        runAsync(new Runnable() {
            @Override public void run() {
                try {
                    startLatch.await();

                    IgniteCache<Object, Object> cache = client2.cache(cacheName);

                    if (cache == null)
                        countFails.incrementAndGet();

                    stopLatch.countDown();
                }
                catch (Exception e) {
                    exceptionFails.incrementAndGet();
                }
            }
        });

        client1.getOrCreateCache(cacheName);

        startLatch.countDown();

        IgniteCache<Object, Object> cache = client2.cache(cacheName);

        if (cache == null)
            countFails.incrementAndGet();

        stopLatch.await();

        if (countFails.get() != 0 || exceptionFails.get() != 0)
            fail("Cache return null in " + countFails.get() + " of 3 cases. Total exception: " + exceptionFails.get());
    }
}
