/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridCacheConcurrentGetCacheOnClientTest extends GridCommonAbstractTest{
    /**
     *
     */
    @Test
    public void test() throws Exception {
        IgniteConfiguration node1cfg = getConfiguration("node1");
        IgniteConfiguration node2cfg = getConfiguration("node2");

        Ignite node1 = startGrid("node1", node1cfg);
        Ignite node2 = startGrid("node2", node2cfg);

        IgniteConfiguration clientCfg1 = getConfiguration("client");
        clientCfg1.setClientMode(true);

        IgniteConfiguration clientCfg2 = getConfiguration("client");
        clientCfg2.setClientMode(true);

        final IgniteEx client1 = (IgniteEx)startGrid("client1", clientCfg1);
        final IgniteEx client2 = (IgniteEx)startGrid("client2", clientCfg2);

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
