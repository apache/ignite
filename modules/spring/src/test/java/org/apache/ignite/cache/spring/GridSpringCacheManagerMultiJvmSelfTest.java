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

package org.apache.ignite.cache.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Spring cache test in multi jvm environment.
 */
public class GridSpringCacheManagerMultiJvmSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName(int idx) {
        return getTestIgniteInstanceName() + idx;
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "testGrid";
    }

    /**
     * @throws Exception If failed.
     */
    public void testSyncCache() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9488");

        IgniteEx loc = startGrid(0);

        final int threads = 4;
        final int entries = 100_000;
        final int remoteNum = 2;

        final CountDownLatch latch = new CountDownLatch(1);

        List<IgniteInternalFuture<Integer>> futures = new ArrayList<>(remoteNum);

        for (int i = 0; i < remoteNum; i++) {
            final int gridIdx = i + 1;

            final IgniteEx remote = startGrid(gridIdx);

            IgniteInternalFuture<Integer> calledCntFut = GridTestUtils.runAsync(new Callable<Integer>() {
                @Override public Integer call() throws Exception {
                    latch.await();

                    return executeRemotely((IgniteProcessProxy)remote, new TestIgniteCallable<Integer>() {
                        @Override public Integer call(Ignite ignite) throws Exception {
                            BeanFactory factory =
                                new ClassPathXmlApplicationContext(
                                    "org/apache/ignite/cache/spring/spring-caching" + gridIdx + ".xml");

                            final GridSpringDynamicCacheTestService dynamicSvc =
                                (GridSpringDynamicCacheTestService)factory.getBean("dynamicTestService");

                            final CyclicBarrier barrier = new CyclicBarrier(threads);

                            GridTestUtils.runMultiThreaded(
                                new Callable() {
                                    @Override public Object call() throws Exception {
                                        for (int i = 0; i < entries; i++) {
                                            barrier.await();

                                            assertEquals("value" + i, dynamicSvc.cacheableSync(i));
                                            assertEquals("value" + i, dynamicSvc.cacheableSync(i));
                                        }

                                        return null;
                                    }
                                },
                                threads,
                                "get-sync");

                            return dynamicSvc.called();
                        }
                    });

                }
            });

            futures.add(calledCntFut);
        }

        latch.countDown();

        int totalCalledCnt = 0;

        for (IgniteInternalFuture<Integer> future : futures)
            totalCalledCnt += future.get();

        IgniteCache<Object, Object> cache = loc.cache("dynamicCache");

        assertEquals(entries, cache.size());
        assertEquals(entries, totalCalledCnt);

        for (int i = 0; i < entries; i++)
            assertEquals("value" + i, cache.get(i));
    }
}
