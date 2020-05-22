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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.processors.cache.persistence.DataStructure;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.HEAP_STACK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.OFF_HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.OFF_HEAP_STACK;

/**
 *
 */
public class PageLockTrackerManagerTest {
    /**
     *
     */
    @Test
    public void testDisableTracking() {
        System.setProperty("IGNITE_PAGE_LOCK_TRACKER_TYPE", String.valueOf(-1));

        try {
            PageLockTrackerManager mgr = new PageLockTrackerManager(new ListeningTestLogger());

            PageLockListener pll = mgr.createPageLockTracker("test");

            Assert.assertNotNull(pll);
            Assert.assertSame(pll, DataStructure.NOOP_LSNR);

        } finally {
            System.clearProperty("IGNITE_PAGE_LOCK_TRACKER_TYPE");
        }

        System.setProperty("IGNITE_PAGE_LOCK_TRACKER_TYPE", String.valueOf(HEAP_LOG));

        try {
            PageLockTrackerManager mgr = new PageLockTrackerManager(new ListeningTestLogger());

            PageLockListener pll = mgr.createPageLockTracker("test");

            Assert.assertNotNull(pll);
            Assert.assertNotSame(pll, DataStructure.NOOP_LSNR);

        } finally {
            System.clearProperty("IGNITE_PAGE_LOCK_TRACKER_TYPE");
        }
    }

    /**
     *
     */
    @Test
    public void testMemoryCalculation() throws Exception {
        int[] trackerTypes = new int[] {HEAP_STACK, HEAP_LOG, OFF_HEAP_STACK, OFF_HEAP_LOG};

        for (int type : trackerTypes)
            testMemoryCalculation0(type);
    }

    /**
     * @param type Tracker type.
     */
    public void testMemoryCalculation0(int type) throws Exception {
        System.out.println(">>> Calculation mempory tracker type:" + type);

        int timeOutWorkerInterval = 10_000;

        System.setProperty("IGNITE_PAGE_LOCK_TRACKER_TYPE", String.valueOf(type));
        System.setProperty("IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL", String.valueOf(timeOutWorkerInterval));

        try {
            PageLockTrackerManager mgr = new PageLockTrackerManager(new ListeningTestLogger());

            mgr.start();

            printOverhead(mgr);

            long heapOverhead0 = mgr.getHeapOverhead();
            long offHeapOverhead0 = mgr.getOffHeapOverhead();
            long totalOverhead0 = mgr.getTotalOverhead();

            Assert.assertTrue(heapOverhead0 > 0);
            Assert.assertTrue(offHeapOverhead0 >= 0);
            Assert.assertEquals(heapOverhead0 + offHeapOverhead0, totalOverhead0);

            PageLockListener pls = mgr.createPageLockTracker("test");

            printOverhead(mgr);

            long heapOverhead1 = mgr.getHeapOverhead();
            long offHeapOverhead1 = mgr.getOffHeapOverhead();
            long totalOverhead1 = mgr.getTotalOverhead();

            Assert.assertTrue(heapOverhead1 > 0);
            Assert.assertTrue(offHeapOverhead1 >= 0);
            Assert.assertTrue(heapOverhead1 > heapOverhead0);
            Assert.assertTrue(offHeapOverhead1 >= offHeapOverhead0);
            Assert.assertEquals(heapOverhead1 + offHeapOverhead1, totalOverhead1);

            int threads = 2_000;

            int cacheId = 1;
            long pageId = 2;
            long page = 3;
            long pageAdder = 4;

            List<Thread> threadsList = new ArrayList<>(threads);

            String threadNamePreffix = "my-thread-";

            CountDownLatch startThreadsLatch = new CountDownLatch(threads);
            CountDownLatch finishThreadsLatch = new CountDownLatch(1);

            for (int i = 0; i < threads; i++) {
                Thread th = new Thread(() -> {
                    startThreadsLatch.countDown();

                    pls.onBeforeReadLock(cacheId, pageId, page);

                    pls.onReadLock(cacheId, pageId, page, pageAdder);

                    pls.onReadUnlock(cacheId, pageId, page, pageAdder);

                    try {
                        finishThreadsLatch.await();
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }
                });

                th.setName(threadNamePreffix + i);

                threadsList.add(th);

                th.start();

                System.out.println(">>> start thread:" + th.getName());
            }

            startThreadsLatch.await();

            printOverhead(mgr);

            long heapOverhead2 = mgr.getHeapOverhead();
            long offHeapOverhead2 = mgr.getOffHeapOverhead();
            long totalOverhead2 = mgr.getTotalOverhead();

            Assert.assertTrue(heapOverhead2 > heapOverhead1);
            Assert.assertTrue(offHeapOverhead2 >= offHeapOverhead1);
            Assert.assertEquals(heapOverhead2 + offHeapOverhead2, totalOverhead2);

            finishThreadsLatch.countDown();

            threadsList.forEach(th -> {
                try {
                    System.out.println(">>> await thread:" + th.getName());

                    th.join();
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }
            });

            // Await cleanup worker interval.
            U.sleep(2 * timeOutWorkerInterval);

            printOverhead(mgr);

            long heapOverhead3 = mgr.getHeapOverhead();
            long offHeapOverhead3 = mgr.getOffHeapOverhead();
            long totalOverhead3 = mgr.getTotalOverhead();

            Assert.assertTrue(heapOverhead3 > 0);
            Assert.assertTrue(offHeapOverhead3 >= 0);
            Assert.assertTrue(heapOverhead3 < heapOverhead2);
            Assert.assertTrue(offHeapOverhead3 <= offHeapOverhead2);
            Assert.assertEquals(heapOverhead3 + offHeapOverhead3, totalOverhead3);

            mgr.stop();
        }
        finally {
            System.clearProperty("IGNITE_PAGE_LOCK_TRACKER_TYPE");
            System.clearProperty("IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL");
        }
    }

    private void printOverhead(PageLockTrackerManager mgr) {
        System.out.println(
            "Head:" + mgr.getHeapOverhead()
                + ", OffHeap:" + mgr.getOffHeapOverhead()
                + ", Total:" + mgr.getTotalOverhead());
    }
}
