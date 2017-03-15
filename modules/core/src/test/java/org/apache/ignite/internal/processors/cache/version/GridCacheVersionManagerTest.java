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

package org.apache.ignite.internal.processors.cache.version;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.TestCase;

/**
 * Simple tests for {@link GridCacheVersionManager}.
 */
public class GridCacheVersionManagerTest extends TestCase {
    /**
     * Test for for {@link GridCacheVersionManager#nextAtomicFutureVersion()}.
     */
    public void testNextAtomicIdMonotonicalGrows() {
        GridCacheVersionManager mgr = new GridCacheVersionManager();
        int n = GridCacheVersionManager.THREAD_RESERVE_SIZE;

        for (int i = 0; i < n * 3 + 5; i++) {
            long l = mgr.nextAtomicFutureVersion();
            assertEquals(i, l);
        }

        assertEquals(n * 4, mgr.globalAtomicCnt.get());
    }

    /**
     * Test for for {@link GridCacheVersionManager#nextAtomicFutureVersion()} with multiple threads.
     *
     * @throws InterruptedException if fails.
     */
    public void testNextAtomicMultiThread() throws InterruptedException {
        int threadsNum = 3;

        final GridCacheVersionManager mgr = new GridCacheVersionManager();
        final int n = GridCacheVersionManager.THREAD_RESERVE_SIZE;
        final int perThreadNum = n * 2 + 10;

        final int[] vals = new int[n * threadsNum * (perThreadNum / n + 1)];

        ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);

        final CountDownLatch latch = new CountDownLatch(threadsNum);

        for (int i = 0; i < threadsNum; i++) {
            executorService.submit(new Runnable() {
                @Override public void run() {
                    for (int i = 0; i < perThreadNum; i++) {
                        long l = mgr.nextAtomicFutureVersion();
                        vals[(int)l]++;
                    }
                    latch.countDown();
                }
            });
        }

        latch.await();
        executorService.shutdown();

        int cnt = 0;

        for (int i = 0; i < vals.length; i++) {
            if (vals[i] > 0)
                cnt++;
        }

        assertEquals(threadsNum * perThreadNum, cnt);
    }
}
