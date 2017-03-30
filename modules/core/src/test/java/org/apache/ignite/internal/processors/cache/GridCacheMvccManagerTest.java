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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.TestCase;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;

/**
 * Simple tests for {@link GridCacheVersionManager}.
 */
public class GridCacheMvccManagerTest extends TestCase {

    /**
     * Test for for {@link GridCacheMvccManager#nextAtomicId()}.
     */
    public void testNextAtomicIdMonotonicalGrows() {
        GridCacheMvccManager mgr = new GridCacheMvccManager();
        int n = GridCacheMvccManager.THREAD_RESERVE_SIZE;

        for (int i = 1; i < n * 3 + 5; i++) {
            long l = mgr.nextAtomicId();
            assertEquals(i, l);
        }

        assertEquals(n * 4, mgr.globalAtomicCnt.get());
    }

    /**
     * Test for for {@link GridCacheMvccManager#nextAtomicId()} with multiple threads.
     *
     * @throws InterruptedException if fails.
     */
    public void testNextAtomicMultiThread() throws InterruptedException {
        int threadsNum = 3;

        final GridCacheMvccManager mgr = new GridCacheMvccManager();
        final int n = GridCacheMvccManager.THREAD_RESERVE_SIZE;
        final int perThreadNum = n * 2 + 10;

        final int[] vals = new int[n * threadsNum * (perThreadNum / n + 1)];

        ExecutorService executorService = Executors.newFixedThreadPool(threadsNum);

        final CountDownLatch latch = new CountDownLatch(threadsNum);

        for (int i = 0; i < threadsNum; i++) {
            executorService.submit(new Runnable() {
                @Override public void run() {
                    for (int i = 0; i < perThreadNum; i++) {
                        long l = mgr.nextAtomicId();
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
