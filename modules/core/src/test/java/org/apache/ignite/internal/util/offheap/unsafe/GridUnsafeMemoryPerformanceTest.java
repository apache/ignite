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

package org.apache.ignite.internal.util.offheap.unsafe;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridUnsafeMemoryPerformanceTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testGuardedOpsPerformance() throws Exception {
        final GridUnsafeGuard guard = new GridUnsafeGuard();

        final AtomicInteger i = new AtomicInteger();

        final AtomicBoolean run = new AtomicBoolean(true);

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int x = 0;

                while (run.get()) {
                    guard.begin();
                    guard.end();

                    x++;
                }

                i.addAndGet(x);
            }
        }, 4);

        int time = 60;

        Thread.sleep(time * 1000);

        run.set(false);

        fut.get();

        X.println("Op/sec: " + (float) i.get() / time);
    }
}
