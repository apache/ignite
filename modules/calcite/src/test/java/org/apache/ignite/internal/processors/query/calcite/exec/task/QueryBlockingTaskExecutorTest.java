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

package org.apache.ignite.internal.processors.query.calcite.exec.task;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.NoOpIgniteSecurityProcessor;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class QueryBlockingTaskExecutorTest extends GridCommonAbstractTest {
    /** Tests that tasks for different queries can be executed concurrently. */
    @Test
    public void testConcurrentTasks() throws Exception {
        GridTestKernalContext ctx = newContext(new IgniteConfiguration().setQueryThreadPoolSize(10));
        ctx.add(new NoOpIgniteSecurityProcessor(ctx));
        QueryBlockingTaskExecutor executor = new QueryBlockingTaskExecutor(ctx);
        executor.onStart(ctx);

        CountDownLatch latch = new CountDownLatch(1);

        AtomicInteger cnt = new AtomicInteger();
        AtomicBoolean fail = new AtomicBoolean();

        UUID qryId1 = UUID.randomUUID();
        UUID qryId2 = UUID.randomUUID();

        Runnable task = () -> {
            cnt.incrementAndGet();

            try {
                if (!latch.await(1_000L, TimeUnit.MILLISECONDS))
                    fail.set(true);
            }
            catch (InterruptedException e) {
                fail.set(true);
            }
        };

        executor.execute(qryId1, 0, task);
        executor.execute(qryId1, 1, task);
        executor.execute(qryId2, 0, task);

        assertTrue("Failed to wait for tasks completion",
            GridTestUtils.waitForCondition(() -> cnt.get() == 3, 1_000L));

        latch.countDown();

        assertFalse("Failed to wait for latch", fail.get());
    }

    /** Tests that tasks for the same query can't be executed concurrently. */
    @Test
    public void testSameQueryTasks() throws Exception {
        GridTestKernalContext ctx = newContext(new IgniteConfiguration().setQueryThreadPoolSize(10));
        ctx.add(new NoOpIgniteSecurityProcessor(ctx));
        QueryBlockingTaskExecutor executor = new QueryBlockingTaskExecutor(ctx);
        executor.onStart(ctx);

        int qryCnt = 20;
        int taskCnt = 10_000;

        UUID[] qryIds = new UUID[qryCnt];
        AtomicBoolean[] blocked = new AtomicBoolean[qryCnt];

        for (int i = 0; i < qryCnt; i += 2)
            qryIds[i] = qryIds[i + 1] = UUID.randomUUID();

        for (int i = 0; i < qryCnt; i++)
            blocked[i] = new AtomicBoolean();

        AtomicInteger cnt = new AtomicInteger();
        AtomicBoolean fail = new AtomicBoolean();

        for (int i = 0; i < taskCnt; i++) {
            int qryIdx = ThreadLocalRandom.current().nextInt(qryCnt);

            executor.execute(qryIds[qryIdx], qryIdx, () -> {
                if (!blocked[qryIdx].compareAndSet(false, true))
                    fail.set(true);

                doSleep(ThreadLocalRandom.current().nextLong(10L));

                blocked[qryIdx].set(false);

                cnt.incrementAndGet();
            });
        }

        assertTrue("Failed to wait for tasks completion",
            GridTestUtils.waitForCondition(() -> cnt.get() == taskCnt, getTestTimeout()));

        assertFalse("Tasks for the same query executed concurrently", fail.get());
    }
}
