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
package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.AsyncCheckpointer;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointScope;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.logger.NullLogger;
import org.junit.AfterClass;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.SplitAndSortCpPagesTest.getTestCollection;
import static org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.SplitAndSortCpPagesTest.validateOrder;
import static org.junit.Assert.assertEquals;

public class AsyncCpSplitCpPagesUnitTest {

    private static final ForkJoinPool pool = new ForkJoinPool();

    @AfterClass
    public static void close() {
        pool.shutdown();
        try {
            if(!pool.awaitTermination(1, TimeUnit.SECONDS))
                pool.shutdownNow();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAsyncLazyCpPagesSubmit() throws ExecutionException, InterruptedException, IgniteCheckedException {
        final CheckpointScope scope = getTestCollection();

        final AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(6, getClass().getSimpleName(), new NullLogger());

        final BlockingQueue<FullPageId[]> queue = asyncCheckpointer != null ? new LinkedBlockingQueue<FullPageId[]>() : null;
        final ForkJoinTask<Integer> task
            = asyncCheckpointer.splitAndSortCpPagesIfNeeded3(scope, queue);
        final AtomicInteger totalPagesAfterSort = new AtomicInteger();
        final CountDownFuture fut = asyncCheckpointer.lazySubmit(task, queue,
            new IgniteClosure<FullPageId[], Callable<Void>>() {
                @Override public Callable<Void> apply(final FullPageId[] ids) {
                    return new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            final int length = ids.length;
                            totalPagesAfterSort.addAndGet(length);
                            validateOrder(Arrays.asList(ids), length);
                            return null;
                        }
                    };
            }
        });

        fut.get();
        assertEquals(totalPagesAfterSort.get(), scope.totalCpPages());

    }
}
