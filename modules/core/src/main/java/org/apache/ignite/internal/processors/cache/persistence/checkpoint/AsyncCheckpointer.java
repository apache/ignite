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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR;

public class AsyncCheckpointer {

    /** Checkpoint runner thread name prefix. */
    public static final String CHECKPOINT_RUNNER = "checkpoint-runner";

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private ExecutorService asyncRunner;
    private IgniteLogger log;

    public AsyncCheckpointer(int checkpointThreads, String igniteInstanceName, IgniteLogger log) {
        this.log = log;

        asyncRunner = new IgniteThreadPoolExecutor(
            CHECKPOINT_RUNNER,
            igniteInstanceName,
            checkpointThreads,
            checkpointThreads,
            30_000,
            new LinkedBlockingQueue<Runnable>()
        );
    }

    public void shutdownCheckpointer() {
        if (asyncRunner != null) {
            asyncRunner.shutdownNow();

            try {
                asyncRunner.awaitTermination(2, TimeUnit.MINUTES);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void execute(Runnable write) {
        try {
            asyncRunner.execute(write);
        }
        catch (RejectedExecutionException ignore) {
            // Run the task synchronously.
            write.run();
        }
    }


    public void execute(final Callable<Void> task, final CountDownFuture doneReportFut) {
        execute(wrapRunnableWithDoneReporting(task, doneReportFut));
    }

    /**
     * @param write actual runnable performing required action.
     * @param doneReportFut  Count down future to report this runnable completion stage.
     * @return wrapper runnable which will report result to {@code doneReportFut}
     */
    private static Runnable wrapRunnableWithDoneReporting(final Callable<Void> write,
        final CountDownFuture doneReportFut) {
        return new Runnable() {
            @Override public void run() {
                try {
                    write.call();

                    doneReportFut.onDone((Void)null); // success
                }
                catch (Throwable t) {
                    doneReportFut.onDone(t); //reporting error
                }
            }
        };
    }

    public CountDownFuture quickSortAndWritePages(
        CheckpointScope cpScope,
        IgniteClosure<FullPageId[], Callable<Void>> taskFactory) {

        final CountDownDynamicFuture cntDownDynamicFut = new CountDownDynamicFuture(1); // init cntr 1 protects from premature completing
        FullPageId[] pageIds = cpScope.toArray();

        final Callable<Void> task = new QuickSortRecursiveTask(pageIds, SEQUENTIAL_CP_PAGE_COMPARATOR,
            taskFactory,
            new IgniteInClosure<Callable<Void>>() {
                @Override public void apply(Callable<Void> call) {
                    fork(call, cntDownDynamicFut);
                }
            });

        fork(task, cntDownDynamicFut);

        cntDownDynamicFut.onDone((Void)null); //submit of all tasks completed

        return cntDownDynamicFut;
    }

    private void fork(Callable<Void> call, CountDownDynamicFuture cntDownDynamicFut) {
        cntDownDynamicFut.incrementTasksCount(); // for created task about to be forked

        execute(call, cntDownDynamicFut);
    }
}
