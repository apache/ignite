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

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR;

/**
 * Asynchronous Checkpointer, encapsulates thread pool functionality and allows
 */
public class AsyncCheckpointer {
    /** Checkpoint runner thread name prefix. */
    public static final String CHECKPOINT_RUNNER = "checkpoint-runner";

    /** Blocking queue. */
    private final LinkedBlockingQueue<Runnable> blockingQueue;

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    private ThreadPoolExecutor asyncRunner;

    /**  Number of checkpoint threads. */
    private int checkpointThreads;

    /** Logger. */
    private IgniteLogger log;

    /**
     * @param checkpointThreads Number of checkpoint threads.
     * @param igniteInstanceName Ignite instance name.
     * @param log Logger.
     */
    public AsyncCheckpointer(int checkpointThreads, String igniteInstanceName, IgniteLogger log) {
        this.checkpointThreads = checkpointThreads;
        this.log = log;

        blockingQueue = new LinkedBlockingQueue<>();
        asyncRunner = new IgniteThreadPoolExecutor(
            CHECKPOINT_RUNNER,
            igniteInstanceName,
            checkpointThreads,
            checkpointThreads,
            30_000,
            blockingQueue
        );
    }

    /**
     * Close async checkpointer, stops all thread from pool
     */
    public void shutdownCheckpointer() {
        asyncRunner.shutdownNow();

        try {
            asyncRunner.awaitTermination(2, TimeUnit.MINUTES);
        }
        catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Executes the given runnable in thread pool.
     *
     * @param runnable task to run.
     */
    private void execute(Runnable runnable) {
        try {
            asyncRunner.execute(runnable);
        }
        catch (RejectedExecutionException ignore) {
            // Run the task synchronously.
            runnable.run();
        }
    }

    /**
     * Executes the given runnable in thread pool.
     *
     * @param task task to run.
     * @param doneReportFut Count down future to report this runnable completion.
     */
    private void execute(Callable<Void> task, CountDownFuture doneReportFut) {
        execute(wrapRunnableWithDoneReporting(task, doneReportFut));
    }

    /**
     * @param task actual callable performing required action.
     * @param doneReportFut Count down future to report this runnable completion.
     * @return wrapper runnable which will report result to {@code doneReportFut}
     */
    private static Runnable wrapRunnableWithDoneReporting(final Callable<Void> task,
        final CountDownFuture doneReportFut) {
        return new Runnable() {
            @Override public void run() {
                try {
                    task.call();

                    doneReportFut.onDone((Void)null); // success
                }
                catch (Throwable t) {
                    doneReportFut.onDone(t); //reporting error
                }
            }
        };
    }

    /**
     * @param cpScope Checkpoint scope, contains unsorted collections.
     * @param taskFactory write pages task factory. Should provide callable to write given pages array.
     * @return future will be completed when background writing is done.
     */
    public CheckpointFsyncScope quickSortAndWritePages(CheckpointScope cpScope,
        BiFunction<FullPageIdsBuffer, ConcurrentHashMap<PageStore, LongAdder>, Callable<Void>> taskFactory) {

        final ForkNowForkLaterStrategy stgy = () -> {
            if (asyncRunner.getActiveCount() < checkpointThreads) {
                if (log.isTraceEnabled())
                    log.trace("Need to fill pool by computing tasks, fork now");

                return true; // need to fill the pool
            }

            if (blockingQueue.size() < checkpointThreads) {
                if (log.isTraceEnabled())
                    log.trace("Need to fill queue by computing tasks 1 for each thread, fork now");

                return true; // need to fill the queue
            }

            return false; // sufficient queue and pool is already busy, don't flood queue
        };

        CheckpointFsyncScope scope = new CheckpointFsyncScope();
        cpScope.independentSets().forEach(set -> {
            CheckpointFsyncScope.Stripe stripeScope = scope.newStripe();

            IgniteInClosure<Callable<Void>> submitter
                = callable -> fork(callable, stripeScope.future);

            IgniteClosure<FullPageIdsBuffer, Callable<Void>> factoryForStripe
                = buffer -> taskFactory.apply(buffer, stripeScope.fsyncScope);

            submitter.apply(new QuickSortRecursiveTask(set,
                SEQUENTIAL_CP_PAGE_COMPARATOR,
                factoryForStripe,
                submitter,
                stgy));
        });

        return scope;
    }

    /**
     * Executes the given runnable in thread pool, registers future to be waited.
     *
     * @param task task to run.
     * @param cntDownDynamicFut Count down future to register job and then report this runnable completion.
     */
    public void fork(Callable<Void> task, CountDownDynamicFuture cntDownDynamicFut) {
        cntDownDynamicFut.incrementTasksCount(); // for created task about to be forked

        execute(task, cntDownDynamicFut);
    }
}
