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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR;

public class AsyncCheckpointer {

    /** Checkpoint runner thread name. */
    public static final String CHECKPOINT_RUNNER = "checkpoint-runner";
    private volatile ForkJoinPool pageQuickSortPool;

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

    private static ForkJoinTask<Integer> splitAndSortCpPagesIfNeeded3(
        ForkJoinPool pool,
        IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> cpPagesTuple,
        ConcurrentLinkedQueue<FullPageId[]> queue) {
        FullPageId[] pageIds = CheckpointScope.pagesToArray(cpPagesTuple);

        final QuickSortRecursiveTask task = new QuickSortRecursiveTask(pageIds, SEQUENTIAL_CP_PAGE_COMPARATOR, queue);
        final ForkJoinTask<Integer> submit = pool.submit(task);

        return submit;
    }

    public void shutdownCheckpointer() {
        final ForkJoinPool fjPool = pageQuickSortPool;
        if (fjPool != null)
            fjPool.shutdownNow();

        if (asyncRunner != null) {
            asyncRunner.shutdownNow();

            try {
                asyncRunner.awaitTermination(2, TimeUnit.MINUTES);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }

        if (fjPool != null) {
            try {
                fjPool.awaitTermination(2, TimeUnit.SECONDS);
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public ForkJoinTask<Integer> splitAndSortCpPagesIfNeeded3(
        IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> tuple,
        ConcurrentLinkedQueue<FullPageId[]> queue) {
        if (pageQuickSortPool == null) {
            synchronized (this) {
                if (pageQuickSortPool == null)
                    pageQuickSortPool = new ForkJoinPool();
            }
        }
        return splitAndSortCpPagesIfNeeded3(pageQuickSortPool, tuple, queue);
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

    public Future<CountDownFuture> lazySubmit(ForkJoinTask<Integer> cpPagesChunksCountFuture,
        ConcurrentLinkedQueue<FullPageId[]> queue,
        IgniteBiClosure<FullPageId[], Future<CountDownFuture>, Runnable> factory) {

        Future<CountDownFuture> wrCompleteFuture = new ChainedFuture<>(cpPagesChunksCountFuture,
            new IgniteClosure<Integer, CountDownFuture>() {
                @Override public CountDownFuture apply(Integer integer) {
                    return new CountDownFuture(integer);
                }
            });
        while (true) {
            final FullPageId[] poll = queue.poll();
            if (poll != null) {
                final Runnable runnable = factory.apply(poll, wrCompleteFuture);
                if (log.isInfoEnabled())
                    log.info("Scheduling " + poll.length + " pages write");

                execute(runnable);
            }
            else
                LockSupport.parkNanos(1);

            if (cpPagesChunksCountFuture.isDone())
                break;
        }
        return wrCompleteFuture;
    }

    public static class QuickSortRecursiveTask extends RecursiveTask<Integer> {
        /** Source array to sort. */
        private final FullPageId[] array;
        /** Start position. Index of first element inclusive. */
        private final int position;
        /** Limit. Index of last element exclusive. */
        private final int limit;

        private boolean rootTask;
        private Comparator<FullPageId> comp;
        @Nullable private final ConcurrentLinkedQueue<FullPageId[]> queue;

        public QuickSortRecursiveTask(FullPageId[] arr,
            Comparator<FullPageId> comp,
            @Nullable ConcurrentLinkedQueue<FullPageId[]> queue) {
            this(arr, 0, arr.length, comp, queue);
            this.rootTask = true;
        }

        private QuickSortRecursiveTask(FullPageId[] arr, int position, int limit,
            Comparator<FullPageId> comp,
            @Nullable ConcurrentLinkedQueue<FullPageId[]> queue) {
            this.array = arr;
            this.position = position;
            this.limit = limit;
            this.comp = comp;
            this.queue = queue;
        }

        @Override protected Integer compute() {
            final int remaining = limit - position;
            if (isUnderThreshold(remaining)) {
                Arrays.sort(array, position, limit, comp);
                if (false)
                    System.err.println("Sorted [" + remaining + "] in " + Thread.currentThread().getName());

                if (queue != null) {
                    final FullPageId[] ids = Arrays.copyOfRange(array, position, limit);
                    final boolean res = queue.offer(ids);
                    assert res;
                }

                return 1; // one chunk was produced
            }
            else {
                int centerIndex = partition2(array, position, limit, comp);
                if (false)
                    System.err.println("centerIndex=" + centerIndex);
                QuickSortRecursiveTask t1 = new QuickSortRecursiveTask(array, position, centerIndex, comp, queue);
                QuickSortRecursiveTask t2 = new QuickSortRecursiveTask(array, centerIndex, limit, comp, queue);
                t1.fork();
                final Integer t2Result = t2.compute();
                final Integer t1Result = t1.join();
                return t1Result + t2Result;
            }
        }

        public static boolean isUnderThreshold(int cnt) {
            return cnt < 1024 * 16;
        }

        int partition(FullPageId[] arr, int position, int limit,
            Comparator<FullPageId> comp) {
            if (false)
                System.err.println("Partition from " + position + " to " + limit); //todo remove
            int i = position;
            FullPageId x = arr[limit - 1];
            for (int j = position; j < limit - 1; j++) {
                if (comp.compare(arr[j], x) < 0) {
                    swap(arr, i, j);
                    i++;
                }
            }
            swap(arr, i, limit - 1);
            return i;
        }

        static int partition2(FullPageId[] arr, int position, int limit,
            Comparator<FullPageId> comp) {
            int left = position;
            int right = limit - 1;
            final int randomIdx = (limit - position) / 2 + position;
            FullPageId referenceElement = arr[randomIdx]; // taking middle element as reference

            while (left <= right) {
                //searching number which is greater than reference
                while (comp.compare(arr[left], referenceElement) < 0)
                    left++;
                //searching number which is less than reference
                while (comp.compare(arr[right], referenceElement) > 0)
                    right--;

                // swap the values
                if (left <= right) {
                    FullPageId tmp = arr[left];
                    arr[left] = arr[right];
                    arr[right] = tmp;

                    //increment left index and decrement right index
                    left++;
                    right--;
                }
            }
            return left;
        }

        void swap(FullPageId[] arr, int i, int j) {
            FullPageId tmp = arr[i];
            arr[i] = arr[j];
            arr[j] = tmp;
        }

    }
}
