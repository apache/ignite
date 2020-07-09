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

package org.apache.ignite.internal.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;

import static java.util.stream.IntStream.range;

/**
 * Striped executor.
 */
public class StripedExecutor implements ExecutorService {
    /** Stripes. */
    private final Stripe[] stripes;

    /** Threshold for starvation checks */
    private final long threshold;

    /** */
    private final IgniteLogger log;

    /**
     * @param cnt Count.
     * @param igniteInstanceName Node name.
     * @param poolName Pool name.
     * @param log Logger.
     * @param errHnd Critical failure handler.
     * @param gridWorkerLsnr Listener to link with every stripe worker.
     */
    public StripedExecutor(
        int cnt,
        String igniteInstanceName,
        String poolName,
        final IgniteLogger log,
        IgniteInClosure<Throwable> errHnd,
        GridWorkerListener gridWorkerLsnr,
        long failureDetectionTimeout
    ) {
        this(cnt, igniteInstanceName, poolName, log, errHnd, false, gridWorkerLsnr, failureDetectionTimeout);
    }

    /**
     * @param cnt Count.
     * @param igniteInstanceName Node name.
     * @param poolName Pool name.
     * @param log Logger.
     * @param errHnd Critical failure handler.
     * @param stealTasks {@code True} to steal tasks.
     * @param gridWorkerLsnr listener to link with every stripe worker.
     */
    public StripedExecutor(
        int cnt,
        String igniteInstanceName,
        String poolName,
        final IgniteLogger log,
        IgniteInClosure<Throwable> errHnd,
        boolean stealTasks,
        GridWorkerListener gridWorkerLsnr,
        long failureDetectionTimeout
    ) {
        A.ensure(cnt > 0, "cnt > 0");

        boolean success = false;

        stripes = new Stripe[cnt];

        threshold = failureDetectionTimeout;

        this.log = log;

        try {
            for (int i = 0; i < cnt; i++) {
                stripes[i] = stealTasks
                    ? new StripeConcurrentQueue(igniteInstanceName, poolName, i, log, stripes, errHnd, gridWorkerLsnr)
                    : new StripeConcurrentQueue(igniteInstanceName, poolName, i, log, errHnd, gridWorkerLsnr);
            }

            for (int i = 0; i < cnt; i++)
                stripes[i].start();

            success = true;
        }
        catch (Error | RuntimeException e) {
            U.error(log, "Failed to initialize striped pool.", e);

            throw e;
        }
        finally {
            if (!success) {
                for (Stripe stripe : stripes)
                    U.cancel(stripe);

                for (Stripe stripe : stripes)
                    U.join(stripe, log);
            }
        }
    }

    /**
     * Checks starvation in striped pool. Maybe too verbose
     * but this is needed to faster debug possible issues.
     *
     * @return Flag representing presence of possible starvation in striped pool.
     */
    public boolean detectStarvation() {
        boolean starvationDetected = false;

        for (Stripe stripe : stripes) {
            boolean active = stripe.active;

            long lastStartedTs = stripe.lastStartedTs;

            if (active && lastStartedTs + threshold < U.currentTimeMillis()) {
                starvationDetected = true;
                boolean deadlockPresent = U.deadlockPresent();

                GridStringBuilder sb = new GridStringBuilder();

                sb.a(">>> Possible starvation in striped pool.").a(U.nl())
                    .a("    Thread name: ").a(stripe.thread.getName()).a(U.nl())
                    .a("    Queue: ").a(stripe.queueToString()).a(U.nl())
                    .a("    Deadlock: ").a(deadlockPresent).a(U.nl())
                    .a("    Completed: ").a(stripe.completedCnt).a(U.nl());

                U.printStackTrace(
                    stripe.thread.getId(),
                    sb);

                String msg = sb.toString();

                U.warn(log, msg);
            }
        }
        return starvationDetected;
    }

    /**
     * @return Stripes count.
     */
    public int stripesCount() {
        return stripes.length;
    }

    /**
     * @return Stripes of this executor.
     */
    public Stripe[] stripes() {
        return stripes;
    }

    /**
     * Execute command.
     *
     * @param idx Index.
     * @param cmd Command.
     */
    public void execute(int idx, Runnable cmd) {
        if (idx == -1)
            execute(cmd);
        else {
            assert idx >= 0 : idx;

            stripes[idx % stripes.length].execute(cmd);
        }
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        signalStop();
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable cmd) {
        stripes[ThreadLocalRandom.current().nextInt(stripes.length)].execute(cmd);
    }

    /**
     * {@inheritDoc}
     *
     * @return Empty list (always).
     */
    @NotNull @Override public List<Runnable> shutdownNow() {
        signalStop();

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException {
        awaitStop();

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        for (Stripe stripe : stripes) {
            if (stripe != null && stripe.isCancelled())
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        for (Stripe stripe : stripes) {
            if (stripe.thread.getState() != Thread.State.TERMINATED)
                return false;
        }

        return true;
    }

    /**
     * Stops executor.
     */
    public void stop() {
        signalStop();

        awaitStop();
    }

    /**
     * Signals all stripes.
     */
    private void signalStop() {
        for (Stripe stripe : stripes)
            U.cancel(stripe);
    }

    /**
     * Waits for all stripes to stop.
     */
    private void awaitStop() {
        for (Stripe stripe : stripes)
            U.join(stripe, log);
    }

    /**
     * @return Return total queue size of all stripes.
     */
    public int queueSize() {
        int size = 0;

        for (Stripe stripe : stripes)
            size += stripe.queueSize();

        return size;
    }

    /**
     * @param idx Stripe index.
     * @return Queue size of specific stripe.
     */
    public int queueStripeSize(int idx) {
        A.ensure(idx >= 0, "Stripe index should be non-negative: " + idx);

        return stripes[idx % stripes.length].queueSize();
    }

    /**
     * @return Completed tasks count.
     */
    public long completedTasks() {
        long cnt = 0;

        for (Stripe stripe : stripes)
            cnt += stripe.completedCnt;

        return cnt;
    }

    /**
     * @return Completed tasks per stripe count.
     */
    public long[] stripesCompletedTasks() {
        long[] res = new long[stripesCount()];

        for (int i = 0; i < res.length; i++)
            res[i] = stripes[i].completedCnt;

        return res;
    }

    /**
     * @return Number of active tasks per stripe.
     */
    public boolean[] stripesActiveStatuses() {
        boolean[] res = new boolean[stripesCount()];

        for (int i = 0; i < res.length; i++)
            res[i] = stripes[i].active;

        return res;
    }

    /**
     * @return Number of active tasks.
     */
    public int activeStripesCount() {
        int res = 0;

        for (boolean status : stripesActiveStatuses()) {
            if (status)
                res++;
        }

        return res;
    }

    /**
     * @return Size of queue per stripe.
     */
    public int[] stripesQueueSizes() {
        int[] res = new int[stripesCount()];

        for (int i = 0; i < res.length; i++)
            res[i] = stripes[i].queueSize();

        return res;
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> Future<T> submit(
        @NotNull Runnable task,
        T res
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public Future<?> submit(@NotNull Runnable task) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> Future<T> submit(@NotNull Callable<T> task) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> List<Future<T>> invokeAll(
        @NotNull Collection<? extends Callable<T>> tasks,
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @Override public <T> T invokeAny(
        @NotNull Collection<? extends Callable<T>> tasks,
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    /**
     * Method for await all task completion in some specific striped indexes or all.
     * The method guarantees what all tasks which were passed before this call
     * will be completed before method return control.
     *
     * @param stripes Striped idxs for await. Empty params means await all stripes.
     */
    public void awaitComplete(int... stripes) throws InterruptedException {
        CountDownLatch awaitLatch;

        if (stripes.length == 0) {
            awaitLatch = new CountDownLatch(stripesCount());

            // We have to ensure that all asynchronous updates are done.
            // StripedExecutor guarantees ordering inside stripe - it would enough to await "finishing" tasks.
            range(0, stripesCount()).forEach(idx -> execute(idx, awaitLatch::countDown));
        }
        else {
            awaitLatch = new CountDownLatch(stripes.length);

            for (int idx : stripes)
                execute(idx, awaitLatch::countDown);
        }

        while (true) {
            if (awaitLatch.await(60, TimeUnit.SECONDS))
                break;
            else {
                U.log(log, "Await stripes executor complete tasks" +
                    ", awaitLatch=" + awaitLatch.getCount() +
                    ", stripes=" + (stripes.length == 0 ?
                    Arrays.toString(range(0, stripesCount()).toArray()) : Arrays.toString(stripes)) +
                    ", queueSize=" + Arrays.toString(stripesQueueSizes()) +
                    ", activeStatus=" + Arrays.toString(stripesActiveStatuses()));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StripedExecutor.class, this);
    }

    /**
     * Stripe.
     */
    public abstract static class Stripe extends GridWorker {
        /** */
        private final String igniteInstanceName;

        /** */
        protected final int idx;

        /** */
        private final IgniteLogger log;

        /** */
        private volatile long completedCnt;

        /** */
        private volatile boolean active;

        /** */
        private volatile long lastStartedTs;

        /** Thread executing the loop. */
        protected Thread thread;

        /** Critical failure handler. */
        private IgniteInClosure<Throwable> errHnd;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         * @param errHnd Exception handler.
         * @param gridWorkerLsnr listener to link with stripe worker.
         */
        public Stripe(
            String igniteInstanceName,
            String poolName,
            int idx,
            IgniteLogger log,
            IgniteInClosure<Throwable> errHnd,
            GridWorkerListener gridWorkerLsnr
        ) {
            super(igniteInstanceName, poolName + "-stripe-" + idx, log, gridWorkerLsnr);

            this.igniteInstanceName = igniteInstanceName;
            this.idx = idx;
            this.log = log;
            this.errHnd = errHnd;
        }

        /**
         * Starts the stripe.
         */
        void start() {
            thread = new IgniteThread(igniteInstanceName,
                name(),
                this,
                IgniteThread.GRP_IDX_UNASSIGNED,
                idx,
                GridIoPolicy.UNDEFINED);

            thread.start();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NonAtomicOperationOnVolatileField")
        @Override public void body() {
            while (!isCancelled()) {
                Runnable cmd;

                try {
                    blockingSectionBegin();

                    try {
                        cmd = take();
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    if (cmd != null) {
                        active = true;

                        lastStartedTs = U.currentTimeMillis();

                        updateHeartbeat();

                        try {
                            cmd.run();
                        }
                        finally {
                            active = false;
                            completedCnt++;
                        }
                    }

                    onIdle();
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();

                    break;
                }
                catch (Throwable e) {
                    errHnd.apply(e);

                    U.error(log, "Failed to execute runnable.", e);
                }
            }

            if (!isCancelled) {
                errHnd.apply(new IllegalStateException("Thread " + Thread.currentThread().getName() +
                    " is terminated unexpectedly"));
            }
        }

        /**
         * Execute the command.
         *
         * @param cmd Command.
         */
        abstract void execute(Runnable cmd);

        /**
         * @return Next runnable.
         * @throws InterruptedException If interrupted.
         */
        abstract Runnable take() throws InterruptedException;

        /**
         * @return Queue size.
         */
        abstract int queueSize();

        /**
         * @return Stripe's queue to string presentation.
         */
        abstract String queueToString();

        /**
         * @return Stripe queue.
         */
        public abstract Queue<Runnable> queue();

        /**
         * @return Stripe index.
         */
        public int index() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Stripe.class, this);
        }
    }

    /**
     * Stripe.
     */
    private static class StripeConcurrentQueue extends Stripe {
        /** */
        private static final int IGNITE_TASKS_STEALING_THRESHOLD =
            IgniteSystemProperties.getInteger(
                IgniteSystemProperties.IGNITE_DATA_STREAMING_EXECUTOR_SERVICE_TASKS_STEALING_THRESHOLD, 4);

        /** Queue. */
        private final Queue<Runnable> queue;

        /** */
        @GridToStringExclude
        private final Stripe[] others;

        /** */
        private volatile boolean parked;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         * @param errHnd Critical failure handler.
         * @param gridWorkerLsnr listener to link with stripe worker.
         */
        StripeConcurrentQueue(
            String igniteInstanceName,
            String poolName,
            int idx,
            IgniteLogger log,
            IgniteInClosure<Throwable> errHnd,
            GridWorkerListener gridWorkerLsnr
        ) {
            this(igniteInstanceName, poolName, idx, log, null, errHnd, gridWorkerLsnr);
        }

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         * @param errHnd Critical failure handler.
         * @param gridWorkerLsnr listener to link with stripe worker.
         */
        StripeConcurrentQueue(
            String igniteInstanceName,
            String poolName,
            int idx,
            IgniteLogger log,
            Stripe[] others,
            IgniteInClosure<Throwable> errHnd,
            GridWorkerListener gridWorkerLsnr
        ) {
            super(
                igniteInstanceName,
                poolName,
                idx,
                log,
                errHnd,
                gridWorkerLsnr);

            this.others = others;

            this.queue = others == null ? new ConcurrentLinkedQueue<Runnable>() : new ConcurrentLinkedDeque<Runnable>();
        }

        /** {@inheritDoc} */
        @Override Runnable take() throws InterruptedException {
            Runnable r;

            for (int i = 0; i < 2048; i++) {
                r = queue.poll();

                if (r != null)
                    return r;
            }

            parked = true;

            try {
                for (;;) {
                    r = queue.poll();

                    if (r != null)
                        return r;

                    if (others != null) {
                        int len = others.length;
                        int init = ThreadLocalRandom.current().nextInt(len);
                        int cur = init;

                        while (true) {
                            if (cur != idx) {
                                Deque<Runnable> queue = (Deque<Runnable>) ((StripeConcurrentQueue) others[cur]).queue;

                                if (queue.size() > IGNITE_TASKS_STEALING_THRESHOLD && (r = queue.pollLast()) != null)
                                    return r;
                            }

                            if ((cur = (cur + 1) % len) == init)
                                break;
                        }
                    }

                    LockSupport.park();

                    if (Thread.interrupted())
                        throw new InterruptedException();
                }
            }
            finally {
                parked = false;
            }
        }

        /** {@inheritDoc} */
        @Override void execute(Runnable cmd) {
            queue.add(cmd);

            if (parked)
                LockSupport.unpark(thread);

            if (others != null && queueSize() > IGNITE_TASKS_STEALING_THRESHOLD) {
                for (Stripe other : others) {
                    if (((StripeConcurrentQueue)other).parked)
                        LockSupport.unpark(other.thread);
                }
            }
        }

        /** {@inheritDoc} */
        @Override String queueToString() {
            return String.valueOf(queue);
        }

        /** {@inheritDoc} */
        @Override public Queue<Runnable> queue() {
            return queue;
        }

        /** {@inheritDoc} */
        @Override int queueSize() {
            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StripeConcurrentQueue.class, this, super.toString());
        }
    }
}
