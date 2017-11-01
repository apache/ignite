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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * Striped executor.
 */
public class StripedExecutor implements ExecutorService {
    /** */
    private static final int SPIN_CNT = 2048;

    /** Stripes. */
    private final Stripe[] stripes;

    /** For starvation checks. */
    private final long[] completedCntrs;

    /** */
    private final IgniteLogger log;

    /**
     * @param cnt Count.
     * @param igniteInstanceName Node name.
     * @param poolName Pool name.
     * @param log Logger.
     */
    public StripedExecutor(int cnt, String igniteInstanceName, String poolName, final IgniteLogger log) {
        this(cnt, igniteInstanceName, poolName, log, false);
    }

    /**
     * @param cnt Count.
     * @param igniteInstanceName Node name.
     * @param poolName Pool name.
     * @param log Logger.
     * @param stealTasks {@code True} to steal tasks.
     */
    public StripedExecutor(int cnt, String igniteInstanceName, String poolName, final IgniteLogger log,
        boolean stealTasks) {
        A.ensure(cnt > 0, "cnt > 0");

        stripes = stealTasks ? StealingStripe.create(igniteInstanceName, poolName, log, cnt) : new Stripe[cnt];

        completedCntrs = new long[cnt];

        Arrays.fill(completedCntrs, -1);

        this.log = log;

        boolean success = false;

        try {
            if(!stealTasks) {
                for (int i = 0; i < cnt; i++)
                    stripes[i] = new MpscQueueBasedStripe(igniteInstanceName, poolName, i, log);
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
                for (Stripe stripe : stripes) {
                    if (stripe != null)
                        stripe.signalStop();
                }

                for (Stripe stripe : stripes) {
                    if (stripe != null)
                        stripe.awaitStop();
                }
            }
        }
    }

    /**
     * Checks starvation in striped pool. Maybe too verbose
     * but this is needed to faster debug possible issues.
     */
    public void checkStarvation() {
        for (int i = 0; i < stripes.length; i++) {
            Stripe stripe = stripes[i];

            long completedCnt = stripe.completedCnt;

            boolean active = stripe.active;

            if (completedCntrs[i] != -1 &&
                completedCntrs[i] == completedCnt &&
                active) {
                boolean deadlockPresent = U.deadlockPresent();

                GridStringBuilder sb = new GridStringBuilder();

                sb.a(">>> Possible starvation in striped pool.").a(U.nl())
                    .a("    Thread name: ").a(stripe.thread.getName()).a(U.nl())
                    .a("    Queue: ").a(stripe.queueToString()).a(U.nl())
                    .a("    Deadlock: ").a(deadlockPresent).a(U.nl())
                    .a("    Completed: ").a(completedCnt).a(U.nl());

                U.printStackTrace(
                    stripe.thread.getId(),
                    sb);

                String msg = sb.toString();

                U.warn(log, msg);
            }

            if (active || completedCnt > 0)
                completedCntrs[i] = completedCnt;
        }
    }

    /**
     * @return Stripes count.
     */
    public int stripes() {
        return stripes.length;
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
            if (stripe != null && stripe.stopping)
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
            stripe.signalStop();
    }

    /**
     * @throws IgniteInterruptedException If interrupted.
     */
    private void awaitStop() throws IgniteInterruptedException {
        for (Stripe stripe : stripes)
            stripe.awaitStop();
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
        long[] res = new long[stripes()];

        for (int i = 0; i < res.length; i++)
            res[i] = stripes[i].completedCnt;

        return res;
    }

    /**
     * @return Number of active tasks per stripe.
     */
    public boolean[] stripesActiveStatuses() {
        boolean[] res = new boolean[stripes()];

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
        int[] res = new int[stripes()];

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StripedExecutor.class, this);
    }

    /**
     * Stripe.
     */
    private abstract static class Stripe implements Runnable {
        /** */
        private final String igniteInstanceName;

        /** */
        private final String poolName;

        /** */
        protected final int idx;

        /** */
        private final IgniteLogger log;

        /** Stopping flag. */
        private volatile boolean stopping;

        /** */
        private volatile long completedCnt;

        /** */
        private volatile boolean active;

        /** Thread executing the loop. */
        protected Thread thread;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         */
        Stripe(
            String igniteInstanceName,
            String poolName,
            int idx,
            IgniteLogger log
        ) {
            this.igniteInstanceName = igniteInstanceName;
            this.poolName = poolName;
            this.idx = idx;
            this.log = log;
        }

        /**
         * Starts the stripe.
         */
        void start() {
            thread = new IgniteThread(igniteInstanceName,
                poolName + "-stripe-" + idx,
                this,
                IgniteThread.GRP_IDX_UNASSIGNED,
                idx,
                GridIoPolicy.UNDEFINED);

            thread.start();
        }

        /**
         * Stop the stripe.
         */
        void signalStop() {
            stopping = true;

            U.interrupt(thread);
        }

        /**
         * Await thread stop.
         */
        void awaitStop() {
            try {
                if (thread != null)
                    thread.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (!stopping) {
                try {
                    Runnable cmd = take();

                    if (cmd != null) {
                        active = true;

                        try {
                            cmd.run();
                        }
                        finally {
                            active = false;
                            completedCnt++;
                        }
                    }
                }
                catch (InterruptedException ignored) {
                    stopping = true;

                    Thread.currentThread().interrupt();

                    return;
                }
                catch (Throwable e) {
                    U.error(log, "Failed to execute runnable.", e);
                }
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

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Stripe.class, this);
        }
    }

    /** */
    private static final class StealingStripe extends Stripe {
        /** */
        private static final int IGNITE_TASKS_STEALING_THRESHOLD =
            IgniteSystemProperties.getInteger(
                IgniteSystemProperties.IGNITE_DATA_STREAMING_EXECUTOR_SERVICE_TASKS_STEALING_THRESHOLD, 4);

        /** */
        @GridToStringExclude
        private final Deque<Runnable>[] queues;

        /** */
        @GridToStringExclude
        private final IgniteRunnable unpark;

        /** */
        @GridToStringExclude
        private Random rnd;

        /** */
        private final Deque<Runnable> queue;

        /** */
        private final AtomicBoolean parked = new AtomicBoolean();

        /** */
        @SuppressWarnings("unchecked")
        static Stripe[] create(String igniteInstanceName, String poolName, IgniteLogger log, final int poolSize) {
            final StealingStripe[] stripes = new StealingStripe[poolSize];
            Deque<Runnable>[] queues = new Deque[poolSize];

            IgniteRunnable unpark = new IgniteRunnable() {
                @Override public void run() {
                    int init = ThreadLocalRandom.current().nextInt(poolSize);

                    for (int cur = init;;) {
                        if (stripes[cur].parked.compareAndSet(true, false)) {
                            LockSupport.unpark(stripes[cur].thread);

                            break;
                        }

                        if ((cur = (cur + 1) % poolSize) == init)
                            break;
                    }
                }
            };

            for (int i = 0; i < poolSize; i++) {
                queues[i] = new ConcurrentLinkedDeque8<>();
                stripes[i] = new StealingStripe(i, igniteInstanceName, poolName, log, queues, unpark);
            }

            return stripes;
        }

        /**
         * @param idx Stripe index.
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param log Logger.
         * @param queues Other queues to steal tasks from.
         * @param unpark Unpark callback, unparks random parked stripe from the pool.
         */
        private StealingStripe(
            int idx,
            String igniteInstanceName,
            String poolName,
            IgniteLogger log,
            Deque<Runnable>[] queues,
            IgniteRunnable unpark
        ) {
            super(
                igniteInstanceName,
                poolName,
                idx,
                log);

            this.queues = queues;
            this.unpark = unpark;

            queue = queues[idx];
        }

        /** {@inheritDoc} */
        @Override Runnable take() throws InterruptedException {
            Runnable task;

            for (int i = 0; i < SPIN_CNT; i++) {
                if ((task = queue.poll()) != null)
                    return task;
            }

            for (;;) {
                parked.set(true);

                if ((task = queue.poll()) != null) {
                    parked.set(false);

                    return task;
                }

                int len = queues.length, init = random().nextInt(len);

                for (int cur = init;;) {
                    if(cur != idx) {
                        Deque<Runnable> queue = queues[cur];

                        if(queue.size() > IGNITE_TASKS_STEALING_THRESHOLD && (task = queue.pollLast()) != null) {
                            parked.set(false);

                            return task;
                        }
                    }

                    if ((cur = (cur + 1) % len) == init)
                        break;
                }

                LockSupport.park();

                if (Thread.interrupted())
                    throw new InterruptedException();
            }
        }

        /** {@inheritDoc} */
        @Override void execute(Runnable cmd) {
            queue.add(cmd);

            if (parked.compareAndSet(true, false))
                LockSupport.unpark(thread);
            else if(queue.size() > IGNITE_TASKS_STEALING_THRESHOLD)
                unpark.run();
        }

        /** {@inheritDoc} */
        @Override String queueToString() {
            return String.valueOf(queue);
        }

        /** {@inheritDoc} */
        @Override int queueSize() {
            return queue.size();
        }

        /** */
        private Random random() {
            return rnd == null ? rnd = ThreadLocalRandom.current() : rnd;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StealingStripe.class, this, super.toString());
        }
    }

    /** */
    private static final class MpscQueueBasedStripe extends Stripe {
        /** */
        private final AtomicBoolean parked = new AtomicBoolean();

        /** */
        private final MpscQueue<Runnable> queue = new MpscQueue<>();

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         */
        private MpscQueueBasedStripe(String igniteInstanceName, String poolName, int idx, IgniteLogger log) {
            super(igniteInstanceName, poolName, idx, log);
        }

        /** {@inheritDoc} */
        @Override void execute(Runnable cmd) {
            queue.offer(cmd);

            if (parked.get() && parked.compareAndSet(true, false))
                LockSupport.unpark(thread);
        }

        /** {@inheritDoc} */
        @Override Runnable take() throws InterruptedException {
            Runnable task;

            for (int i = 0; i < SPIN_CNT; i++) {
                if ((task = queue.poll()) != null)
                    return task;
            }

            for (;;) {
                parked.set(true);

                if ((task = queue.poll()) != null) {
                    parked.set(false);

                    return task;
                }

                LockSupport.park();

                if (Thread.interrupted())
                    throw new InterruptedException();
            }
        }

        /** {@inheritDoc} */
        @Override int queueSize() {
            return queue.size();
        }

        /** {@inheritDoc} */
        @Override String queueToString() {
            return String.valueOf(queue);
        }
    }
}
