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

package org.apache.ignite.thread;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * An {@link ExecutorService} that executes submitted tasks using pooled grid threads.
 */
public class IgniteStripedThreadPoolExecutor implements ExecutorService {
    /** */
    public static final int DFLT_SEG_POOL_SIZE = 8;

    /** */
    public static final int DFLT_CONCUR_LVL = 16;

    /** */
    private final ExecutorService[] execs;

    /** */
    private final int segShift;

    /** */
    private final int segMask;

    /**
     *
     */
    public IgniteStripedThreadPoolExecutor() {
        execs = new ExecutorService[DFLT_CONCUR_LVL];

        ThreadFactory factory = new IgniteThreadFactory(null);

        for (int i = 0; i < DFLT_CONCUR_LVL; i++)
            execs[i] = Executors.newFixedThreadPool(DFLT_SEG_POOL_SIZE, factory);

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;

        while (ssize < DFLT_CONCUR_LVL) {
            ++sshift;

            ssize <<= 1;
        }

        segShift = 32 - sshift;
        segMask = ssize - 1;

    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        for (ExecutorService exec : execs)
            exec.shutdown();
    }

    /** {@inheritDoc} */
    @Override public List<Runnable> shutdownNow() {
        List<Runnable> res = new LinkedList<>();

        for (ExecutorService exec : execs) {
            for (Runnable r : exec.shutdownNow())
                res.add(r);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        for (ExecutorService exec : execs) {
            if (!exec.isShutdown())
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        for (ExecutorService exec : execs) {
            if (!exec.isTerminated())
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean res = true;

        for (ExecutorService exec : execs)
            res &= exec.awaitTermination(timeout, unit);

        return res;
    }

    /** {@inheritDoc} */
    @Override public <T> Future<T> submit(Callable<T> task) {
        return execForTask(task).submit(task);
    }

    /** {@inheritDoc} */
    @Override public <T> Future<T> submit(Runnable task, T result) {
        return execForTask(task).submit(task, result);
    }

    /** {@inheritDoc} */
    @Override public Future<?> submit(Runnable task) {
        return execForTask(task).submit(task);
    }

    /** {@inheritDoc} */
    @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
        List<Future<T>> futs = new LinkedList<>();

        for (Callable<T> task : tasks)
            futs.add(execForTask(task).submit(task));

        boolean done = false;

        try {
            for (Future<T> fut : futs) {
                try {
                    fut.get();
                }
                catch (ExecutionException | InterruptedException ignored) {
                    // No-op.
                }
            }

            done = true;

            return futs;
        }
        finally {
            if (!done) {
                for (Future<T> fut : futs)
                    fut.cancel(true);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
        TimeUnit unit) throws InterruptedException {
        throw new RuntimeException("Not implemented.");
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
        ExecutionException {
        throw new RuntimeException("Not implemented.");
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        throw new RuntimeException("Not implemented.");
    }

    /** {@inheritDoc} */
    @Override public void execute(Runnable cmd) {
        execForTask(cmd).execute(cmd);
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     *
     * @param h Hash code.
     * @return Enhanced hash code.
     */
    private int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    /**
     * @param cmd Command.
     * @return Service.
     */
    private <T> ExecutorService execForTask(T cmd) {
        assert cmd != null;

        //return execs[ThreadLocalRandom8.current().nextInt(DFLT_CONCUR_LVL)];
        return execs[(hash(cmd.hashCode()) >>> segShift) & segMask];
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteStripedThreadPoolExecutor.class, this);
    }
}