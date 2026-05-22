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

package org.apache.ignite.internal.util.worker.queue;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextSnapshot;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareWrapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.thread.IgniteThread.GRP_IDX_UNASSIGNED;

/**
 * Represents a single-threaded, asynchronous queue elements handler. It automatically captures the {@link OperationContext}
 * attached to the thread that submitted the item for handling and restores it before handling actually begins in the
 * worker thread.
 *
 * @param <T> Type of items to be processed.
 * @param <W> Type of wrapper over processing item that are stored in the underlying queue.
 */
abstract class AsyncQueueHandler<T, W extends OperationContextAwareWrapper<T>> extends GridWorker {
    /** */
    private final BlockingQueue<W> workerQueue;

    /** */
    private Thread workerThread;

    /** */
    protected AsyncQueueHandler(
        @Nullable String igniteInstanceName,
        String workerThreadName,
        IgniteLogger log,
        @Nullable WorkersRegistry workerReg,
        BlockingQueue<W> workerQueue
    ) {
        super(igniteInstanceName, workerThreadName, log, workerReg);

        this.workerQueue = workerQueue;
    }

    /** */
    protected abstract W wrapQueueElement(T delegate, OperationContextSnapshot snapshot);

    /** */
    protected Thread.UncaughtExceptionHandler uncaughtExceptionHandler() {
        return null;
    }

    /** */
    protected IgniteThread createWorkerThread(GridWorker worker) {
        return new IgniteThread(igniteInstanceName(), name(), worker, GRP_IDX_UNASSIGNED, -1, GridIoPolicy.UNDEFINED);
    }

    /** */
    @Nullable protected OperationContextAwareWrapper<T> takeQueuedElement() throws InterruptedException {
        blockingSectionBegin();

        try {
            return workerQueue.take();
        }
        finally {
            blockingSectionEnd();
        }
    }

    /** */
    @Nullable protected OperationContextAwareWrapper<T> pollQueuedElement(
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException {
        blockingSectionBegin();

        try {
            return workerQueue.poll(timeout, unit);
        }
        finally {
            blockingSectionEnd();
        }
    }

    /** */
    public void start() {
        synchronized (this) {
            if (workerThread != null)
                return;

            workerThread = createWorkerThread(this);

            Thread.UncaughtExceptionHandler errHnd = uncaughtExceptionHandler();

            if (errHnd != null)
                workerThread.setUncaughtExceptionHandler(errHnd);

            workerThread.start();
        }
    }

    /** {@inheritDoc} */
    @Override protected void cleanup() {
        synchronized (this) {
            workerThread = null;
        }
    }

    /** */
    public boolean addToQueue(@NotNull T t) {
        assert !OperationContextAwareWrapper.class.isAssignableFrom(t.getClass());

        return workerQueue.add(wrapQueueElement(t, OperationContext.createSnapshot()));
    }

    /** */
    public boolean removeQueuedElement(Object o) {
        return workerQueue.removeIf(w -> o.equals(w.delegate()));
    }

    /** */
    @NotNull public Iterable<T> queuedElements() {
        return new Iterable<>() {
            @Override public @NotNull Iterator<T> iterator() {
                return F.iterator(workerQueue, OperationContextAwareWrapper::delegate, true);
            }
        };
    }

    /** */
    public void clearQueue() {
        workerQueue.clear();
    }

    /** */
    public int queueSize() {
        return workerQueue.size();
    }

    /** */
    public boolean isQueueEmpty() {
        return workerQueue.isEmpty();
    }

    /** */
    public void drainQueue(Consumer<? super T> consumer) {
        W element;

        while (true) {
            element = workerQueue.poll();

            if (element == null)
                break;

            consumer.accept(element.delegate());
        }
    }
}
