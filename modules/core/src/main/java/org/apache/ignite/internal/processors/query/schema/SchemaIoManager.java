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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for schema IO requests/responses.
 */
public class SchemaIoManager {
    /** Context. */
    private final GridKernalContext ctx;

    /** Query processor. */
    private final GridQueryProcessor qryProc;

    /** Logger. */
    private final IgniteLogger log;

    /** Lock for concurrency control. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Pending IO messages. */
    private final Queue<Object> pendingMsgs = new ConcurrentLinkedDeque<>();

    /** Whether manager is active. */
    private boolean active;

    /** Stop flag. */
    private volatile boolean stopped;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param qryProc Query processor.
     */
    public SchemaIoManager(GridKernalContext ctx, GridQueryProcessor qryProc) {
        this.ctx = ctx;
        this.qryProc = qryProc;

        log = ctx.log(SchemaIoManager.class);
    }

    /**
     * Activate manager (on node start or reconnect).
     */
    public void onStartOrReconnect() {
        lock.writeLock().lock();

        try {
            assert !active;

            active = true;

            if (!pendingMsgs.isEmpty()) {
                Queue<Object> msgs = new LinkedList<>(pendingMsgs);

                pendingMsgs.clear();

                new IgniteThread(new IoWorker(msgs)).start();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Deactivate manager (on node stop or disconnect).
     */
    public void onDisconnect() {
        lock.writeLock().lock();

        try {
            active = false;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Handle stop.
     */
    public void onStop() {
        lock.writeLock().lock();

        try {
            stopped = true;
        }
        finally {
            lock.writeLock().unlock();
        }

    }

    /**
     * Handle message arrival.
     *
     * @param msg Message.
     */
    public void onMessage(Object msg) {
        lock.readLock().lock();

        try {
            if (!active) {
                pendingMsgs.add(msg);

                return;
            }
        }
        finally {
            lock.readLock().unlock();
        }

        qryProc.processMessage(msg);
    }

    /**
     * IO worker to process pending IO messages.
     */
    private class IoWorker extends GridWorker {
        /** Messages to be processed. */
        private final Queue<Object> msgs;

        /**
         * Constructor.
         *
         * @param msgs Messages to be processed.
         */
        public IoWorker(Queue<Object> msgs) {
            super(ctx.igniteInstanceName(), ctx.igniteInstanceName() + "query-schema-io-worker",
                SchemaIoManager.this.log);

            this.msgs = msgs;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                if (stopped)
                    return;

                Object msg = msgs.poll();

                if (msg == null)
                    break;

                onMessage(msg);
            }
        }
    }
}
