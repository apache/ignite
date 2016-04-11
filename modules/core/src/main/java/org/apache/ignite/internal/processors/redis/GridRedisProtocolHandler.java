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

package org.apache.ignite.internal.processors.redis;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.redis.handler.GridRedisCommandHandler;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.jsr166.LongAdder8;

/**
 * Command protocol handler.
 */
public class GridRedisProtocolHandler {
    /** Worker name. */
    private final static String WORKER_NAME = "redis-proc-worker";

    /** Logger. */
    private final IgniteLogger log;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Busy lock. */
    private final GridSpinReadWriteLock busyLock = new GridSpinReadWriteLock();

    /** Workers count. */
    private final LongAdder8 workersCnt = new LongAdder8();

    /** OK-to-start latch. */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Command handlers. */
    protected final Map<GridRedisCommand, GridRedisCommandHandler> handlers = new EnumMap<>(GridRedisCommand.class);

    /**
     * @param log
     */
    public GridRedisProtocolHandler(GridKernalContext ctx, IgniteLogger log) {
        this.ctx = ctx;
        this.log = log;
    }

    public void addCommandHandler(GridRedisCommandHandler hnd) {
        assert !handlers.containsValue(hnd);

        if (log.isDebugEnabled())
            log.debug("Added Redis command handler: " + hnd);

        for (GridRedisCommand cmd : hnd.supportedCommands()) {
            assert !handlers.containsKey(cmd) : cmd;

            handlers.put(cmd, hnd);
        }
    }

    /**
     * @param msg Request message.
     * @return Response.
     * @throws IgniteCheckedException In case of error.
     */
    public GridRedisMessage handle(GridRedisMessage msg) throws IgniteCheckedException {
        return handleAsync(msg).get();
    }

    /**
     * @param msg Request message.
     * @return Future.
     */
    public IgniteInternalFuture<GridRedisMessage> handleAsync(final GridRedisMessage msg) {
        if (!busyLock.tryReadLock())
            return new GridFinishedFuture<>(
                new IgniteCheckedException("Failed to handle request (received request while stopping grid)."));

        try {
            final GridWorkerFuture<GridRedisMessage> fut = new GridWorkerFuture<>();

            workersCnt.increment();

            GridWorker w = new GridWorker(ctx.gridName(), WORKER_NAME, log) {
                @Override protected void body() {
                    try {
                        IgniteInternalFuture<GridRedisMessage> res = handleRequest(msg);

                        res.listen(new IgniteInClosure<IgniteInternalFuture<GridRedisMessage>>() {
                            @Override public void apply(IgniteInternalFuture<GridRedisMessage> f) {
                                try {
                                    fut.onDone(f.get());
                                }
                                catch (IgniteCheckedException e) {
                                    fut.onDone(e);
                                }
                            }
                        });
                    }
                    catch (Throwable e) {
                        if (e instanceof Error)
                            U.error(log, "Client request execution failed with error.", e);

                        fut.onDone(U.cast(e));

                        if (e instanceof Error)
                            throw e;
                    }
                    finally {
                        workersCnt.decrement();
                    }
                }
            };

            fut.setWorker(w);

            try {
                ctx.getRedisExecutorService().execute(w);
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to execute worker due to execution rejection " +
                    "(increase upper bound on Redis executor service). " +
                    "Will attempt to process request in the current thread instead.", e);

                w.run();
            }

            return fut;
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * Handles with the assigned command handler.
     *
     * @param req Request.
     * @return Future.
     */
    private IgniteInternalFuture<GridRedisMessage> handleRequest(final GridRedisMessage req) {
        if (startLatch.getCount() > 0) {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
                return new GridFinishedFuture<>(new IgniteCheckedException("Failed to handle request " +
                    "(protocol handler was interrupted when awaiting grid start).", e));
            }
        }

        if (log.isDebugEnabled())
            log.debug("Request from Redis client: " + req);

        GridRedisCommandHandler hnd = handlers.get(req.command());

        IgniteInternalFuture<GridRedisMessage> res = hnd == null ? null : hnd.handleAsync(req);

        if (res == null)
            return new GridFinishedFuture<>(
                new IgniteCheckedException("Failed to find registered handler for command: " + req.command()));

        return res;
    }

    /**
     * Starts request handling.
     */
    void start() {
        startLatch.countDown();
    }

    /**
     * Ensures pending requests are processed and no more requests are taken.
     */
    void stopGracefully() {
        busyLock.writeLock();

        boolean interrupted = Thread.interrupted();

        while (workersCnt.sum() != 0) {
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();

        startLatch.countDown();
    }
}
