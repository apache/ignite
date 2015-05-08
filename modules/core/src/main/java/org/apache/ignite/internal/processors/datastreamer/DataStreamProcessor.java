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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.stream.*;
import org.apache.ignite.thread.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.internal.GridTopic.*;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;

/**
 *
 */
public class DataStreamProcessor<K, V> extends GridProcessorAdapter {
    /** Loaders map (access is not supposed to be highly concurrent). */
    private Collection<DataStreamerImpl> ldrs = new GridConcurrentHashSet<>();

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Flushing thread. */
    private Thread flusher;

    /** */
    private final DelayQueue<DataStreamerImpl<K, V>> flushQ = new DelayQueue<>();

    /** Marshaller. */
    private final Marshaller marsh;

    /**
     * @param ctx Kernal context.
     */
    public DataStreamProcessor(GridKernalContext ctx) {
        super(ctx);

        ctx.io().addMessageListener(TOPIC_DATASTREAM, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                assert msg instanceof DataStreamerRequest;

                processRequest(nodeId, (DataStreamerRequest)msg);
            }
        });

        marsh = ctx.config().getMarshaller();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        flusher = new IgniteThread(new GridWorker(ctx.gridName(), "grid-data-loader-flusher", log) {
            @Override protected void body() throws InterruptedException {
                while (!isCancelled()) {
                    DataStreamerImpl<K, V> ldr = flushQ.take();

                    if (!busyLock.enterBusy())
                        return;

                    try {
                        if (ldr.isClosed())
                            continue;

                        ldr.tryFlush();

                        flushQ.offer(ldr);
                    }
                    finally {
                        busyLock.leaveBusy();
                    }
                }
            }
        });

        flusher.start();

        if (log.isDebugEnabled())
            log.debug("Started data streamer processor.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        ctx.io().removeMessageListener(TOPIC_DATASTREAM);

        busyLock.block();

        U.interrupt(flusher);
        U.join(flusher, log);

        for (DataStreamerImpl<?, ?> ldr : ldrs) {
            if (log.isDebugEnabled())
                log.debug("Closing active data streamer on grid stop [ldr=" + ldr + ", cancel=" + cancel + ']');

            try {
                ldr.closeEx(cancel);
            }
            catch (IgniteInterruptedCheckedException e) {
                U.warn(log, "Interrupted while waiting for completion of the data streamer: " + ldr, e);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to close data streamer: " + ldr, e);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Stopped data streamer processor.");
    }

    /**
     * @param cacheName Cache name ({@code null} for default cache).
     * @return Data loader.
     */
    public DataStreamerImpl<K, V> dataStreamer(@Nullable String cacheName) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to create data streamer (grid is stopping).");

        try {
            final DataStreamerImpl<K, V> ldr = new DataStreamerImpl<>(ctx, cacheName, flushQ);

            ldrs.add(ldr);

            ldr.internalFuture().listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    boolean b = ldrs.remove(ldr);

                    assert b : "Loader has not been added to set: " + ldr;

                    if (log.isDebugEnabled())
                        log.debug("Loader has been completed: " + ldr);
                }
            });

            return ldr;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId Sender ID.
     * @param req Request.
     */
    private void processRequest(UUID nodeId, DataStreamerRequest req) {
        if (!busyLock.enterBusy()) {
            if (log.isDebugEnabled())
                log.debug("Ignoring data load request (node is stopping): " + req);

            return;
        }

        try {
            if (log.isDebugEnabled())
                log.debug("Processing data load request: " + req);

            Object topic;

            try {
                topic = marsh.unmarshal(req.responseTopicBytes(), null);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to unmarshal topic from request: " + req, e);

                return;
            }

            ClassLoader clsLdr;

            if (req.forceLocalDeployment())
                clsLdr = U.gridClassLoader();
            else {
                GridDeployment dep = ctx.deploy().getGlobalDeployment(
                    req.deploymentMode(),
                    req.sampleClassName(),
                    req.sampleClassName(),
                    req.userVersion(),
                    nodeId,
                    req.classLoaderId(),
                    req.participants(),
                    null);

                if (dep == null) {
                    sendResponse(nodeId,
                        topic,
                        req.requestId(),
                        new IgniteCheckedException("Failed to get deployment for request [sndId=" + nodeId +
                            ", req=" + req + ']'),
                        false);

                    return;
                }

                clsLdr = dep.classLoader();
            }

            StreamReceiver<K, V> updater;

            try {
                updater = marsh.unmarshal(req.updaterBytes(), clsLdr);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to unmarshal message [nodeId=" + nodeId + ", req=" + req + ']', e);

                sendResponse(nodeId, topic, req.requestId(), e, false);

                return;
            }

            Collection<DataStreamerEntry> col = req.entries();

            DataStreamerUpdateJob job = new DataStreamerUpdateJob(ctx,
                log,
                req.cacheName(),
                col,
                req.ignoreDeploymentOwnership(),
                req.skipStore(),
                updater);

            Exception err = null;

            try {
                job.call();
            }
            catch (Exception e) {
                U.error(log, "Failed to finish update job.", e);

                err = e;
            }

            sendResponse(nodeId, topic, req.requestId(), err, req.forceLocalDeployment());
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param resTopic Response topic.
     * @param reqId Request ID.
     * @param err Error.
     * @param forceLocDep Force local deployment.
     */
    private void sendResponse(UUID nodeId, Object resTopic, long reqId, @Nullable Throwable err,
        boolean forceLocDep) {
        byte[] errBytes;

        try {
            errBytes = err != null ? marsh.marshal(err) : null;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to marshal message.", e);

            return;
        }

        DataStreamerResponse res = new DataStreamerResponse(reqId, errBytes, forceLocDep);

        try {
            ctx.io().send(nodeId, resTopic, res, PUBLIC_POOL);
        }
        catch (IgniteCheckedException e) {
            if (ctx.discovery().alive(nodeId))
                U.error(log, "Failed to respond to node [nodeId=" + nodeId + ", res=" + res + ']', e);
            else if (log.isDebugEnabled())
                log.debug("Node has left the grid: " + nodeId);
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Data streamer processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   ldrsSize: " + ldrs.size());
    }
}
