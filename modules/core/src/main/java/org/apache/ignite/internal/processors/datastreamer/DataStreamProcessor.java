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

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.DelayQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridTopic.TOPIC_DATASTREAM;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;

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

    /** */
    private byte[] marshErrBytes;

    /**
     * @param ctx Kernal context.
     */
    public DataStreamProcessor(GridKernalContext ctx) {
        super(ctx);

        if (!ctx.clientNode()) {
            ctx.io().addMessageListener(TOPIC_DATASTREAM, new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg) {
                    assert msg instanceof DataStreamerRequest;

                    processRequest(nodeId, (DataStreamerRequest)msg);
                }
            });
        }

        marsh = ctx.config().getMarshaller();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        marshErrBytes = U.marshal(marsh, new IgniteCheckedException("Failed to marshal response error, " +
            "see node log for details."));

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

        if (!ctx.clientNode())
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

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        for (DataStreamerImpl<?, ?> ldr : ldrs)
            ldr.onDisconnected(reconnectFut);
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
    private void processRequest(final UUID nodeId, final DataStreamerRequest req) {
        if (!busyLock.enterBusy()) {
            if (log.isDebugEnabled())
                log.debug("Ignoring data load request (node is stopping): " + req);

            return;
        }

        try {
            if (log.isDebugEnabled())
                log.debug("Processing data load request: " + req);

            AffinityTopologyVersion locAffVer = ctx.cache().context().exchange().readyAffinityVersion();
            AffinityTopologyVersion rmtAffVer = req.topologyVersion();

            if (locAffVer.compareTo(rmtAffVer) < 0) {
                if (log.isDebugEnabled())
                    log.debug("Received request has higher affinity topology version [request=" + req +
                        ", locTopVer=" + locAffVer + ", rmtTopVer=" + rmtAffVer + ']');

                IgniteInternalFuture<?> fut = ctx.cache().context().exchange().affinityReadyFuture(rmtAffVer);

                if (fut != null && !fut.isDone()) {
                    fut.listen(new CI1<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> t) {
                            ctx.closure().runLocalSafe(new Runnable() {
                                @Override public void run() {
                                    processRequest(nodeId, req);
                                }
                            }, false);
                        }
                    });

                    return;
                }
            }

            Object topic;

            try {
                topic = U.unmarshal(marsh, req.responseTopicBytes(), U.resolveClassLoader(null, ctx.config()));
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
                updater = U.unmarshal(marsh, req.updaterBytes(), U.resolveClassLoader(clsLdr, ctx.config()));

                if (updater != null)
                    ctx.resource().injectGeneric(updater);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to unmarshal message [nodeId=" + nodeId + ", req=" + req + ']', e);

                sendResponse(nodeId, topic, req.requestId(), e, false);

                return;
            }

            localUpdate(nodeId, req, updater, topic);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId Node id.
     * @param req Request.
     * @param updater Updater.
     * @param topic Topic.
     */
    private void localUpdate(final UUID nodeId,
        final DataStreamerRequest req,
        final StreamReceiver<K, V> updater,
        final Object topic) {
        final boolean allowOverwrite = !(updater instanceof DataStreamerImpl.IsolatedUpdater);

        try {
            GridCacheAdapter cache = ctx.cache().internalCache(req.cacheName());

            if (cache == null)
                throw new IgniteCheckedException("Cache not created or already destroyed.");

            GridCacheContext cctx = cache.context();

            DataStreamerUpdateJob job = null;

            GridFutureAdapter waitFut = null;

            if (!allowOverwrite)
                cctx.topology().readLock();

            GridDhtTopologyFuture topWaitFut = null;

            try {
                GridDhtTopologyFuture fut = cctx.topologyVersionFuture();

                AffinityTopologyVersion topVer = fut.topologyVersion();

                if (!allowOverwrite && !topVer.equals(req.topologyVersion())) {
                    Exception err = new IgniteCheckedException(
                        "DataStreamer will retry data transfer at stable topology " +
                            "[reqTop=" + req.topologyVersion() + ", topVer=" + topVer + ", node=remote]");

                    sendResponse(nodeId, topic, req.requestId(), err, req.forceLocalDeployment());
                }
                else if (allowOverwrite || fut.isDone()) {
                    job = new DataStreamerUpdateJob(ctx,
                        log,
                        req.cacheName(),
                        req.entries(),
                        req.ignoreDeploymentOwnership(),
                        req.skipStore(),
                        req.keepBinary(),
                        updater);

                    waitFut = allowOverwrite ? null : cctx.mvcc().addDataStreamerFuture(topVer);
                }
                else
                    topWaitFut = fut;
            }
            finally {
                if (!allowOverwrite)
                    cctx.topology().readUnlock();
            }

            if (topWaitFut != null) {
                // Need call 'listen' after topology read lock is released.
                topWaitFut.listen(new IgniteInClosure<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> e) {
                        localUpdate(nodeId, req, updater, topic);
                    }
                });

                return;
            }

            if (job != null) {
                try {
                    job.call();

                    sendResponse(nodeId, topic, req.requestId(), null, req.forceLocalDeployment());
                }
                finally {
                    if (waitFut != null)
                        waitFut.onDone();
                }
            }
        }
        catch (Throwable e) {
            sendResponse(nodeId, topic, req.requestId(), e, req.forceLocalDeployment());

            if (e instanceof Error)
                throw (Error)e;
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
            errBytes = err != null ? U.marshal(marsh, err) : null;
        }
        catch (Exception e) {
            U.error(log, "Failed to marshal error [err=" + err + ", marshErr=" + e + ']', e);

            errBytes = marshErrBytes;
        }

        DataStreamerResponse res = new DataStreamerResponse(reqId, errBytes, forceLocDep);

        try {
            Byte plc = GridIoManager.currentPolicy();

            if (plc == null)
                plc = PUBLIC_POOL;

            ctx.io().send(nodeId, resTopic, res, plc);
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
