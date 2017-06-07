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

package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteDiagnosticMessage;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridTimerTask;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CLUSTER_PROC;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;

/**
 *
 */
public class ClusterProcessor extends GridProcessorAdapter {
    /** */
    private final boolean DIAGNOSTIC_ENABLED =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_DIAGNOSTIC_ENABLED, false);

    /** */
    private static final String DIAGNOSTIC_LOG_CATEGORY = "org.apache.ignite.internal.diagnostic";

    /** */
    private static final String ATTR_UPDATE_NOTIFIER_STATUS = "UPDATE_NOTIFIER_STATUS";

    /** Periodic version check delay. */
    private static final long PERIODIC_VER_CHECK_DELAY = 1000 * 60 * 60; // Every hour.

    /** Periodic version check delay. */
    private static final long PERIODIC_VER_CHECK_CONN_TIMEOUT = 10 * 1000; // 10 seconds.

    /** */
    private IgniteClusterImpl cluster;

    /** */
    private final AtomicBoolean notifyEnabled = new AtomicBoolean();

    /** */
    @GridToStringExclude
    private Timer updateNtfTimer;

    /** Version checker. */
    @GridToStringExclude
    private GridUpdateNotifier verChecker;

    /** */
    private final IgniteLogger diagnosticLog;

    /** */
    private final AtomicReference<ConcurrentHashMap<Long, InternalDiagnosticFuture>> diagnosticFutMap =
        new AtomicReference<>();

    /** */
    private final AtomicLong diagFutId = new AtomicLong();

    /**
     * @param ctx Kernal context.
     */
    public ClusterProcessor(GridKernalContext ctx) {
        super(ctx);

        notifyEnabled.set(IgniteSystemProperties.getBoolean(IGNITE_UPDATE_NOTIFIER,
            Boolean.parseBoolean(IgniteProperties.get("ignite.update.notifier.enabled.by.default"))));

        cluster = new IgniteClusterImpl(ctx);

        diagnosticLog = ctx.log(DIAGNOSTIC_LOG_CATEGORY);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void initListeners() throws IgniteCheckedException {
        ctx.event().addLocalEventListener(new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    assert evt instanceof DiscoveryEvent;
                    assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                    DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                    UUID nodeId = discoEvt.eventNode().id();

                    ConcurrentHashMap<Long, InternalDiagnosticFuture> futs = diagnosticFutMap.get();

                    if (futs != null) {
                        for (InternalDiagnosticFuture fut : futs.values()) {
                            if (fut.nodeId.equals(nodeId))
                                fut.onDone("Target node failed: " + nodeId);
                        }
                    }
                }
            },
            EVT_NODE_FAILED, EVT_NODE_LEFT);

        ctx.io().addMessageListener(GridTopic.TOPIC_INTERNAL_DIAGNOSTIC, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (msg instanceof IgniteDiagnosticMessage) {
                    IgniteDiagnosticMessage msg0 = (IgniteDiagnosticMessage)msg;

                    if (msg0.request()) {
                        ClusterNode node = ctx.discovery().node(nodeId);

                        if (node == null) {
                            if (diagnosticLog.isDebugEnabled()) {
                                diagnosticLog.debug("Skip diagnostic request, sender node left " +
                                    "[node=" + nodeId + ", msg=" + msg + ']');
                            }

                            return;
                        }

                        String resMsg;

                        IgniteClosure<GridKernalContext, String> c;

                        try {
                            c = msg0.unmarshalClosure(ctx);

                            resMsg = c.apply(ctx);
                        }
                        catch (Exception e) {
                            U.error(diagnosticLog, "Failed to run diagnostic closure: " + e, e);

                            resMsg = "Failed to run diagnostic closure: " + e;
                        }

                        IgniteDiagnosticMessage res = IgniteDiagnosticMessage.createResponse(resMsg, msg0.futureId());

                        try {
                            ctx.io().sendToGridTopic(node, GridTopic.TOPIC_INTERNAL_DIAGNOSTIC, res, GridIoPolicy.SYSTEM_POOL);
                        }
                        catch (ClusterTopologyCheckedException ignore) {
                            if (diagnosticLog.isDebugEnabled()) {
                                diagnosticLog.debug("Failed to send diagnostic response, node left " +
                                    "[node=" + nodeId + ", msg=" + msg + ']');
                            }
                        }
                        catch (IgniteCheckedException e) {
                            U.error(diagnosticLog, "Failed to send diagnostic response [msg=" + msg0 + "]", e);
                        }
                    }
                    else {
                        InternalDiagnosticFuture fut = diagnosticFuturesMap().get(msg0.futureId());

                        if (fut != null)
                            fut.onResponse(msg0);
                        else
                            U.warn(diagnosticLog, "Failed to find diagnostic message future [msg=" + msg0 + ']');
                    }
                }
                else
                    U.warn(diagnosticLog, "Received unexpected message: " + msg);
            }
        });
    }

    /**
     * @return Logger for diagnostic category.
     */
    public IgniteLogger diagnosticLog() {
        return diagnosticLog;
    }

    /**
     * @return Cluster.
     */
    public IgniteClusterImpl get() {
        return cluster;
    }

    /**
     * @return Client reconnect future.
     */
    public IgniteFuture<?> clientReconnectFuture() {
        IgniteFuture<?> fut = cluster.clientReconnectFuture();

        return fut != null ? fut : new IgniteFinishedFutureImpl<>();
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return CLUSTER_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        dataBag.addJoiningNodeData(CLUSTER_PROC.ordinal(), getDiscoveryData());
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        dataBag.addNodeSpecificData(CLUSTER_PROC.ordinal(), getDiscoveryData());
    }


    /**
     * @return Discovery data.
     */
    private Serializable getDiscoveryData() {
        HashMap<String, Object> map = new HashMap<>();

        map.put(ATTR_UPDATE_NOTIFIER_STATUS, notifyEnabled.get());

        return map;
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        Map<UUID, Serializable> nodeSpecData = data.nodeSpecificData();

        if (nodeSpecData != null) {
            Boolean lstFlag = findLastFlag(nodeSpecData.values());

            if (lstFlag != null)
                notifyEnabled.set(lstFlag);
        }
    }


    /**
     * @param vals collection to seek through.
     */
    private Boolean findLastFlag(Collection<Serializable> vals) {
        Boolean flag = null;

        for (Serializable ser : vals) {
            if (ser != null) {
                Map<String, Object> map = (Map<String, Object>) ser;

                if (map.containsKey(ATTR_UPDATE_NOTIFIER_STATUS))
                    flag = (Boolean) map.get(ATTR_UPDATE_NOTIFIER_STATUS);
            }
        }

        return flag;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean activeOnStart) throws IgniteCheckedException {
        if (notifyEnabled.get()) {
            try {
                verChecker = new GridUpdateNotifier(ctx.igniteInstanceName(),
                    VER_STR,
                    ctx.gateway(),
                    ctx.plugins().allProviders(),
                    false);

                updateNtfTimer = new Timer("ignite-update-notifier-timer", true);

                // Setup periodic version check.
                updateNtfTimer.scheduleAtFixedRate(
                    new UpdateNotifierTimerTask((IgniteKernal)ctx.grid(), verChecker, notifyEnabled),
                    0, PERIODIC_VER_CHECK_DELAY);
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to create GridUpdateNotifier: " + e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // Cancel update notification timer.
        if (updateNtfTimer != null)
            updateNtfTimer.cancel();

        if (verChecker != null)
            verChecker.stop();

    }

    /**
     * Disables update notifier.
     */
    public void disableUpdateNotifier() {
        notifyEnabled.set(false);
    }

    /**
     * @return Update notifier status.
     */
    public boolean updateNotifierEnabled() {
        return notifyEnabled.get();
    }

    /**
     * @return Latest version string.
     */
    public String latestVersion() {
        return verChecker != null ? verChecker.latestVersion() : null;
    }

    /**
     * @param nodeId Target node ID.
     * @param dhtVer Tx dht version.
     * @param nearVer Tx near version.
     * @param msg Local message to log.
     */
    public void dumpRemoteTxInfo(UUID nodeId, GridCacheVersion dhtVer, GridCacheVersion nearVer, final String msg) {
        if (!DIAGNOSTIC_ENABLED)
            return;

        IgniteInternalFuture<String> fut = diagnosticInfo(nodeId,
            new IgniteDiagnosticMessage.TxInfoClosure(ctx, dhtVer, nearVer),
            msg);

        listenAndLog(fut);
    }

    /**
     * @param nodeId Target node ID.
     * @param cacheId Cache ID.
     * @param keys Keys.
     * @param msg Local message to log.
     */
    public void dumpTxKeyInfo(UUID nodeId, int cacheId, Collection<KeyCacheObject> keys, final String msg) {
        if (!DIAGNOSTIC_ENABLED)
            return;

        IgniteInternalFuture<String> fut = diagnosticInfo(nodeId, new IgniteDiagnosticMessage.TxEntriesInfoClosure(ctx, cacheId, keys), msg);

        listenAndLog(fut);
    }

    /**
     * @param nodeId Target node ID.
     * @param msg Local message to log.
     */
    public void dumpBasicInfo(final UUID nodeId, final String msg,
        @Nullable IgniteInClosure<IgniteInternalFuture<String>> lsnr) {
        if (!DIAGNOSTIC_ENABLED)
            return;

        IgniteInternalFuture<String> fut = diagnosticInfo(nodeId, new IgniteDiagnosticMessage.BaseClosure(ctx), msg);

        if (lsnr != null)
            fut.listen(lsnr);

        listenAndLog(fut);
    }

    /**
     * @param nodeId Target node ID.
     * @param topVer Exchange topology version.
     * @param msg Local message to log.
     */
    public void dumpExchangeInfo(final UUID nodeId, AffinityTopologyVersion topVer, final String msg) {
        if (!DIAGNOSTIC_ENABLED)
            return;

        IgniteInternalFuture<String> fut = diagnosticInfo(nodeId, new IgniteDiagnosticMessage.ExchangeInfoClosure(ctx, topVer), msg);

        listenAndLog(fut);
    }

    /**
     * @param fut Future.
     */
    private void listenAndLog(IgniteInternalFuture<String> fut) {
        fut.listen(new CI1<IgniteInternalFuture<String>>() {
            @Override public void apply(IgniteInternalFuture<String> msgFut) {
                try {
                    String msg = msgFut.get();

                    diagnosticLog.info(msg);
                }
                catch (Exception e) {
                    U.error(diagnosticLog, "Failed to dump diagnostic info: " + e, e);
                }
            }
        });
    }

    /**
     * @param nodeId Target node ID.
     * @param c Closure.
     * @param baseMsg Local message to log.
     * @return Message future.
     */
    private IgniteInternalFuture<String> diagnosticInfo(final UUID nodeId,
        IgniteClosure<GridKernalContext, String> c,
        final String baseMsg) {
        final GridFutureAdapter<String> infoFut = new GridFutureAdapter<>();

        final IgniteInternalFuture<String> rmtFut = sendDiagnosticMessage(nodeId, c);

        rmtFut.listen(new CI1<IgniteInternalFuture<String>>() {
            @Override public void apply(IgniteInternalFuture<String> fut) {
                String rmtMsg;

                try {
                    rmtMsg = fut.get();
                }
                catch (Exception e) {
                    rmtMsg = "Diagnostic processing error: " + e;
                }

                final String rmtMsg0 = rmtMsg;

                IgniteInternalFuture<String> locFut = IgniteDiagnosticMessage.dumpCommunicationInfo(ctx, nodeId);

                locFut.listen(new CI1<IgniteInternalFuture<String>>() {
                    @Override public void apply(IgniteInternalFuture<String> locFut) {
                        String locMsg;

                        try {
                            locMsg = locFut.get();
                        }
                        catch (Exception e) {
                            locMsg = "Failed to get info for local node: " + e;
                        }

                        String sb = baseMsg + U.nl() +
                            "Remote node information:" + U.nl() + rmtMsg0 +
                            U.nl() + "Local communication statistics:" + U.nl() +
                            locMsg;

                        infoFut.onDone(sb);
                    }
                });
            }
        });

        return infoFut;
    }

    /**
     * @param nodeId Target node ID.
     * @param c Message closure.
     * @return Message future.
     */
    private IgniteInternalFuture<String> sendDiagnosticMessage(UUID nodeId, IgniteClosure<GridKernalContext, String> c) {
        try {
            IgniteDiagnosticMessage msg = IgniteDiagnosticMessage.createRequest(ctx,
                c,
                diagFutId.getAndIncrement());

            InternalDiagnosticFuture fut = new InternalDiagnosticFuture(nodeId, msg.futureId());

            diagnosticFuturesMap().put(msg.futureId(), fut);

            ctx.io().sendToGridTopic(nodeId, GridTopic.TOPIC_INTERNAL_DIAGNOSTIC, msg, GridIoPolicy.SYSTEM_POOL);

            return fut;
        }
        catch (Exception e) {
            U.error(log, "Failed to send diagnostic message: " + e);

            return new GridFinishedFuture<>("Failed to send diagnostic message: " + e);
        }
    }

    /**
     * @return Diagnostic messages futures map.
     */
    private ConcurrentHashMap<Long, InternalDiagnosticFuture> diagnosticFuturesMap() {
        ConcurrentHashMap<Long, InternalDiagnosticFuture> map = diagnosticFutMap.get();

        if (map == null) {
            if (!diagnosticFutMap.compareAndSet(null, map = new ConcurrentHashMap<>()))
                map = diagnosticFutMap.get();
        }

        return map;
    }

    /**
     * Update notifier timer task.
     */
    private static class UpdateNotifierTimerTask extends GridTimerTask {
        /** Reference to kernal. */
        private final WeakReference<IgniteKernal> kernalRef;

        /** Logger. */
        private final IgniteLogger log;

        /** Version checker. */
        private final GridUpdateNotifier verChecker;

        /** Whether this is the first run. */
        private boolean first = true;

        /** */
        private final AtomicBoolean notifyEnabled;

        /**
         * Constructor.
         *
         * @param kernal Kernal.
         * @param verChecker Version checker.
         */
        private UpdateNotifierTimerTask(IgniteKernal kernal, GridUpdateNotifier verChecker,
            AtomicBoolean notifyEnabled) {
            kernalRef = new WeakReference<>(kernal);

            log = kernal.context().log(UpdateNotifierTimerTask.class);

            this.verChecker = verChecker;
            this.notifyEnabled = notifyEnabled;
        }

        /** {@inheritDoc} */
        @Override public void safeRun() throws InterruptedException {
            if (!notifyEnabled.get())
                return;

            if (!first) {
                IgniteKernal kernal = kernalRef.get();

                if (kernal != null)
                    verChecker.topologySize(kernal.cluster().nodes().size());
            }

            verChecker.checkForNewVersion(log);

            // Just wait for 10 secs.
            Thread.sleep(PERIODIC_VER_CHECK_CONN_TIMEOUT);

            // Just wait another 60 secs in order to get
            // version info even on slow connection.
            for (int i = 0; i < 60 && verChecker.latestVersion() == null; i++)
                Thread.sleep(1000);

            // Report status if one is available.
            // No-op if status is NOT available.
            verChecker.reportStatus(log);

            if (first) {
                first = false;

                verChecker.reportOnlyNew(true);
            }
        }
    }

    /**
     *
     */
    class InternalDiagnosticFuture extends GridFutureAdapter<String> {
        /** */
        private final long id;

        /** */
        private final UUID nodeId;

        /**
         * @param id Future ID.
         */
        InternalDiagnosticFuture(UUID nodeId, long id) {
            this.nodeId = nodeId;
            this.id = id;
        }

        /**
         * @param msg Response message.
         */
        public void onResponse(IgniteDiagnosticMessage msg) {
            onDone(msg.message());
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable String res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                diagnosticFuturesMap().remove(id);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(InternalDiagnosticFuture.class, this);
        }
    }
}