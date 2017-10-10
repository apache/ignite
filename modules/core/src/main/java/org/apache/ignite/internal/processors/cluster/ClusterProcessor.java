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
import org.apache.ignite.internal.IgniteDiagnosticInfo;
import org.apache.ignite.internal.IgniteDiagnosticMessage;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
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
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DIAGNOSTIC_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CLUSTER_PROC;
import static org.apache.ignite.internal.GridTopic.TOPIC_INTERNAL_DIAGNOSTIC;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;

/**
 *
 */
public class ClusterProcessor extends GridProcessorAdapter {
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
    private final AtomicReference<ConcurrentHashMap<Long, InternalDiagnosticFuture>> diagnosticFutMap =
        new AtomicReference<>();

    /** */
    private final AtomicLong diagFutId = new AtomicLong();

    /**
     * @param ctx Kernal context.
     */
    public ClusterProcessor(GridKernalContext ctx) {
        super(ctx);

        notifyEnabled.set(IgniteSystemProperties.getBoolean(IGNITE_UPDATE_NOTIFIER, true));

        cluster = new IgniteClusterImpl(ctx);
    }

    /**
     * @return Diagnostic flag.
     */
    public boolean diagnosticEnabled() {
        return getBoolean(IGNITE_DIAGNOSTIC_ENABLED, true);
    }

    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void initDiagnosticListeners() throws IgniteCheckedException {
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
                                fut.onDone(new IgniteDiagnosticInfo("Target node failed: " + nodeId));
                        }
                    }
                }
            },
            EVT_NODE_FAILED, EVT_NODE_LEFT);

        ctx.io().addMessageListener(TOPIC_INTERNAL_DIAGNOSTIC, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
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

                        byte[] diagRes;

                        IgniteClosure<GridKernalContext, IgniteDiagnosticInfo> c;

                        try {
                            c = msg0.unmarshal(marsh);

                            diagRes = marsh.marshal(c.apply(ctx));
                        }
                        catch (Exception e) {
                            U.error(diagnosticLog, "Failed to run diagnostic closure: " + e, e);

                            try {
                                IgniteDiagnosticInfo errInfo =
                                    new IgniteDiagnosticInfo("Failed to run diagnostic closure: " + e);

                                diagRes = marsh.marshal(errInfo);
                            }
                            catch (Exception e0) {
                                U.error(diagnosticLog, "Failed to marshal diagnostic closure result: " + e, e);

                                diagRes = null;
                            }
                        }

                        IgniteDiagnosticMessage res = IgniteDiagnosticMessage.createResponse(diagRes, msg0.futureId());

                        try {
                            ctx.io().sendToGridTopic(node, TOPIC_INTERNAL_DIAGNOSTIC, res, GridIoPolicy.SYSTEM_POOL);
                        }
                        catch (ClusterTopologyCheckedException e) {
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

                        if (fut != null) {
                            IgniteDiagnosticInfo res;

                            try {
                                res = msg0.unmarshal(marsh);

                                if (res == null)
                                    res = new IgniteDiagnosticInfo("Remote node failed to marshal response.");
                            }
                            catch (Exception e) {
                                U.error(diagnosticLog, "Failed to unmarshal diagnostic response: " + e, e);

                                res = new IgniteDiagnosticInfo("Failed to unmarshal diagnostic response: " + e);
                            }

                            fut.onResponse(res);
                        }
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
    @SuppressWarnings("unchecked")
    private Boolean findLastFlag(Collection<Serializable> vals) {
        Boolean flag = null;

        for (Serializable ser : vals) {
            if (ser != null) {
                Map<String, Object> map = (Map<String, Object>)ser;

                if (map.containsKey(ATTR_UPDATE_NOTIFIER_STATUS))
                    flag = (Boolean) map.get(ATTR_UPDATE_NOTIFIER_STATUS);
            }
        }

        return flag;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        if (notifyEnabled.get()) {
            try {
                verChecker = new GridUpdateNotifier(ctx.igniteInstanceName(), VER_STR, false);

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

        // Io manager can be null, if invoke stop before create io manager, for example
        // exception on start.
        if (ctx.io() != null)
            ctx.io().removeMessageListener(TOPIC_INTERNAL_DIAGNOSTIC);
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
     * Sends diagnostic message closure to remote node. When response received dumps
     * remote message and local communication info about connection(s) with remote node.
     *
     * @param nodeId Target node ID.
     * @param c Closure to send.
     * @param baseMsg Local message to log.
     * @return Message future.
     */
    public IgniteInternalFuture<String> requestDiagnosticInfo(final UUID nodeId,
        IgniteClosure<GridKernalContext, IgniteDiagnosticInfo> c,
        final String baseMsg) {
        final GridFutureAdapter<String> infoFut = new GridFutureAdapter<>();

        final IgniteInternalFuture<IgniteDiagnosticInfo> rmtFut = sendDiagnosticMessage(nodeId, c);

        rmtFut.listen(new CI1<IgniteInternalFuture<IgniteDiagnosticInfo>>() {
            @Override public void apply(IgniteInternalFuture<IgniteDiagnosticInfo> fut) {
                String rmtMsg;

                try {
                    rmtMsg = fut.get().message();
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

                        String msg = baseMsg + U.nl() +
                            "Remote node information:" + U.nl() + rmtMsg0 +
                            U.nl() + "Local communication statistics:" + U.nl() +
                            locMsg;

                        infoFut.onDone(msg);
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
    private IgniteInternalFuture<IgniteDiagnosticInfo> sendDiagnosticMessage(UUID nodeId,
        IgniteClosure<GridKernalContext, IgniteDiagnosticInfo> c) {
        try {
            IgniteDiagnosticMessage msg = IgniteDiagnosticMessage.createRequest(marsh,
                c,
                diagFutId.getAndIncrement());

            InternalDiagnosticFuture fut = new InternalDiagnosticFuture(nodeId, msg.futureId());

            diagnosticFuturesMap().put(msg.futureId(), fut);

            ctx.io().sendToGridTopic(nodeId, TOPIC_INTERNAL_DIAGNOSTIC, msg, GridIoPolicy.SYSTEM_POOL);

            return fut;
        }
        catch (Exception e) {
            U.error(diagnosticLog, "Failed to send diagnostic message: " + e);

            return new GridFinishedFuture<>(new IgniteDiagnosticInfo("Failed to send diagnostic message: " + e));
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
        private UpdateNotifierTimerTask(
            IgniteKernal kernal,
            GridUpdateNotifier verChecker,
            AtomicBoolean notifyEnabled
        ) {
            log = kernal.context().log(UpdateNotifierTimerTask.class);

            this.verChecker = verChecker;
            this.notifyEnabled = notifyEnabled;
        }

        /** {@inheritDoc} */
        @Override public void safeRun() throws InterruptedException {
            if (!notifyEnabled.get())
                return;

            verChecker.checkForNewVersion(log, first);

            // Just wait for 10 secs.
            Thread.sleep(PERIODIC_VER_CHECK_CONN_TIMEOUT);

            // Just wait another 60 secs in order to get
            // version info even on slow connection.
            for (int i = 0; i < 60 && verChecker.latestVersion() == null; i++)
                Thread.sleep(1000);

            // Report status if one is available.
            // No-op if status is NOT available.
            verChecker.reportStatus(log);

            if (first && verChecker.error() == null) {
                first = false;

                verChecker.reportOnlyNew(true);
            }
        }
    }

    /**
     *
     */
    class InternalDiagnosticFuture extends GridFutureAdapter<IgniteDiagnosticInfo> {
        /** */
        private final long id;

        /** */
        private final UUID nodeId;

        /**
         * @param nodeId Target node ID.
         * @param id Future ID.
         */
        InternalDiagnosticFuture(UUID nodeId, long id) {
            this.nodeId = nodeId;
            this.id = id;
        }

        /**
         * @param res Response.
         */
        public void onResponse(IgniteDiagnosticInfo res) {
            onDone(res);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable IgniteDiagnosticInfo res, @Nullable Throwable err) {
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