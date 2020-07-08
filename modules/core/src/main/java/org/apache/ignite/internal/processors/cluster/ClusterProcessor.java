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
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.ClusterTagUpdatedEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDiagnosticInfo;
import org.apache.ignite.internal.IgniteDiagnosticMessage;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridTimerTask;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.mxbean.IgniteClusterMXBean;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CLUSTER_NAME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DIAGNOSTIC_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_TAG_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CLUSTER_PROC;
import static org.apache.ignite.internal.GridTopic.TOPIC_INTERNAL_DIAGNOSTIC;
import static org.apache.ignite.internal.GridTopic.TOPIC_METRICS;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;

/**
 *
 */
public class ClusterProcessor extends GridProcessorAdapter implements DistributedMetastorageLifecycleListener {
    /** */
    private static final String ATTR_UPDATE_NOTIFIER_STATUS = "UPDATE_NOTIFIER_STATUS";

    /** */
    private static final String CLUSTER_ID_TAG_KEY =
        DistributedMetaStorage.IGNITE_INTERNAL_KEY_PREFIX + "cluster.id.tag";

    /** */
    private static final String M_BEAN_NAME = "IgniteCluster";

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

    /** */
    private final Map<UUID, byte[]> allNodesMetrics = new ConcurrentHashMap<>();

    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** */
    private DiscoveryMetricsProvider metricsProvider;

    /** */
    private boolean sndMetrics;

    /** Cluster ID is stored in local variable before activation when it goes to distributed metastorage. */
    private volatile UUID locClusterId;

    /** Cluster tag is stored in local variable before activation when it goes to distributed metastorage. */
    private volatile String locClusterTag;

    /** */
    private volatile DistributedMetaStorage metastorage;

    /** */
    private ObjectName mBean;

    /**
     * @param ctx Kernal context.
     */
    public ClusterProcessor(GridKernalContext ctx) {
        super(ctx);

        notifyEnabled.set(IgniteSystemProperties.getBoolean(IGNITE_UPDATE_NOTIFIER, true));

        cluster = new IgniteClusterImpl(ctx);

        sndMetrics = !(ctx.config().getDiscoverySpi() instanceof TcpDiscoverySpi);
    }

    /**
     * @return Diagnostic flag.
     */
    public boolean diagnosticEnabled() {
        return getBoolean(IGNITE_DIAGNOSTIC_ENABLED, true);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

        GridInternalSubscriptionProcessor isp = ctx.internalSubscriptionProcessor();

        isp.registerDistributedMetastorageListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
        ClusterIdAndTag idAndTag = readKey(metastorage, CLUSTER_ID_TAG_KEY, "Reading cluster ID and tag " +
            "from metastorage failed, default values will be generated");

        if (log.isInfoEnabled())
            log.info("Cluster ID and tag has been read from metastorage: " + idAndTag);

        if (idAndTag != null) {
            locClusterId = idAndTag.id();
            locClusterTag = idAndTag.tag();
        }

        metastorage.listen(
            (k) -> k.equals(CLUSTER_ID_TAG_KEY),
            (String k, ClusterIdAndTag oldVal, ClusterIdAndTag newVal) -> {
                if (log.isInfoEnabled())
                    log.info(
                        "Cluster tag will be set to new value: " +
                            newVal != null ? newVal.tag() : "null" +
                            ", previous value was: " +
                            oldVal != null ? oldVal.tag() : "null");

                if (oldVal != null && newVal != null) {
                    if (ctx.event().isRecordable(EVT_CLUSTER_TAG_UPDATED)) {
                        String msg = "Tag of cluster with id " +
                            oldVal.id() +
                            " has been updated to new value: " +
                            newVal.tag() +
                            ", previous value was " +
                            oldVal.tag();

                        ctx.closure().runLocalSafe(() -> ctx.event().record(
                            new ClusterTagUpdatedEvent(
                                ctx.discovery().localNode(),
                                msg,
                                oldVal.id(),
                                oldVal.tag(),
                                newVal.tag()
                            )
                        ));
                    }
                }

                cluster.setTag(newVal != null ? newVal.tag() : null);
            }
        );
    }

    /**
     * @param metastorage Metastorage.
     * @param key Key.
     * @param errMsg Err message.
     */
    private <T extends Serializable> T readKey(ReadableDistributedMetaStorage metastorage, String key, String errMsg) {
        try {
            return metastorage.read(key);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, errMsg, e);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
        this.metastorage = metastorage;

        ctx.closure().runLocalSafe(
            () -> {
                try {
                    ClusterIdAndTag idAndTag = new ClusterIdAndTag(cluster.id(), cluster.tag());

                    if (log.isInfoEnabled())
                        log.info("Writing cluster ID and tag to metastorage on ready for write " + idAndTag);

                    metastorage.writeAsync(CLUSTER_ID_TAG_KEY, idAndTag);
                }
                catch (IgniteCheckedException e) {
                    ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
                }
            }
        );
    }

    /**
     * Method is called when user requests updating tag through public API.
     *
     * @param newTag New tag.
     */
    public void updateTag(String newTag) throws IgniteCheckedException {
        ClusterIdAndTag oldTag = metastorage.read(CLUSTER_ID_TAG_KEY);

        if (oldTag == null)
            throw new IgniteCheckedException("Cannot change tag as default tag has not been set yet. " +
                "Please try again later.");

        if (!metastorage.compareAndSet(CLUSTER_ID_TAG_KEY, oldTag, new ClusterIdAndTag(oldTag.id(), newTag))) {
            ClusterIdAndTag concurrentValue = metastorage.read(CLUSTER_ID_TAG_KEY);

            throw new IgniteCheckedException("Cluster tag has been concurrently updated to different value: " +
                concurrentValue.tag());
        }
        else
            cluster.setTag(newTag);
    }

    /**
     * Node makes ID and tag available through public API on local join event.
     *
     * Two cases.
     * <ul>
     *     <li>In in-memory scenario very first node of the cluster generates ID and tag,
     *     other nodes receives them on join.</li>
     *     <li>When persistence is enabled each node reads ID and tag from metastorage
     *     when it becomes ready for read.</li>
     * </ul>
     */
    public void onLocalJoin() {
        cluster.setId(locClusterId != null ? locClusterId : UUID.randomUUID());

        cluster.setTag(locClusterTag != null ? locClusterTag :
            ClusterTagGenerator.generateTag());
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        assert ctx.clientNode();

        locClusterId = null;
        locClusterTag = null;

        cluster.setId(null);
        cluster.setTag(null);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) {
        assert ctx.clientNode();

        cluster.setId(locClusterId);
        cluster.setTag(locClusterTag);

        return null;
    }

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

                allNodesMetrics.remove(nodeId);
            }
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);

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

        if (sndMetrics) {
            ctx.io().addMessageListener(TOPIC_METRICS, new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                    if (msg instanceof ClusterMetricsUpdateMessage)
                        processMetricsUpdateMessage(nodeId, (ClusterMetricsUpdateMessage)msg);
                    else
                        U.warn(log, "Received unexpected message for TOPIC_METRICS: " + msg);
                }
            });
        }
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

        dataBag.addGridCommonData(CLUSTER_PROC.ordinal(), new ClusterIdAndTag(cluster.id(), cluster.tag()));
    }

    /**
     * @return Discovery data.
     */
    private Serializable getDiscoveryData() {
        HashMap<String, Object> map = new HashMap<>(2);

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

        ClusterIdAndTag commonData = (ClusterIdAndTag)data.commonData();

        if (commonData != null) {
            Serializable remoteClusterId = commonData.id();

            if (remoteClusterId != null) {
                if (locClusterId != null && !locClusterId.equals(remoteClusterId)) {
                    log.warning("Received cluster ID differs from locally stored cluster ID " +
                        "and will be rewritten. " +
                        "Received cluster ID: " + remoteClusterId +
                        ", local cluster ID: " + locClusterId);
                }

                locClusterId = (UUID)remoteClusterId;
            }

            String remoteClusterTag = commonData.tag();

            if (remoteClusterTag != null)
                locClusterTag = remoteClusterTag;
        }
    }

    /**
     * @param vals collection to seek through.
     */
    private Boolean findLastFlag(Collection<Serializable> vals) {
        Boolean flag = null;

        for (Serializable ser : vals) {
            if (ser != null) {
                Map<String, Object> map = (Map<String, Object>)ser;

                if (map.containsKey(ATTR_UPDATE_NOTIFIER_STATUS))
                    flag = (Boolean)map.get(ATTR_UPDATE_NOTIFIER_STATUS);
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

        if (sndMetrics) {
            metricsProvider = ctx.discovery().createMetricsProvider();

            long updateFreq = ctx.config().getMetricsUpdateFrequency();

            ctx.timeout().addTimeoutObject(new MetricsUpdateTimeoutObject(updateFreq));
        }

        IgniteClusterMXBeanImpl mxBeanImpl = new IgniteClusterMXBeanImpl(cluster);

        if (!U.IGNITE_MBEANS_DISABLED) {
            try {
                mBean = U.registerMBean(
                    ctx.config().getMBeanServer(),
                    ctx.igniteInstanceName(),
                    M_BEAN_NAME,
                    mxBeanImpl.getClass().getSimpleName(),
                    mxBeanImpl,
                    IgniteClusterMXBean.class);

                if (log.isDebugEnabled())
                    log.debug("Registered " + M_BEAN_NAME + " MBean: " + mBean);
            }
            catch (Throwable e) {
                U.error(log, "Failed to register MBean for cluster: ", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        unregisterMBean();
    }

    /**
     * Unregister IgniteCluster MBean.
     */
    private void unregisterMBean() {
        ObjectName mBeanName = mBean;

        if (mBeanName == null)
            return;

        assert !U.IGNITE_MBEANS_DISABLED;

        try {
            ctx.config().getMBeanServer().unregisterMBean(mBeanName);

            mBean = null;

            if (log.isDebugEnabled())
                log.debug("Unregistered " + M_BEAN_NAME + " MBean: " + mBeanName);
        }
        catch (JMException e) {
            U.error(log, "Failed to unregister " + M_BEAN_NAME + " MBean: " + mBeanName, e);
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
     * @param sndNodeId Sender node ID.
     * @param msg Message.
     */
    private void processMetricsUpdateMessage(UUID sndNodeId, ClusterMetricsUpdateMessage msg) {
        byte[] nodeMetrics = msg.nodeMetrics();

        if (nodeMetrics != null) {
            assert msg.allNodesMetrics() == null;

            allNodesMetrics.put(sndNodeId, nodeMetrics);

            updateNodeMetrics(ctx.discovery().discoCache(), sndNodeId, nodeMetrics);
        }
        else {
            Map<UUID, byte[]> allNodesMetrics = msg.allNodesMetrics();

            assert allNodesMetrics != null;

            DiscoCache discoCache = ctx.discovery().discoCache();

            for (Map.Entry<UUID, byte[]> e : allNodesMetrics.entrySet()) {
                if (!ctx.localNodeId().equals(e.getKey()))
                    updateNodeMetrics(discoCache, e.getKey(), e.getValue());
            }
        }
    }

    /**
     * @param discoCache Discovery data cache.
     * @param nodeId Node ID.
     * @param metricsBytes Marshalled metrics.
     */
    private void updateNodeMetrics(DiscoCache discoCache, UUID nodeId, byte[] metricsBytes) {
        ClusterNode node = discoCache.node(nodeId);

        if (node == null || !discoCache.alive(nodeId))
            return;

        try {
            ClusterNodeMetrics metrics = U.unmarshalZip(ctx.config().getMarshaller(), metricsBytes, null);

            assert node instanceof IgniteClusterNode : node;

            IgniteClusterNode node0 = (IgniteClusterNode)node;

            node0.setMetrics(ClusterMetricsSnapshot.deserialize(metrics.metrics(), 0));
            node0.setCacheMetrics(metrics.cacheMetrics());

            ctx.discovery().metricsUpdateEvent(discoCache, node0);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to unmarshal node metrics: " + e);
        }
    }

    /**
     *
     */
    private void updateMetrics() {
        if (ctx.isStopping() || ctx.clientDisconnected())
            return;

        ClusterNode oldest = ctx.discovery().oldestAliveServerNode(AffinityTopologyVersion.NONE);

        if (oldest == null)
            return;

        if (ctx.localNodeId().equals(oldest.id())) {
            IgniteClusterNode locNode = (IgniteClusterNode)ctx.discovery().localNode();

            locNode.setMetrics(metricsProvider.metrics());
            locNode.setCacheMetrics(metricsProvider.cacheMetrics());

            ClusterNodeMetrics metrics = new ClusterNodeMetrics(locNode.metrics(), locNode.cacheMetrics());

            try {
                byte[] metricsBytes = U.zip(U.marshal(ctx.config().getMarshaller(), metrics));

                allNodesMetrics.put(ctx.localNodeId(), metricsBytes);
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Failed to marshal local node metrics: " + e, e);
            }

            ctx.discovery().metricsUpdateEvent(ctx.discovery().discoCache(), locNode);

            Collection<ClusterNode> allNodes = ctx.discovery().allNodes();

            ClusterMetricsUpdateMessage msg = new ClusterMetricsUpdateMessage(new HashMap<>(allNodesMetrics));

            for (ClusterNode node : allNodes) {
                if (ctx.localNodeId().equals(node.id()) || !ctx.discovery().alive(node.id()))
                    continue;

                try {
                    ctx.io().sendToGridTopic(node, TOPIC_METRICS, msg, GridIoPolicy.SYSTEM_POOL);
                }
                catch (ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send metrics update, node failed: " + e);
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, "Failed to send metrics update: " + e, e);
                }
            }
        }
        else {
            ClusterNodeMetrics metrics = new ClusterNodeMetrics(metricsProvider.metrics(), metricsProvider.cacheMetrics());

            try {
                byte[] metricsBytes = U.zip(U.marshal(ctx.config().getMarshaller(), metrics));

                ClusterMetricsUpdateMessage msg = new ClusterMetricsUpdateMessage(metricsBytes);

                ctx.io().sendToGridTopic(oldest, TOPIC_METRICS, msg, GridIoPolicy.SYSTEM_POOL);
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send metrics update to oldest, node failed: " + e);
            }
            catch (IgniteCheckedException e) {
                LT.warn(log, e, "Failed to send metrics update to oldest: " + e, false, false);
            }
        }
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
     * Get cluster name.
     *
     * @return Cluster name.
     * */
    public String clusterName() {
        return IgniteSystemProperties.getString(
            IGNITE_CLUSTER_NAME,
            ctx.cache().utilityCache().context().dynamicDeploymentId().toString()
        );
    }

    /**
     * Sends diagnostic message closure to remote node. When response received dumps remote message and local
     * communication info about connection(s) with remote node.
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

    /**
     *
     */
    private class MetricsUpdateTimeoutObject implements GridTimeoutObject, Runnable {
        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private long endTime;

        /** */
        private final long timeout;

        /**
         * @param timeout Timeout.
         */
        MetricsUpdateTimeoutObject(long timeout) {
            this.timeout = timeout;

            endTime = U.currentTimeMillis() + timeout;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            updateMetrics();

            endTime = U.currentTimeMillis() + timeout;

            ctx.timeout().addTimeoutObject(this);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            ctx.getSystemExecutorService().execute(this);
        }
    }
}
