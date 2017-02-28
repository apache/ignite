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

package org.apache.ignite.internal.managers.discovery;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridNodeOrderComparator;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinitySharedManager;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetrics;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.service.GridServiceProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridTuple5;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiHistorySupport;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.DiscoverySpiOrderSupport;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_LATE_AFFINITY_ASSIGNMENT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_DFLT_SUID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PEER_CLASSLOADING;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_USER_NAME;
import static org.apache.ignite.internal.IgniteVersionUtils.VER;
import static org.apache.ignite.plugin.segmentation.SegmentationPolicy.NOOP;

/**
 * Discovery SPI manager.
 */
public class GridDiscoveryManager extends GridManagerAdapter<DiscoverySpi> {
    /** Metrics update frequency. */
    private static final long METRICS_UPDATE_FREQ = 3000;

    /** */
    private static final MemoryMXBean mem = ManagementFactory.getMemoryMXBean();

    /** */
    private static final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    /** */
    private static final RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();

    /** */
    private static final ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    /** */
    private static final Collection<GarbageCollectorMXBean> gc = ManagementFactory.getGarbageCollectorMXBeans();

    /** */
    private static final String PREFIX = "Topology snapshot";

    /** Discovery cached history size. */
    static final int DISCOVERY_HISTORY_SIZE = getInteger(IGNITE_DISCOVERY_HISTORY_SIZE, 500);

    /** Predicate filtering out daemon nodes. */
    private static final IgnitePredicate<ClusterNode> FILTER_DAEMON = new P1<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            return !n.isDaemon();
        }
    };

    /** Predicate filtering client nodes. */
    private static final IgnitePredicate<ClusterNode> FILTER_CLI = new P1<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            return CU.clientNode(n);
        }
    };

    /** Discovery event worker. */
    private final DiscoveryWorker discoWrk = new DiscoveryWorker();

    /** Network segment check worker. */
    private SegmentCheckWorker segChkWrk;

    /** Network segment check thread. */
    private IgniteThread segChkThread;

    /** Last logged topology. */
    private final AtomicLong lastLoggedTop = new AtomicLong();

    /** Local node. */
    private ClusterNode locNode;

    /** Local node daemon flag. */
    private boolean isLocDaemon;

    /** {@code True} if resolvers were configured and network segment check is enabled. */
    private boolean hasRslvrs;

    /** Last segment check result. */
    private final AtomicBoolean lastSegChkRes = new AtomicBoolean(true);

    /** Topology cache history. */
    private final GridBoundedConcurrentLinkedHashMap<AffinityTopologyVersion, DiscoCache> discoCacheHist =
        new GridBoundedConcurrentLinkedHashMap<>(DISCOVERY_HISTORY_SIZE);

    /** Topology snapshots history. */
    private volatile Map<Long, Collection<ClusterNode>> topHist = new HashMap<>();

    /** Topology version. */
    private final AtomicReference<Snapshot> topSnap =
        new AtomicReference<>(new Snapshot(AffinityTopologyVersion.ZERO, null));

    /** Minor topology version. */
    private int minorTopVer;

    /** Order supported flag. */
    private boolean discoOrdered;

    /** Topology snapshots history supported flag. */
    private boolean histSupported;

    /** Configured network segment check frequency. */
    private long segChkFreq;

    /** Local node join to topology event. */
    private GridFutureAdapter<DiscoveryEvent> locJoinEvt = new GridFutureAdapter<>();

    /** GC CPU load. */
    private volatile double gcCpuLoad;

    /** CPU load. */
    private volatile double cpuLoad;

    /** Metrics. */
    private final GridLocalMetrics metrics = createMetrics();

    /** Metrics update worker. */
    private GridTimeoutProcessor.CancelableTask metricsUpdateTask;

    /** Custom event listener. */
    private ConcurrentMap<Class<?>, List<CustomEventListener<DiscoveryCustomMessage>>> customEvtLsnrs =
        new ConcurrentHashMap8<>();

    /** Map of dynamic cache filters. */
    private Map<String, CachePredicate> registeredCaches = new HashMap<>();

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Received custom messages history. */
    private final ArrayDeque<IgniteUuid> rcvdCustomMsgs = new ArrayDeque<>();

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** @param ctx Context. */
    public GridDiscoveryManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getDiscoverySpi());
    }

    /**
     * @return Memory usage of non-heap memory.
     */
    private MemoryUsage nonHeapMemoryUsage() {
        // Workaround of exception in WebSphere.
        // We received the following exception:
        // java.lang.IllegalArgumentException: used value cannot be larger than the committed value
        // at java.lang.management.MemoryUsage.<init>(MemoryUsage.java:105)
        // at com.ibm.lang.management.MemoryMXBeanImpl.getNonHeapMemoryUsageImpl(Native Method)
        // at com.ibm.lang.management.MemoryMXBeanImpl.getNonHeapMemoryUsage(MemoryMXBeanImpl.java:143)
        // at org.apache.ignite.spi.metrics.jdk.GridJdkLocalMetricsSpi.getMetrics(GridJdkLocalMetricsSpi.java:242)
        //
        // We so had to workaround this with exception handling, because we can not control classes from WebSphere.
        try {
            return mem.getNonHeapMemoryUsage();
        }
        catch (IllegalArgumentException ignored) {
            return new MemoryUsage(0, 0, 0, 0);
        }
    }

    /** {@inheritDoc} */
    @Override public void onBeforeSpiStart() {
        DiscoverySpi spi = getSpi();

        spi.setNodeAttributes(ctx.nodeAttributes(), VER);
    }

    /**
     * Adds dynamic cache filter.
     *
     * @param cacheName Cache name.
     * @param filter Cache filter.
     * @param nearEnabled Near enabled flag.
     * @param cacheMode Cache mode.
     */
    public void setCacheFilter(
        String cacheName,
        IgnitePredicate<ClusterNode> filter,
        boolean nearEnabled,
        CacheMode cacheMode
    ) {
        if (!registeredCaches.containsKey(cacheName))
            registeredCaches.put(cacheName, new CachePredicate(filter, nearEnabled, cacheMode));
    }

    /**
     * Removes dynamic cache filter.
     *
     * @param cacheName Cache name.
     */
    public void removeCacheFilter(String cacheName) {
        CachePredicate p = registeredCaches.remove(cacheName);

        assert p != null : cacheName;
    }

    /**
     * Adds near node ID to cache filter.
     *
     * @param cacheName Cache name.
     * @param clientNodeId Near node ID.
     * @param nearEnabled Near enabled flag.
     * @return {@code True} if new node ID was added.
     */
    public boolean addClientNode(String cacheName, UUID clientNodeId, boolean nearEnabled) {
        CachePredicate p = registeredCaches.get(cacheName);

        assert p != null : cacheName;

        return p.addClientNode(clientNodeId, nearEnabled);
    }

    /**
     * Removes near node ID from cache filter.
     *
     * @param cacheName Cache name.
     * @param clientNodeId Near node ID.
     * @return {@code True} if existing node ID was removed.
     */
    public boolean onClientCacheClose(String cacheName, UUID clientNodeId) {
        CachePredicate p = registeredCaches.get(cacheName);

        assert p != null : cacheName;

        return p.onNodeLeft(clientNodeId);
    }

    /**
     * @return Client nodes map.
     */
    public Map<String, Map<UUID, Boolean>> clientNodesMap() {
        Map<String, Map<UUID, Boolean>> res = null;

        for (Map.Entry<String, CachePredicate> entry : registeredCaches.entrySet()) {
            CachePredicate pred = entry.getValue();

            if (!F.isEmpty(pred.clientNodes)) {
                if (res == null)
                    res = U.newHashMap(registeredCaches.size());

                res.put(entry.getKey(), new HashMap<>(pred.clientNodes));
            }
        }

        return res;
    }

    /**
     * @param leftNodeId Left node ID.
     */
    private void updateClientNodes(UUID leftNodeId) {
        for (Map.Entry<String, CachePredicate> entry : registeredCaches.entrySet()) {
            CachePredicate pred = entry.getValue();

            pred.onNodeLeft(leftNodeId);
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        if (Boolean.TRUE.equals(ctx.config().isClientMode()) && !getSpi().isClientMode())
            ctx.performance().add("Enable client mode for TcpDiscoverySpi " +
                    "(set TcpDiscoverySpi.forceServerMode to false)");
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        long totSysMemory = -1;

        try {
            totSysMemory = U.<Long>property(os, "totalPhysicalMemorySize");
        }
        catch (RuntimeException ignored) {
            // No-op.
        }

        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_PHY_RAM, totSysMemory);

        DiscoverySpi spi = getSpi();

        discoOrdered = discoOrdered();

        histSupported = historySupported();

        isLocDaemon = ctx.isDaemon();

        hasRslvrs = !ctx.config().isClientMode() && !F.isEmpty(ctx.config().getSegmentationResolvers());

        segChkFreq = ctx.config().getSegmentCheckFrequency();

        if (hasRslvrs) {
            if (segChkFreq < 0)
                throw new IgniteCheckedException("Segment check frequency cannot be negative: " + segChkFreq);

            if (segChkFreq > 0 && segChkFreq < 2000)
                U.warn(log, "Configuration parameter 'segmentCheckFrequency' is too low " +
                    "(at least 2000 ms recommended): " + segChkFreq);

            int segResAttemp = ctx.config().getSegmentationResolveAttempts();

            if (segResAttemp < 1)
                throw new IgniteCheckedException(
                    "Segment resolve attempts cannot be negative or zero: " + segResAttemp);

            checkSegmentOnStart();
        }

        metricsUpdateTask = ctx.timeout().schedule(new MetricsUpdater(), METRICS_UPDATE_FREQ, METRICS_UPDATE_FREQ);

        spi.setMetricsProvider(createMetricsProvider());

        if (ctx.security().enabled()) {
            spi.setAuthenticator(new DiscoverySpiNodeAuthenticator() {
                @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
                    try {
                        return ctx.security().authenticateNode(node, cred);
                    }
                    catch (IgniteCheckedException e) {
                        throw U.convertException(e);
                    }
                }

                @Override public boolean isGlobalNodeAuthentication() {
                    return ctx.security().isGlobalNodeAuthentication();
                }
            });
        }

        spi.setListener(new DiscoverySpiListener() {
            private long gridStartTime;

            @Override public void onDiscovery(
                final int type,
                final long topVer,
                final ClusterNode node,
                final Collection<ClusterNode> topSnapshot,
                final Map<Long, Collection<ClusterNode>> snapshots,
                @Nullable DiscoverySpiCustomMessage spiCustomMsg
            ) {
                DiscoveryCustomMessage customMsg = spiCustomMsg == null ? null
                    : ((CustomMessageWrapper)spiCustomMsg).delegate();

                if (skipMessage(type, customMsg))
                    return;

                final ClusterNode locNode = localNode();

                if (snapshots != null)
                    topHist = snapshots;

                boolean verChanged;

                if (type == EVT_NODE_METRICS_UPDATED)
                    verChanged = false;
                else {
                    if (type != EVT_NODE_SEGMENTED &&
                        type != EVT_CLIENT_NODE_DISCONNECTED &&
                        type != EVT_CLIENT_NODE_RECONNECTED &&
                        type != DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                        minorTopVer = 0;

                        verChanged = true;
                    }
                    else
                        verChanged = false;
                }

                if (type == EVT_NODE_FAILED || type == EVT_NODE_LEFT) {
                    for (DiscoCache c : discoCacheHist.values())
                        c.updateAlives(node);

                    updateClientNodes(node.id());
                }

                final AffinityTopologyVersion nextTopVer;

                if (type == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                    assert customMsg != null;

                    boolean incMinorTopVer = ctx.cache().onCustomEvent(customMsg,
                        new AffinityTopologyVersion(topVer, minorTopVer));

                    if (incMinorTopVer) {
                        minorTopVer++;

                        verChanged = true;
                    }

                    nextTopVer = new AffinityTopologyVersion(topVer, minorTopVer);
                }
                else {
                    nextTopVer = new AffinityTopologyVersion(topVer, minorTopVer);

                    ctx.cache().onDiscoveryEvent(type, node, nextTopVer);
                }

                if (type == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                    for (Class cls = customMsg.getClass(); cls != null; cls = cls.getSuperclass()) {
                        List<CustomEventListener<DiscoveryCustomMessage>> list = customEvtLsnrs.get(cls);

                        if (list != null) {
                            for (CustomEventListener<DiscoveryCustomMessage> lsnr : list) {
                                try {
                                    lsnr.onCustomEvent(nextTopVer, node, customMsg);
                                }
                                catch (Exception e) {
                                    U.error(log, "Failed to notify direct custom event listener: " + customMsg, e);
                                }
                            }
                        }
                    }
                }

                // Put topology snapshot into discovery history.
                // There is no race possible between history maintenance and concurrent discovery
                // event notifications, since SPI notifies manager about all events from this listener.
                if (verChanged) {
                    DiscoCache cache = new DiscoCache(locNode, F.view(topSnapshot, F.remoteNodes(locNode.id())));

                    discoCacheHist.put(nextTopVer, cache);

                    boolean set = updateTopologyVersionIfGreater(nextTopVer, cache);

                    assert set || topVer == 0 : "Topology version has not been updated [this.topVer=" +
                        topSnap + ", topVer=" + topVer + ", node=" + node +
                        ", evt=" + U.gridEventName(type) + ']';
                }

                // If this is a local join event, just save it and do not notify listeners.
                if (type == EVT_NODE_JOINED && node.id().equals(locNode.id())) {
                    if (gridStartTime == 0)
                        gridStartTime = getSpi().getGridStartTime();

                    updateTopologyVersionIfGreater(new AffinityTopologyVersion(locNode.order()),
                        new DiscoCache(localNode(), F.view(topSnapshot, F.remoteNodes(locNode.id()))));

                    startLatch.countDown();

                    DiscoveryEvent discoEvt = new DiscoveryEvent();

                    discoEvt.node(ctx.discovery().localNode());
                    discoEvt.eventNode(node);
                    discoEvt.type(EVT_NODE_JOINED);

                    discoEvt.topologySnapshot(topVer, new ArrayList<>(
                        F.viewReadOnly(topSnapshot, new C1<ClusterNode, ClusterNode>() {
                            @Override public ClusterNode apply(ClusterNode e) {
                                return e;
                            }
                        }, FILTER_DAEMON)));

                    locJoinEvt.onDone(discoEvt);

                    return;
                }
                else if (type == EVT_CLIENT_NODE_DISCONNECTED) {
                    /*
                     * Notify all components from discovery thread to avoid concurrent
                     * reconnect while disconnect handling is in progress.
                     */

                    assert locNode.isClient() : locNode;
                    assert node.isClient() : node;

                    ((IgniteKernal)ctx.grid()).onDisconnected();

                    locJoinEvt = new GridFutureAdapter<>();

                    registeredCaches.clear();

                    for (AffinityTopologyVersion histVer : discoCacheHist.keySet()) {
                        Object rmvd = discoCacheHist.remove(histVer);

                        assert rmvd != null : histVer;
                    }

                    topHist.clear();

                    topSnap.set(new Snapshot(AffinityTopologyVersion.ZERO,
                        new DiscoCache(locNode, Collections.<ClusterNode>emptySet())));
                }
                else if (type == EVT_CLIENT_NODE_RECONNECTED) {
                    assert locNode.isClient() : locNode;
                    assert node.isClient() : node;

                    boolean clusterRestarted = gridStartTime != getSpi().getGridStartTime();

                    gridStartTime = getSpi().getGridStartTime();

                    ((IgniteKernal)ctx.grid()).onReconnected(clusterRestarted);

                    ctx.cluster().clientReconnectFuture().listen(new CI1<IgniteFuture<?>>() {
                        @Override public void apply(IgniteFuture<?> fut) {
                            try {
                                fut.get();

                                discoWrk.addEvent(type, nextTopVer, node, topSnapshot, null);
                            }
                            catch (IgniteException ignore) {
                                // No-op.
                            }
                        }
                    });

                    return;
                }

                if (type == EVT_CLIENT_NODE_DISCONNECTED || type == EVT_NODE_SEGMENTED || !ctx.clientDisconnected())
                    discoWrk.addEvent(type, nextTopVer, node, topSnapshot, customMsg);
            }
        });

        spi.setDataExchange(new DiscoverySpiDataExchange() {
            @Override public Map<Integer, Serializable> collect(UUID nodeId) {
                assert nodeId != null;

                Map<Integer, Serializable> data = new HashMap<>();

                for (GridComponent comp : ctx.components()) {
                    Serializable compData = comp.collectDiscoveryData(nodeId);

                    if (compData != null) {
                        assert comp.discoveryDataType() != null;

                        data.put(comp.discoveryDataType().ordinal(), compData);
                    }
                }

                return data;
            }

            @Override public void onExchange(UUID joiningNodeId, UUID nodeId, Map<Integer, Serializable> data) {
                for (Map.Entry<Integer, Serializable> e : data.entrySet()) {
                    GridComponent comp = null;

                    for (GridComponent c : ctx.components()) {
                        if (c.discoveryDataType() != null && c.discoveryDataType().ordinal() == e.getKey()) {
                            comp = c;

                            break;
                        }
                    }

                    if (comp != null)
                        comp.onDiscoveryDataReceived(joiningNodeId, nodeId, e.getValue());
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Received discovery data for unknown component: " + e.getKey());
                    }
                }
            }
        });

        startSpi();

        try {
            U.await(startLatch);
        }
        catch (IgniteInterruptedException e) {
            throw new IgniteCheckedException("Failed to start discovery manager (thread has been interrupted).", e);
        }

        // Start segment check worker only if frequency is greater than 0.
        if (hasRslvrs && segChkFreq > 0) {
            segChkWrk = new SegmentCheckWorker();

            segChkThread = new IgniteThread(segChkWrk);

            segChkThread.start();
        }

        locNode = spi.getLocalNode();

        checkAttributes(discoCache().remoteNodes());

        ctx.service().initCompatibilityMode(discoCache().remoteNodes());

        // Start discovery worker.
        new IgniteThread(discoWrk).start();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     * @param type Message type.
     * @param customMsg Custom message.
     * @return {@code True} if should not process message.
     */
    private boolean skipMessage(int type, @Nullable DiscoveryCustomMessage customMsg) {
        if (type == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
            assert customMsg != null && customMsg.id() != null : customMsg;

            if (rcvdCustomMsgs.contains(customMsg.id())) {
                if (log.isDebugEnabled())
                    log.debug("Received duplicated custom message, will ignore [msg=" + customMsg + "]");

                return true;
            }

            rcvdCustomMsgs.addLast(customMsg.id());

            while (rcvdCustomMsgs.size() > DISCOVERY_HISTORY_SIZE)
                rcvdCustomMsgs.pollFirst();
        }

        return false;
    }

    /**
     * @param msgCls Message class.
     * @param lsnr Custom event listener.
     */
    public <T extends DiscoveryCustomMessage> void setCustomEventListener(Class<T> msgCls, CustomEventListener<T> lsnr) {
        List<CustomEventListener<DiscoveryCustomMessage>> list = customEvtLsnrs.get(msgCls);

        if (list == null) {
            list = F.addIfAbsent(customEvtLsnrs, msgCls,
                new CopyOnWriteArrayList<CustomEventListener<DiscoveryCustomMessage>>());
        }

        list.add((CustomEventListener<DiscoveryCustomMessage>)lsnr);
    }

    /**
     * @return Metrics.
     */
    private GridLocalMetrics createMetrics() {
        return new GridLocalMetrics() {
            @Override public int getAvailableProcessors() {
                return os.getAvailableProcessors();
            }

            @Override public double getCurrentCpuLoad() {
                return cpuLoad;
            }

            @Override public double getCurrentGcCpuLoad() {
                return gcCpuLoad;
            }

            @Override public long getHeapMemoryInitialized() {
                return mem.getHeapMemoryUsage().getInit();
            }

            @Override public long getHeapMemoryUsed() {
                return mem.getHeapMemoryUsage().getUsed();
            }

            @Override public long getHeapMemoryCommitted() {
                return mem.getHeapMemoryUsage().getCommitted();
            }

            @Override public long getHeapMemoryMaximum() {
                return mem.getHeapMemoryUsage().getMax();
            }

            @Override public long getNonHeapMemoryInitialized() {
                return nonHeapMemoryUsage().getInit();
            }

            @Override public long getNonHeapMemoryUsed() {
                return nonHeapMemoryUsage().getUsed();
            }

            @Override public long getNonHeapMemoryCommitted() {
                return nonHeapMemoryUsage().getCommitted();
            }

            @Override public long getNonHeapMemoryMaximum() {
                return nonHeapMemoryUsage().getMax();
            }

            @Override public long getUptime() {
                return rt.getUptime();
            }

            @Override public long getStartTime() {
                return rt.getStartTime();
            }

            @Override public int getThreadCount() {
                return threads.getThreadCount();
            }

            @Override public int getPeakThreadCount() {
                return threads.getPeakThreadCount();
            }

            @Override public long getTotalStartedThreadCount() {
                return threads.getTotalStartedThreadCount();
            }

            @Override public int getDaemonThreadCount() {
                return threads.getDaemonThreadCount();
            }
        };
    }

    /**
     * @return Metrics provider.
     */
    private DiscoveryMetricsProvider createMetricsProvider() {
        return new DiscoveryMetricsProvider() {
            /** */
            private final long startTime = U.currentTimeMillis();

            /** {@inheritDoc} */
            @Override public ClusterMetrics metrics() {
                GridJobMetrics jm = ctx.jobMetric().getJobMetrics();

                ClusterMetricsSnapshot nm = new ClusterMetricsSnapshot();

                nm.setLastUpdateTime(U.currentTimeMillis());

                // Job metrics.
                nm.setMaximumActiveJobs(jm.getMaximumActiveJobs());
                nm.setCurrentActiveJobs(jm.getCurrentActiveJobs());
                nm.setAverageActiveJobs(jm.getAverageActiveJobs());
                nm.setMaximumWaitingJobs(jm.getMaximumWaitingJobs());
                nm.setCurrentWaitingJobs(jm.getCurrentWaitingJobs());
                nm.setAverageWaitingJobs(jm.getAverageWaitingJobs());
                nm.setMaximumRejectedJobs(jm.getMaximumRejectedJobs());
                nm.setCurrentRejectedJobs(jm.getCurrentRejectedJobs());
                nm.setAverageRejectedJobs(jm.getAverageRejectedJobs());
                nm.setMaximumCancelledJobs(jm.getMaximumCancelledJobs());
                nm.setCurrentCancelledJobs(jm.getCurrentCancelledJobs());
                nm.setAverageCancelledJobs(jm.getAverageCancelledJobs());
                nm.setTotalRejectedJobs(jm.getTotalRejectedJobs());
                nm.setTotalCancelledJobs(jm.getTotalCancelledJobs());
                nm.setTotalExecutedJobs(jm.getTotalExecutedJobs());
                nm.setMaximumJobWaitTime(jm.getMaximumJobWaitTime());
                nm.setCurrentJobWaitTime(jm.getCurrentJobWaitTime());
                nm.setAverageJobWaitTime(jm.getAverageJobWaitTime());
                nm.setMaximumJobExecuteTime(jm.getMaximumJobExecuteTime());
                nm.setCurrentJobExecuteTime(jm.getCurrentJobExecuteTime());
                nm.setAverageJobExecuteTime(jm.getAverageJobExecuteTime());
                nm.setCurrentIdleTime(jm.getCurrentIdleTime());
                nm.setTotalIdleTime(jm.getTotalIdleTime());
                nm.setAverageCpuLoad(jm.getAverageCpuLoad());

                // Job metrics.
                nm.setTotalExecutedTasks(ctx.task().getTotalExecutedTasks());

                // VM metrics.
                nm.setAvailableProcessors(metrics.getAvailableProcessors());
                nm.setCurrentCpuLoad(metrics.getCurrentCpuLoad());
                nm.setCurrentGcCpuLoad(metrics.getCurrentGcCpuLoad());
                nm.setHeapMemoryInitialized(metrics.getHeapMemoryInitialized());
                nm.setHeapMemoryUsed(metrics.getHeapMemoryUsed());
                nm.setHeapMemoryCommitted(metrics.getHeapMemoryCommitted());
                nm.setHeapMemoryMaximum(metrics.getHeapMemoryMaximum());
                nm.setHeapMemoryTotal(metrics.getHeapMemoryMaximum());
                nm.setNonHeapMemoryInitialized(metrics.getNonHeapMemoryInitialized());
                nonHeapMemoryUsed(nm);
                nm.setNonHeapMemoryCommitted(metrics.getNonHeapMemoryCommitted());
                nm.setNonHeapMemoryMaximum(metrics.getNonHeapMemoryMaximum());
                nm.setNonHeapMemoryTotal(metrics.getNonHeapMemoryMaximum());
                nm.setUpTime(metrics.getUptime());
                nm.setStartTime(metrics.getStartTime());
                nm.setNodeStartTime(startTime);
                nm.setCurrentThreadCount(metrics.getThreadCount());
                nm.setMaximumThreadCount(metrics.getPeakThreadCount());
                nm.setTotalStartedThreadCount(metrics.getTotalStartedThreadCount());
                nm.setCurrentDaemonThreadCount(metrics.getDaemonThreadCount());
                nm.setTotalNodes(1);

                // Data metrics.
                nm.setLastDataVersion(ctx.cache().lastDataVersion());

                GridIoManager io = ctx.io();

                // IO metrics.
                nm.setSentMessagesCount(io.getSentMessagesCount());
                nm.setSentBytesCount(io.getSentBytesCount());
                nm.setReceivedMessagesCount(io.getReceivedMessagesCount());
                nm.setReceivedBytesCount(io.getReceivedBytesCount());
                nm.setOutboundMessagesQueueSize(io.getOutboundMessagesQueueSize());

                return nm;
            }

            /**
             * @param nm Initializing metrics snapshot.
             */
            private void nonHeapMemoryUsed(ClusterMetricsSnapshot nm) {
                long nonHeapUsed = metrics.getNonHeapMemoryUsed();

                Map<Integer, CacheMetrics> nodeCacheMetrics = cacheMetrics();

                if (nodeCacheMetrics != null) {
                    for (Map.Entry<Integer, CacheMetrics> entry : nodeCacheMetrics.entrySet()) {
                        CacheMetrics e = entry.getValue();

                        if (e != null)
                            nonHeapUsed += e.getOffHeapAllocatedSize();
                    }
                }

                nm.setNonHeapMemoryUsed(nonHeapUsed);
            }

            /** {@inheritDoc} */
            @Override public Map<Integer, CacheMetrics> cacheMetrics() {
                Collection<GridCacheAdapter<?, ?>> caches = ctx.cache().internalCaches();

                if (F.isEmpty(caches))
                    return Collections.emptyMap();

                Map<Integer, CacheMetrics> metrics = null;

                for (GridCacheAdapter<?, ?> cache : caches) {
                    if (cache.configuration().isStatisticsEnabled() &&
                        cache.context().started() &&
                        cache.context().affinity().affinityTopologyVersion().topologyVersion() > 0) {
                        if (metrics == null)
                            metrics = U.newHashMap(caches.size());

                        metrics.put(cache.context().cacheId(), cache.localMetrics());
                    }
                }

                return metrics == null ? Collections.<Integer, CacheMetrics>emptyMap() : metrics;
            }
        };
    }

    /**
     * @return Local metrics.
     */
    public GridLocalMetrics metrics() {
        return metrics;
    }

    /** @return {@code True} if ordering is supported. */
    private boolean discoOrdered() {
        DiscoverySpiOrderSupport ann = U.getAnnotation(ctx.config().getDiscoverySpi().getClass(),
            DiscoverySpiOrderSupport.class);

        return ann != null && ann.value();
    }

    /** @return {@code True} if topology snapshots history is supported. */
    private boolean historySupported() {
        DiscoverySpiHistorySupport ann = U.getAnnotation(ctx.config().getDiscoverySpi().getClass(),
            DiscoverySpiHistorySupport.class);

        return ann != null && ann.value();
    }

    /**
     * Checks segment on start waiting for correct segment if necessary.
     *
     * @throws IgniteCheckedException If check failed.
     */
    private void checkSegmentOnStart() throws IgniteCheckedException {
        assert hasRslvrs;

        if (log.isDebugEnabled())
            log.debug("Starting network segment check.");

        while (true) {
            if (ctx.segmentation().isValidSegment())
                break;

            if (ctx.config().isWaitForSegmentOnStart()) {
                LT.warn(log, "Failed to check network segment (retrying every 2000 ms).");

                // Wait and check again.
                U.sleep(2000);
            }
            else
                throw new IgniteCheckedException("Failed to check network segment.");
        }

        if (log.isDebugEnabled())
            log.debug("Finished network segment check successfully.");
    }

    /**
     * Checks whether attributes of the local node are consistent with remote nodes.
     *
     * @param nodes List of remote nodes to check attributes on.
     * @throws IgniteCheckedException In case of error.
     */
    private void checkAttributes(Iterable<ClusterNode> nodes) throws IgniteCheckedException {
        ClusterNode locNode = getSpi().getLocalNode();

        assert locNode != null;

        // Fetch local node attributes once.
        String locPreferIpV4 = locNode.attribute("java.net.preferIPv4Stack");

        Object locMode = locNode.attribute(ATTR_DEPLOYMENT_MODE);

        int locJvmMajVer = nodeJavaMajorVersion(locNode);

        boolean locP2pEnabled = locNode.attribute(ATTR_PEER_CLASSLOADING);

        boolean ipV4Warned = false;

        boolean jvmMajVerWarned = false;

        Boolean locMarshUseDfltSuid = locNode.attribute(ATTR_MARSHALLER_USE_DFLT_SUID);
        boolean locMarshUseDfltSuidBool = locMarshUseDfltSuid == null ? true : locMarshUseDfltSuid;

        Boolean locMarshStrSerVer2 = locNode.attribute(ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2);
        boolean locMarshStrSerVer2Bool = locMarshStrSerVer2 == null ?
            false /* turned on and added to the attributes list by default only when BinaryMarshaller is used. */:
            locMarshStrSerVer2;

        boolean locDelayAssign = locNode.attribute(ATTR_LATE_AFFINITY_ASSIGNMENT);

        Boolean locSrvcCompatibilityEnabled = locNode.attribute(ATTR_SERVICES_COMPATIBILITY_MODE);

        for (ClusterNode n : nodes) {
            int rmtJvmMajVer = nodeJavaMajorVersion(n);

            if (locJvmMajVer != rmtJvmMajVer && !jvmMajVerWarned) {
                U.warn(log, "Local java version is different from remote [loc=" +
                    locJvmMajVer + ", rmt=" + rmtJvmMajVer + "]");

                jvmMajVerWarned = true;
            }

            String rmtPreferIpV4 = n.attribute("java.net.preferIPv4Stack");

            if (!F.eq(rmtPreferIpV4, locPreferIpV4)) {
                if (!ipV4Warned)
                    U.warn(log, "Local node's value of 'java.net.preferIPv4Stack' " +
                        "system property differs from remote node's " +
                        "(all nodes in topology should have identical value) " +
                        "[locPreferIpV4=" + locPreferIpV4 + ", rmtPreferIpV4=" + rmtPreferIpV4 +
                        ", locId8=" + U.id8(locNode.id()) + ", rmtId8=" + U.id8(n.id()) +
                        ", rmtAddrs=" + U.addressesAsString(n) + ']',
                        "Local and remote 'java.net.preferIPv4Stack' system properties do not match.");

                ipV4Warned = true;
            }

            // Daemon nodes are allowed to have any deployment they need.
            // Skip data center ID check for daemon nodes.
            if (!isLocDaemon && !n.isDaemon()) {
                Object rmtMode = n.attribute(ATTR_DEPLOYMENT_MODE);

                if (!locMode.equals(rmtMode))
                    throw new IgniteCheckedException("Remote node has deployment mode different from local " +
                        "[locId8=" + U.id8(locNode.id()) + ", locMode=" + locMode +
                        ", rmtId8=" + U.id8(n.id()) + ", rmtMode=" + rmtMode +
                        ", rmtAddrs=" + U.addressesAsString(n) + ']');

                boolean rmtP2pEnabled = n.attribute(ATTR_PEER_CLASSLOADING);

                if (locP2pEnabled != rmtP2pEnabled)
                    throw new IgniteCheckedException("Remote node has peer class loading enabled flag different from" +
                        " local [locId8=" + U.id8(locNode.id()) + ", locPeerClassLoading=" + locP2pEnabled +
                        ", rmtId8=" + U.id8(n.id()) + ", rmtPeerClassLoading=" + rmtP2pEnabled +
                        ", rmtAddrs=" + U.addressesAsString(n) + ']');
            }

            Boolean rmtMarshUseDfltSuid = n.attribute(ATTR_MARSHALLER_USE_DFLT_SUID);
            boolean rmtMarshUseDfltSuidBool = rmtMarshUseDfltSuid == null ? true : rmtMarshUseDfltSuid;

            if (locMarshUseDfltSuidBool != rmtMarshUseDfltSuidBool) {
                throw new IgniteCheckedException("Local node's " + IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID +
                    " property value differs from remote node's value " +
                    "(to make sure all nodes in topology have identical marshaller settings, " +
                    "configure system property explicitly) " +
                    "[locMarshUseDfltSuid=" + locMarshUseDfltSuid + ", rmtMarshUseDfltSuid=" + rmtMarshUseDfltSuid +
                    ", locNodeAddrs=" + U.addressesAsString(locNode) +
                    ", rmtNodeAddrs=" + U.addressesAsString(n) +
                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + n.id() + ']');
            }

            Boolean rmtMarshStrSerVer2 = n.attribute(ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2);
            boolean rmtMarshStrSerVer2Bool = rmtMarshStrSerVer2 == null ? false : rmtMarshStrSerVer2;

            if (locMarshStrSerVer2Bool != rmtMarshStrSerVer2Bool) {
                throw new IgniteCheckedException("Local node's " + IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2 +
                    " property value differs from remote node's value " +
                    "(to make sure all nodes in topology have identical marshaller settings, " +
                    "configure system property explicitly) " +
                    "[locMarshStrSerVer2=" + locMarshStrSerVer2 + ", rmtMarshStrSerVer2=" + rmtMarshStrSerVer2 +
                    ", locNodeAddrs=" + U.addressesAsString(locNode) +
                    ", rmtNodeAddrs=" + U.addressesAsString(n) +
                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + n.id() + ']');
            }

            boolean rmtLateAssign;

            if (n.version().compareToIgnoreTimestamp(CacheAffinitySharedManager.LATE_AFF_ASSIGN_SINCE) >= 0)
                rmtLateAssign = n.attribute(ATTR_LATE_AFFINITY_ASSIGNMENT);
            else
                rmtLateAssign = false;

            if (locDelayAssign != rmtLateAssign) {
                throw new IgniteCheckedException("Remote node has cache affinity assignment mode different from local " +
                    "[locId8=" +  U.id8(locNode.id()) +
                    ", locDelayAssign=" + locDelayAssign +
                    ", rmtId8=" + U.id8(n.id()) +
                    ", rmtLateAssign=" + rmtLateAssign +
                    ", rmtAddrs=" + U.addressesAsString(n) + ']');
            }

            if (n.version().compareToIgnoreTimestamp(GridServiceProcessor.LAZY_SERVICES_CFG_SINCE) >= 0) {
                Boolean rmtSrvcCompatibilityEnabled = n.attribute(ATTR_SERVICES_COMPATIBILITY_MODE);

                if (!F.eq(locSrvcCompatibilityEnabled, rmtSrvcCompatibilityEnabled)) {
                    throw new IgniteCheckedException("Local node's " + IGNITE_SERVICES_COMPATIBILITY_MODE +
                        " property value differs from remote node's value " +
                        "(to make sure all nodes in topology have identical IgniteServices compatibility mode enabled, " +
                        "configure system property explicitly) " +
                        "[locSrvcCompatibilityEnabled=" + locSrvcCompatibilityEnabled +
                        ", rmtSrvcCompatibilityEnabled=" + rmtSrvcCompatibilityEnabled +
                        ", locNodeAddrs=" + U.addressesAsString(locNode) +
                        ", rmtNodeAddrs=" + U.addressesAsString(n) +
                        ", locNodeId=" + locNode.id() + ", rmtNodeId=" + n.id() + ']');
                }
            }
            else if (Boolean.FALSE.equals(locSrvcCompatibilityEnabled)) {
                throw new IgniteCheckedException("Remote node doesn't support lazy services configuration and " +
                    "local node cannot join node because local node's "
                    + IGNITE_SERVICES_COMPATIBILITY_MODE + " property value explicitly set to 'false'" +
                    "[locNodeAddrs=" + U.addressesAsString(locNode) +
                    ", rmtNodeAddrs=" + U.addressesAsString(n) +
                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + n.id() + ']');
            }
        }

        if (log.isDebugEnabled())
            log.debug("Finished node attributes consistency check.");
    }

    /**
     * Gets Java major version running on the node.
     *
     * @param node Cluster node.
     * @return Java major version.
     * @throws IgniteCheckedException If failed to get the version.
     */
    private int nodeJavaMajorVersion(ClusterNode node) throws IgniteCheckedException {
        try {
            // The format is identical for Oracle JDK, OpenJDK and IBM JDK.
            return Integer.parseInt(node.<String>attribute("java.version").split("\\.")[1]);
        }
        catch (Exception e) {
            U.error(log, "Failed to get java major version (unknown 'java.version' format) [ver=" +
                node.<String>attribute("java.version") + "]", e);

            return 0;
        }
    }

    /**
     * @param nodes Nodes.
     * @return Total CPUs.
     */
    private static int cpus(Collection<ClusterNode> nodes) {
        Collection<String> macSet = new HashSet<>(nodes.size(), 1.0f);

        int cpus = 0;

        for (ClusterNode n : nodes) {
            String macs = n.attribute(ATTR_MACS);

            if (macSet.add(macs))
                cpus += n.metrics().getTotalCpus();
        }

        return cpus;
    }

    /**
     * Prints the latest topology info into log taking into account logging/verbosity settings.
     *
     * @param topVer Topology version.
     */
    public void ackTopology(long topVer) {
        ackTopology(topVer, false);
    }

    /**
     * Logs grid size for license compliance.
     *
     * @param topVer Topology version.
     * @param throttle Suppress printing if this topology was already printed.
     */
    private void ackTopology(long topVer, boolean throttle) {
        assert !isLocDaemon;

        DiscoCache discoCache = discoCacheHist.get(new AffinityTopologyVersion(topVer));

        if (discoCache == null) {
            String msg = "Failed to resolve nodes topology [topVer=" + topVer +
                ", hist=" + discoCacheHist.keySet() + ']';

            if (log.isQuiet())
                U.quiet(false, msg);

            if (log.isDebugEnabled())
                log.debug(msg);
            else if (log.isInfoEnabled())
                log.info(msg);

            return;
        }

        Collection<ClusterNode> rmtNodes = discoCache.remoteNodes();

        Collection<ClusterNode> srvNodes = F.view(discoCache.allNodes(), F.not(FILTER_CLI));

        Collection<ClusterNode> clientNodes = F.view(discoCache.allNodes(), FILTER_CLI);

        ClusterNode locNode = discoCache.localNode();

        Collection<ClusterNode> allNodes = discoCache.allNodes();

        long hash = topologyHash(allNodes);

        // Prevent ack-ing topology for the same topology.
        // Can happen only during node startup.
        if (throttle && lastLoggedTop.getAndSet(hash) == hash)
            return;

        int totalCpus = cpus(allNodes);

        double heap = U.heapSize(allNodes, 2);

        if (log.isQuiet())
            U.quiet(false, topologySnapshotMessage(topVer, srvNodes.size(), clientNodes.size(), totalCpus, heap));

        if (log.isDebugEnabled()) {
            String dbg = "";

            dbg += U.nl() + U.nl() +
                ">>> +----------------+" + U.nl() +
                ">>> " + PREFIX + "." + U.nl() +
                ">>> +----------------+" + U.nl() +
                ">>> Grid name: " + (ctx.gridName() == null ? "default" : ctx.gridName()) + U.nl() +
                ">>> Number of server nodes: " + srvNodes.size() + U.nl() +
                ">>> Number of client nodes: " + clientNodes.size() + U.nl() +
                (discoOrdered ? ">>> Topology version: " + topVer + U.nl() : "") +
                ">>> Topology hash: 0x" + Long.toHexString(hash).toUpperCase() + U.nl();

            dbg += ">>> Local: " +
                locNode.id().toString().toUpperCase() + ", " +
                U.addressesAsString(locNode) + ", " +
                locNode.order() + ", " +
                locNode.attribute("os.name") + ' ' +
                locNode.attribute("os.arch") + ' ' +
                locNode.attribute("os.version") + ", " +
                System.getProperty("user.name") + ", " +
                locNode.attribute("java.runtime.name") + ' ' +
                locNode.attribute("java.runtime.version") + U.nl();

            for (ClusterNode node : rmtNodes)
                dbg += ">>> Remote: " +
                    node.id().toString().toUpperCase() + ", " +
                    U.addressesAsString(node) + ", " +
                    node.order() + ", " +
                    node.attribute("os.name") + ' ' +
                    node.attribute("os.arch") + ' ' +
                    node.attribute("os.version") + ", " +
                    node.attribute(ATTR_USER_NAME) + ", " +
                    node.attribute("java.runtime.name") + ' ' +
                    node.attribute("java.runtime.version") + U.nl();

            dbg += ">>> Total number of CPUs: " + totalCpus + U.nl();
            dbg += ">>> Total heap size: " + heap + "GB" + U.nl();

            log.debug(dbg);
        }
        else if (log.isInfoEnabled())
            log.info(topologySnapshotMessage(topVer, srvNodes.size(), clientNodes.size(), totalCpus, heap));
    }

    /**
     * @param topVer Topology version.
     * @param srvNodesNum Server nodes number.
     * @param clientNodesNum Client nodes number.
     * @param totalCpus Total cpu number.
     * @param heap Heap size.
     * @return Topology snapshot message.
     */
    private String topologySnapshotMessage(long topVer, int srvNodesNum, int clientNodesNum, int totalCpus, double heap) {
        return PREFIX + " [" +
            (discoOrdered ? "ver=" + topVer + ", " : "") +
            "servers=" + srvNodesNum +
            ", clients=" + clientNodesNum +
            ", CPUs=" + totalCpus +
            ", heap=" + heap + "GB]";
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop0(boolean cancel) {
        startLatch.countDown();

        // Stop segment check worker.
        if (segChkWrk != null) {
            segChkWrk.cancel();

            U.join(segChkThread, log);
        }

        if (!locJoinEvt.isDone())
            locJoinEvt.onDone(
                new IgniteCheckedException("Failed to wait for local node joined event (grid is stopping)."));
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        busyLock.block();

        // Stop receiving notifications.
        getSpi().setListener(null);

        // Stop discovery worker and metrics updater.
        U.closeQuiet(metricsUpdateTask);

        U.cancel(discoWrk);

        U.join(discoWrk, log);

        // Stop SPI itself.
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @param nodeIds Node IDs to check.
     * @return {@code True} if at least one ID belongs to an alive node.
     */
    public boolean aliveAll(@Nullable Collection<UUID> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty())
            return false;

        for (UUID id : nodeIds)
            if (!alive(id))
                return false;

        return true;
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if node for given ID is alive.
     */
    public boolean alive(UUID nodeId) {
        return getAlive(nodeId) != null;
    }

    /**
     * @param nodeId Node ID.
     * @return Node if node is alive.
     */
    @Nullable public ClusterNode getAlive(UUID nodeId) {
        assert nodeId != null;

        return getSpi().getNode(nodeId); // Go directly to SPI without checking disco cache.
    }

    /**
     * @param node Node.
     * @return {@code True} if node is alive.
     */
    public boolean alive(ClusterNode node) {
        assert node != null;

        return alive(node.id());
    }

    /**
     * @param nodeId ID of the node.
     * @return {@code True} if ping succeeded.
     * @throws IgniteClientDisconnectedCheckedException If ping failed.
     */
    public boolean pingNode(UUID nodeId) throws IgniteClientDisconnectedCheckedException {
        assert nodeId != null;

        if (!busyLock.enterBusy())
            return false;

        try {
            return getSpi().pingNode(nodeId);
        }
        catch (IgniteException e) {
            if (e.hasCause(IgniteClientDisconnectedCheckedException.class)) {
                IgniteFuture<?> reconnectFut = ctx.cluster().clientReconnectFuture();

                throw new IgniteClientDisconnectedCheckedException(reconnectFut, e.getMessage());
            }

            throw e;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId ID of the node.
     * @return {@code True} if ping succeeded.
     */
    public boolean pingNodeNoError(UUID nodeId) {
        assert nodeId != null;

        if (!busyLock.enterBusy())
            return false;

        try {
            return getSpi().pingNode(nodeId);
        }
        catch (IgniteException ignored) {
            return false;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId ID of the node.
     * @return Node for ID.
     */
    @Nullable public ClusterNode node(UUID nodeId) {
        assert nodeId != null;

        return discoCache().node(nodeId);
    }

    /**
     * Gets collection of node for given node IDs and predicates.
     *
     * @param ids Ids to include.
     * @param p Filter for IDs.
     * @return Collection with all alive nodes for given IDs.
     */
    public Collection<ClusterNode> nodes(@Nullable Collection<UUID> ids, IgnitePredicate<UUID>... p) {
        return F.isEmpty(ids) ? Collections.<ClusterNode>emptyList() :
            F.view(
                F.viewReadOnly(ids, U.id2Node(ctx), p),
                F.notNull());
    }

    /**
     * Gets topology hash for given set of nodes.
     *
     * @param nodes Subset of grid nodes for hashing.
     * @return Hash for given topology.
     */
    public long topologyHash(Iterable<? extends ClusterNode> nodes) {
        assert nodes != null;

        Iterator<? extends ClusterNode> iter = nodes.iterator();

        if (!iter.hasNext())
            return 0; // Special case.

        List<String> uids = new ArrayList<>();

        for (ClusterNode node : nodes)
            uids.add(node.id().toString());

        Collections.sort(uids);

        CRC32 hash = new CRC32();

        for (String uuid : uids)
            hash.update(uuid.getBytes());

        return hash.getValue();
    }

    /**
     * Gets future that will be completed when current topology version becomes greater or equal to argument passed.
     *
     * @param awaitVer Topology version to await.
     * @return Future.
     */
    public IgniteInternalFuture<Long> topologyFuture(final long awaitVer) {
        long topVer = topologyVersion();

        if (topVer >= awaitVer)
            return new GridFinishedFuture<>(topVer);

        DiscoTopologyFuture fut = new DiscoTopologyFuture(ctx, awaitVer);

        fut.init();

        return fut;
    }

    /**
     * Gets discovery collection cache from SPI safely guarding against "floating" collections.
     *
     * @return Discovery collection cache.
     */
    private DiscoCache discoCache() {
        Snapshot cur = topSnap.get();

        assert cur != null;

        return cur.discoCache;
    }

    /**
     * Gets discovery collection cache from SPI safely guarding against "floating" collections.
     *
     * @return Discovery collection cache.
     */
    public DiscoCache discoCache(AffinityTopologyVersion topVer) {
        return discoCacheHist.get(topVer);
    }

    /** @return All non-daemon remote nodes in topology. */
    public Collection<ClusterNode> remoteNodes() {
        return discoCache().remoteNodes();
    }

    /** @return All non-daemon nodes in topology. */
    public Collection<ClusterNode> allNodes() {
        return discoCache().allNodes();
    }

    /** @return Full topology size. */
    public int size() {
        return discoCache().allNodes().size();
    }

    /**
     * Gets all nodes for given topology version.
     *
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> nodes(long topVer) {
        return nodes(new AffinityTopologyVersion(topVer));
    }

    /**
     * Gets all nodes for given topology version.
     *
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> nodes(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(null), topVer).allNodes();
    }

    /**
     * @param topVer Topology version.
     * @return All server nodes for given topology version.
     */
    public List<ClusterNode> serverNodes(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(null), topVer).srvNodes;
    }

    /**
     * Gets node from history for given topology version.
     *
     * @param topVer Topology version.
     * @param id Node ID.
     * @return Node.
     */
    public ClusterNode node(AffinityTopologyVersion topVer, UUID id) {
        return resolveDiscoCache(CU.cacheId(null), topVer).node(id);
    }

    /**
     * Gets cache nodes for cache with given name.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> cacheNodes(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(cacheName), topVer).cacheNodes(cacheName, topVer.topologyVersion());
    }

    /**
     * Gets cache nodes for cache with given ID.
     *
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> cacheNodes(int cacheId, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(cacheId, topVer).cacheNodes(cacheId, topVer.topologyVersion());
    }

    /**
     * Gets all nodes with at least one cache configured.
     *
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> cacheNodes(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(null), topVer).allNodesWithCaches(topVer.topologyVersion());
    }

    /**
     * Gets cache remote nodes for cache with given name.
     *
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> remoteCacheNodes(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(null), topVer).remoteCacheNodes(topVer.topologyVersion());
    }

    /**
     * @param topVer Topology version (maximum allowed node order).
     * @return Oldest alive server nodes with at least one cache configured.
     */
    @Nullable public ClusterNode oldestAliveCacheServerNode(AffinityTopologyVersion topVer) {
        DiscoCache cache = resolveDiscoCache(CU.cacheId(null), topVer);

        Map.Entry<ClusterNode, Boolean> e = cache.aliveSrvNodesWithCaches.firstEntry();

        return e != null ? e.getKey() : null;
    }

    /**
     * Gets cache nodes for cache with given name that participate in affinity calculation.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Collection of cache affinity nodes.
     */
    public Collection<ClusterNode> cacheAffinityNodes(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        int cacheId = CU.cacheId(cacheName);

        return resolveDiscoCache(cacheId, topVer).cacheAffinityNodes(cacheId, topVer.topologyVersion());
    }

    /**
     * Gets cache nodes for cache with given ID that participate in affinity calculation.
     *
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     * @return Collection of cache affinity nodes.
     */
    public Collection<ClusterNode> cacheAffinityNodes(int cacheId, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(cacheId, topVer).cacheAffinityNodes(cacheId, topVer.topologyVersion());
    }

    /**
     * Checks if node is a data node for the given cache.
     *
     * @param node Node to check.
     * @param cacheName Cache name.
     * @return {@code True} if node is a cache data node.
     */
    public boolean cacheAffinityNode(ClusterNode node, String cacheName) {
        CachePredicate pred = registeredCaches.get(cacheName);

        return pred != null && pred.dataNode(node);
    }

    /**
     * @param node Node to check.
     * @param cacheName Cache name.
     * @return {@code True} if node has near cache enabled.
     */
    public boolean cacheNearNode(ClusterNode node, String cacheName) {
        CachePredicate pred = registeredCaches.get(cacheName);

        return pred != null && pred.nearNode(node);
    }

    /**
     * @param node Node to check.
     * @param cacheName Cache name.
     * @return {@code True} if node has client cache (without near cache).
     */
    public boolean cacheClientNode(ClusterNode node, String cacheName) {
        CachePredicate pred = registeredCaches.get(cacheName);

        return pred != null && pred.clientNode(node);
    }

    /**
     * @param node Node to check.
     * @param cacheName Cache name.
     * @return If cache with the given name is accessible on the given node.
     */
    public boolean cacheNode(ClusterNode node, String cacheName) {
        CachePredicate pred = registeredCaches.get(cacheName);

        return pred != null && pred.cacheNode(node);
    }

    /**
     * @param node Node to check.
     * @return Public cache names accessible on the given node.
     */
    public Map<String, CacheMode> nodeCaches(ClusterNode node) {
        Map<String, CacheMode> caches = U.newHashMap(registeredCaches.size());

        for (Map.Entry<String, CachePredicate> entry : registeredCaches.entrySet()) {
            String cacheName = entry.getKey();

            CachePredicate pred = entry.getValue();

            if (!CU.isSystemCache(cacheName) && !CU.isIgfsCache(ctx.config(), cacheName) &&
                pred != null && pred.cacheNode(node))
                caches.put(cacheName, pred.cacheMode);
        }

        return caches;
    }

    /**
     * Checks if cache with given ID has at least one node with near cache enabled.
     *
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     * @return {@code True} if cache with given name has at least one node with near cache enabled.
     */
    public boolean hasNearCache(int cacheId, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(cacheId, topVer).hasNearCache(cacheId);
    }

    /**
     * Gets discovery cache for given topology version.
     *
     * @param cacheId Cache ID (participates in exception message).
     * @param topVer Topology version.
     * @return Discovery cache.
     */
    private DiscoCache resolveDiscoCache(int cacheId, AffinityTopologyVersion topVer) {
        Snapshot snap = topSnap.get();

        DiscoCache cache = AffinityTopologyVersion.NONE.equals(topVer) || topVer.equals(snap.topVer) ?
            snap.discoCache : discoCacheHist.get(topVer);

        if (cache == null) {
            DynamicCacheDescriptor desc = ctx.cache().cacheDescriptor(cacheId);

            throw new IgniteException("Failed to resolve nodes topology [" +
                "cacheName=" + (desc != null ? desc.cacheConfiguration().getName() : "N/A") +
                ", topVer=" + topVer +
                ", history=" + discoCacheHist.keySet() +
                ", snap=" + snap +
                ", locNode=" + ctx.discovery().localNode() + ']');
        }

        return cache;
    }

    /**
     * Gets topology by specified version from history storage.
     *
     * @param topVer Topology version.
     * @return Topology nodes or {@code null} if there are no nodes for passed in version.
     */
    @Nullable public Collection<ClusterNode> topology(long topVer) {
        if (!histSupported)
            throw new UnsupportedOperationException("Current discovery SPI does not support " +
                "topology snapshots history (consider using TCP discovery SPI).");

        Map<Long, Collection<ClusterNode>> snapshots = topHist;

        return snapshots.get(topVer);
    }

    /** @return All daemon nodes in topology. */
    public Collection<ClusterNode> daemonNodes() {
        return discoCache().daemonNodes();
    }

    /** @return Local node. */
    public ClusterNode localNode() {
        return locNode == null ? getSpi().getLocalNode() : locNode;
    }

    /** @return Topology version. */
    public long topologyVersion() {
        return topSnap.get().topVer.topologyVersion();
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersionEx() {
        return topSnap.get().topVer;
    }

    /** @return Event that represents a local node joined to topology. */
    public DiscoveryEvent localJoinEvent() {
        try {
            return locJoinEvt.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param msg Custom message.
     * @throws IgniteCheckedException If failed.
     */
    public void sendCustomEvent(DiscoveryCustomMessage msg) throws IgniteCheckedException {
        try {
            getSpi().sendCustomEvent(new CustomMessageWrapper(msg));
        }
        catch (IgniteClientDisconnectedException e) {
            IgniteFuture<?> reconnectFut = ctx.cluster().clientReconnectFuture();

            throw new IgniteClientDisconnectedCheckedException(reconnectFut, e.getMessage());
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Gets first grid node start time, see {@link DiscoverySpi#getGridStartTime()}.
     *
     * @return Start time of the first grid node.
     */
    public long gridStartTime() {
        return getSpi().getGridStartTime();
    }

    /**
     * @param nodeId Node ID.
     * @param warning Warning message to be shown on all nodes.
     * @return Whether node is failed.
     */
    public boolean tryFailNode(UUID nodeId, @Nullable String warning) {
        if (!busyLock.enterBusy())
            return false;

        try {
            if (!getSpi().pingNode(nodeId)) {
                getSpi().failNode(nodeId, warning);

                return true;
            }

            return false;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId Node ID to fail.
     * @param warning Warning message to be shown on all nodes.
     */
    public void failNode(UUID nodeId, @Nullable String warning) {
        if (!busyLock.enterBusy())
            return;

        try {
            getSpi().failNode(nodeId, warning);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Updates topology version if current version is smaller than updated.
     *
     * @param updated Updated topology version.
     * @param discoCache Discovery cache.
     * @return {@code True} if topology was updated.
     */
    private boolean updateTopologyVersionIfGreater(AffinityTopologyVersion updated, DiscoCache discoCache) {
        while (true) {
            Snapshot cur = topSnap.get();

            if (updated.compareTo(cur.topVer) >= 0) {
                if (topSnap.compareAndSet(cur, new Snapshot(updated, discoCache)))
                    return true;
            }
            else
                return false;
        }
    }

    /** Stops local node. */
    private void stopNode() {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    ctx.markSegmented();

                    G.stop(ctx.gridName(), true);
                }
            }
        ).start();
    }

    /** Restarts JVM. */
    private void restartJvm() {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    ctx.markSegmented();

                    G.restart(true);
                }
            }
        ).start();
    }

    /** Worker for network segment checks. */
    private class SegmentCheckWorker extends GridWorker {
        /** */
        private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

        /**
         *
         */
        private SegmentCheckWorker() {
            super(ctx.gridName(), "disco-net-seg-chk-worker", GridDiscoveryManager.this.log);

            assert hasRslvrs;
            assert segChkFreq > 0;
        }

        /**
         *
         */
        public void scheduleSegmentCheck() {
            queue.add(new Object());
        }

        /** {@inheritDoc} */
        @SuppressWarnings("StatementWithEmptyBody")
        @Override protected void body() throws InterruptedException {
            long lastChk = 0;

            while (!isCancelled()) {
                Object req = queue.poll(2000, MILLISECONDS);

                long now = U.currentTimeMillis();

                // Check frequency if segment check has not been requested.
                if (req == null && (segChkFreq == 0 || lastChk + segChkFreq >= now)) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping segment check as it has not been requested and it is not time to check.");

                    continue;
                }

                // We should always check segment if it has been explicitly
                // requested (on any node failure or leave).
                assert req != null || lastChk + segChkFreq < now;

                // Drain queue.
                while (queue.poll() != null) {
                    // No-op.
                }

                if (lastSegChkRes.get()) {
                    boolean segValid = ctx.segmentation().isValidSegment();

                    lastChk = now;

                    if (!segValid) {
                        discoWrk.addEvent(EVT_NODE_SEGMENTED, AffinityTopologyVersion.NONE, getSpi().getLocalNode(),
                            Collections.<ClusterNode>emptyList(), null);

                        lastSegChkRes.set(false);
                    }

                    if (log.isDebugEnabled())
                        log.debug("Segment has been checked [requested=" + (req != null) + ", valid=" + segValid + ']');
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SegmentCheckWorker.class, this);
        }
    }

    /** Worker for discovery events. */
    private class DiscoveryWorker extends GridWorker {
        /** Event queue. */
        private final BlockingQueue<GridTuple5<Integer, AffinityTopologyVersion, ClusterNode, Collection<ClusterNode>,
            DiscoveryCustomMessage>> evts = new LinkedBlockingQueue<>();

        /** Node segmented event fired flag. */
        private boolean nodeSegFired;

        /**
         *
         */
        private DiscoveryWorker() {
            super(ctx.gridName(), "disco-event-worker", GridDiscoveryManager.this.log);
        }

        /**
         * Method is called when any discovery event occurs.
         *
         * @param type Discovery event type. See {@link DiscoveryEvent} for more details.
         * @param topVer Topology version.
         * @param node Remote node this event is connected with.
         * @param topSnapshot Topology snapshot.
         */
        @SuppressWarnings("RedundantTypeArguments")
        private void recordEvent(int type, long topVer, ClusterNode node, Collection<ClusterNode> topSnapshot) {
            assert node != null;

            if (ctx.event().isRecordable(type)) {
                DiscoveryEvent evt = new DiscoveryEvent();

                evt.node(ctx.discovery().localNode());
                evt.eventNode(node);
                evt.type(type);

                evt.topologySnapshot(topVer, U.<ClusterNode, ClusterNode>arrayList(topSnapshot, FILTER_DAEMON));

                if (type == EVT_NODE_METRICS_UPDATED)
                    evt.message("Metrics were updated: " + node);

                else if (type == EVT_NODE_JOINED)
                    evt.message("Node joined: " + node);

                else if (type == EVT_NODE_LEFT)
                    evt.message("Node left: " + node);

                else if (type == EVT_NODE_FAILED)
                    evt.message("Node failed: " + node);

                else if (type == EVT_NODE_SEGMENTED)
                    evt.message("Node segmented: " + node);

                else if (type == EVT_CLIENT_NODE_DISCONNECTED)
                    evt.message("Client node disconnected: " + node);

                else if (type == EVT_CLIENT_NODE_RECONNECTED)
                    evt.message("Client node reconnected: " + node);

                else
                    assert false;

                ctx.event().record(evt);
            }
        }

        /**
         * @param type Event type.
         * @param topVer Topology version.
         * @param node Node.
         * @param topSnapshot Topology snapshot.
         * @param data Custom message.
         */
        void addEvent(
            int type,
            AffinityTopologyVersion topVer,
            ClusterNode node,
            Collection<ClusterNode> topSnapshot,
            @Nullable DiscoveryCustomMessage data
        ) {
            assert node != null : data;

            evts.add(new GridTuple5<>(type, topVer, node, topSnapshot, data));
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            while (!isCancelled()) {
                try {
                    body0();
                }
                catch (InterruptedException e) {
                    throw e;
                }
                catch (Throwable t) {
                    U.error(log, "Unexpected exception in discovery worker thread (ignored).", t);

                    if (t instanceof Error)
                        throw (Error)t;
                }
            }
        }

        /** @throws InterruptedException If interrupted. */
        @SuppressWarnings("DuplicateCondition")
        private void body0() throws InterruptedException {
            GridTuple5<Integer, AffinityTopologyVersion, ClusterNode, Collection<ClusterNode>,
                DiscoveryCustomMessage> evt = evts.take();

            int type = evt.get1();

            AffinityTopologyVersion topVer = evt.get2();

            ClusterNode node = evt.get3();

            boolean isDaemon = node.isDaemon();

            boolean segmented = false;

            switch (type) {
                case EVT_NODE_JOINED: {
                    assert !discoOrdered || topVer.topologyVersion() == node.order() : "Invalid topology version [topVer=" + topVer +
                        ", node=" + node + ']';

                    try {
                        checkAttributes(F.asList(node));
                    }
                    catch (IgniteCheckedException e) {
                        U.warn(log, e.getMessage()); // We a have well-formed attribute warning here.
                    }

                    if (!isDaemon) {
                        if (!isLocDaemon) {
                            if (log.isInfoEnabled())
                                log.info("Added new node to topology: " + node);

                            ackTopology(topVer.topologyVersion(), true);
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Added new node to topology: " + node);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Added new daemon node to topology: " + node);

                    break;
                }

                case EVT_NODE_LEFT: {
                    // Check only if resolvers were configured.
                    if (hasRslvrs)
                        segChkWrk.scheduleSegmentCheck();

                    if (!isDaemon) {
                        if (!isLocDaemon) {
                            if (log.isInfoEnabled())
                                log.info("Node left topology: " + node);

                            ackTopology(topVer.topologyVersion(), true);
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Node left topology: " + node);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Daemon node left topology: " + node);

                    break;
                }

                case EVT_CLIENT_NODE_DISCONNECTED: {
                    // No-op.

                    break;
                }

                case EVT_CLIENT_NODE_RECONNECTED: {
                    if (log.isInfoEnabled())
                        log.info("Client node reconnected to topology: " + node);

                    if (!isLocDaemon)
                        ackTopology(topVer.topologyVersion(), true);

                    break;
                }

                case EVT_NODE_FAILED: {
                    // Check only if resolvers were configured.
                    if (hasRslvrs)
                        segChkWrk.scheduleSegmentCheck();

                    if (!isDaemon) {
                        if (!isLocDaemon) {
                            U.warn(log, "Node FAILED: " + node);

                            ackTopology(topVer.topologyVersion(), true);
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Node FAILED: " + node);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Daemon node FAILED: " + node);

                    break;
                }

                case EVT_NODE_SEGMENTED: {
                    assert F.eqNodes(localNode(), node);

                    if (nodeSegFired) {
                        if (log.isDebugEnabled()) {
                            log.debug("Ignored node segmented event [type=EVT_NODE_SEGMENTED, " +
                                "node=" + node + ']');
                        }

                        return;
                    }

                    // Ignore all further EVT_NODE_SEGMENTED events
                    // until EVT_NODE_RECONNECTED is fired.
                    nodeSegFired = true;

                    lastLoggedTop.set(0);

                    segmented = true;

                    if (!isLocDaemon)
                        U.warn(log, "Local node SEGMENTED: " + node);
                    else if (log.isDebugEnabled())
                        log.debug("Local node SEGMENTED: " + node);

                    break;
                }

                case DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT: {
                    if (ctx.event().isRecordable(DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT)) {
                        DiscoveryCustomEvent customEvt = new DiscoveryCustomEvent();

                        customEvt.node(ctx.discovery().localNode());
                        customEvt.eventNode(node);
                        customEvt.type(type);
                        customEvt.topologySnapshot(topVer.topologyVersion(), evt.get4());
                        customEvt.affinityTopologyVersion(topVer);
                        customEvt.customMessage(evt.get5());

                        ctx.event().record(customEvt);
                    }

                    return;
                }

                // Don't log metric update to avoid flooding the log.
                case EVT_NODE_METRICS_UPDATED:
                    break;

                default:
                    assert false : "Invalid discovery event: " + type;
            }

            recordEvent(type, topVer.topologyVersion(), node, evt.get4());

            if (segmented)
                onSegmentation();
        }

        /**
         *
         */
        private void onSegmentation() {
            SegmentationPolicy segPlc = ctx.config().getSegmentationPolicy();

            // Always disconnect first.
            try {
                getSpi().disconnect();
            }
            catch (IgniteSpiException e) {
                U.error(log, "Failed to disconnect discovery SPI.", e);
            }

            switch (segPlc) {
                case RESTART_JVM:
                    U.warn(log, "Restarting JVM according to configured segmentation policy.");

                    restartJvm();

                    break;

                case STOP:
                    U.warn(log, "Stopping local node according to configured segmentation policy.");

                    stopNode();

                    break;

                default:
                    assert segPlc == NOOP : "Unsupported segmentation policy value: " + segPlc;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DiscoveryWorker.class, this);
        }
    }

    /**
     *
     */
    private class MetricsUpdater implements Runnable {
        /** */
        private long prevGcTime = -1;

        /** */
        private long prevCpuTime = -1;

        /** {@inheritDoc} */
        @Override public void run() {
            gcCpuLoad = getGcCpuLoad();
            cpuLoad = getCpuLoad();
        }

        /**
         * @return GC CPU load.
         */
        private double getGcCpuLoad() {
            long gcTime = 0;

            for (GarbageCollectorMXBean bean : gc) {
                long colTime = bean.getCollectionTime();

                if (colTime > 0)
                    gcTime += colTime;
            }

            gcTime /= metrics.getAvailableProcessors();

            double gc = 0;

            if (prevGcTime > 0) {
                long gcTimeDiff = gcTime - prevGcTime;

                gc = (double)gcTimeDiff / METRICS_UPDATE_FREQ;
            }

            prevGcTime = gcTime;

            return gc;
        }

        /**
         * @return CPU load.
         */
        private double getCpuLoad() {
            long cpuTime;

            try {
                cpuTime = U.<Long>property(os, "processCpuTime");
            }
            catch (IgniteException ignored) {
                return -1;
            }

            // Method reports time in nanoseconds across all processors.
            cpuTime /= 1000000 * metrics.getAvailableProcessors();

            double cpu = 0;

            if (prevCpuTime > 0) {
                long cpuTimeDiff = cpuTime - prevCpuTime;

                // CPU load could go higher than 100% because calculating of cpuTimeDiff also takes some time.
                cpu = Math.min(1.0, (double)cpuTimeDiff / METRICS_UPDATE_FREQ);
            }

            prevCpuTime = cpuTime;

            return cpu;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MetricsUpdater.class, this, super.toString());
        }
    }

    /** Discovery topology future. */
    private static class DiscoTopologyFuture extends GridFutureAdapter<Long> implements GridLocalEventListener {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private GridKernalContext ctx;

        /** Topology await version. */
        private long awaitVer;

        /**
         * @param ctx Context.
         * @param awaitVer Await version.
         */
        private DiscoTopologyFuture(GridKernalContext ctx, long awaitVer) {
            this.ctx = ctx;
            this.awaitVer = awaitVer;
        }

        /** Initializes future. */
        private void init() {
            ctx.event().addLocalEventListener(this, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);

            // Close potential window.
            long topVer = ctx.discovery().topologyVersion();

            if (topVer >= awaitVer)
                onDone(topVer);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Long res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                ctx.event().removeLocalEventListener(this, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            if (discoEvt.topologyVersion() >= awaitVer)
                onDone(discoEvt.topologyVersion());
        }
    }

    /**
     *
     */
    private static class Snapshot {
        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        @GridToStringExclude
        private final DiscoCache discoCache;

        /**
         * @param topVer Topology version.
         * @param discoCache Disco cache.
         */
        private Snapshot(AffinityTopologyVersion topVer, DiscoCache discoCache) {
            this.topVer = topVer;
            this.discoCache = discoCache;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Snapshot.class, this);
        }
    }

    /** Cache for discovery collections. */
    private class DiscoCache {
        /** Remote nodes. */
        private final List<ClusterNode> rmtNodes;

        /** All nodes. */
        private final List<ClusterNode> allNodes;

        /** All server nodes. */
        private final List<ClusterNode> srvNodes;

        /** All nodes with at least one cache configured. */
        @GridToStringInclude
        private final Collection<ClusterNode> allNodesWithCaches;

        /** All nodes with at least one cache configured. */
        @GridToStringInclude
        private final Collection<ClusterNode> rmtNodesWithCaches;

        /** Cache nodes by cache name. */
        @GridToStringInclude
        private final Map<Integer, Collection<ClusterNode>> allCacheNodes;

        /** Cache nodes by cache name. */
        @GridToStringInclude
        private final Map<Integer, Collection<ClusterNode>> affCacheNodes;

        /** Caches where at least one node has near cache enabled. */
        @GridToStringInclude
        private final Set<Integer> nearEnabledCaches;

        /** Nodes grouped by version. */
        private final NavigableMap<IgniteProductVersion, Collection<ClusterNode>> nodesByVer;

        /** Daemon nodes. */
        private final List<ClusterNode> daemonNodes;

        /** Node map. */
        private final Map<UUID, ClusterNode> nodeMap;

        /** Local node. */
        private final ClusterNode loc;

        /** Highest node order. */
        private final long maxOrder;

        /**
         * Cached alive server remote nodes with caches.
         */
        private final ConcurrentSkipListMap<ClusterNode, Boolean> aliveSrvNodesWithCaches;

        /**
         * @param loc Local node.
         * @param rmts Remote nodes.
         */
        private DiscoCache(ClusterNode loc, Collection<ClusterNode> rmts) {
            this.loc = loc;

            rmtNodes = Collections.unmodifiableList(new ArrayList<>(F.view(rmts, FILTER_DAEMON)));

            assert !rmtNodes.contains(loc) : "Remote nodes collection shouldn't contain local node" +
                " [rmtNodes=" + rmtNodes + ", loc=" + loc + ']';

            List<ClusterNode> all = new ArrayList<>(rmtNodes.size() + 1);

            if (!loc.isDaemon())
                all.add(loc);

            all.addAll(rmtNodes);

            Collections.sort(all, GridNodeOrderComparator.INSTANCE);

            allNodes = Collections.unmodifiableList(all);

            Map<Integer, Collection<ClusterNode>> cacheMap = U.newHashMap(allNodes.size());
            Map<Integer, Collection<ClusterNode>> dhtNodesMap = U.newHashMap(allNodes.size());
            Collection<ClusterNode> nodesWithCaches = U.newHashSet(allNodes.size());
            Collection<ClusterNode> rmtNodesWithCaches = U.newHashSet(allNodes.size());

            aliveSrvNodesWithCaches = new ConcurrentSkipListMap<>(GridNodeOrderComparator.INSTANCE);
            nodesByVer = new TreeMap<>();

            long maxOrder0 = 0;

            Set<Integer> nearEnabledSet = new HashSet<>();

            List<ClusterNode> srvNodes = new ArrayList<>();

            for (ClusterNode node : allNodes) {
                assert node.order() != 0 : "Invalid node order [locNode=" + loc + ", node=" + node + ']';
                assert !node.isDaemon();

                if (!CU.clientNode(node))
                    srvNodes.add(node);

                if (node.order() > maxOrder0)
                    maxOrder0 = node.order();

                boolean hasCaches = false;

                for (Map.Entry<String, CachePredicate> entry : registeredCaches.entrySet()) {
                    String cacheName = entry.getKey();

                    CachePredicate filter = entry.getValue();

                    if (filter.cacheNode(node)) {
                        nodesWithCaches.add(node);

                        if (!loc.id().equals(node.id()))
                            rmtNodesWithCaches.add(node);

                        addToMap(cacheMap, cacheName, node);

                        if (filter.dataNode(node))
                            addToMap(dhtNodesMap, cacheName, node);

                        if (filter.nearNode(node))
                            nearEnabledSet.add(CU.cacheId(cacheName));

                        hasCaches = true;
                    }
                }

                if (hasCaches && alive(node.id()) && !CU.clientNode(node))
                    aliveSrvNodesWithCaches.put(node, Boolean.TRUE);

                IgniteProductVersion nodeVer = U.productVersion(node);

                // Create collection for this version if it does not exist.
                Collection<ClusterNode> nodes = nodesByVer.get(nodeVer);

                if (nodes == null) {
                    nodes = new ArrayList<>(allNodes.size());

                    nodesByVer.put(nodeVer, nodes);
                }

                nodes.add(node);
            }

            Collections.sort(srvNodes, CU.nodeComparator(true));

            // Need second iteration to add this node to all previous node versions.
            for (ClusterNode node : allNodes) {
                IgniteProductVersion nodeVer = U.productVersion(node);

                // Get all versions lower or equal node's version.
                NavigableMap<IgniteProductVersion, Collection<ClusterNode>> updateView =
                    nodesByVer.headMap(nodeVer, false);

                for (Collection<ClusterNode> prevVersions : updateView.values())
                    prevVersions.add(node);
            }

            maxOrder = maxOrder0;

            allCacheNodes = Collections.unmodifiableMap(cacheMap);
            affCacheNodes = Collections.unmodifiableMap(dhtNodesMap);
            allNodesWithCaches = Collections.unmodifiableCollection(nodesWithCaches);
            this.rmtNodesWithCaches = Collections.unmodifiableCollection(rmtNodesWithCaches);
            nearEnabledCaches = Collections.unmodifiableSet(nearEnabledSet);
            this.srvNodes = Collections.unmodifiableList(srvNodes);

            daemonNodes = Collections.unmodifiableList(new ArrayList<>(
                F.view(F.concat(false, loc, rmts), F0.not(FILTER_DAEMON))));

            Map<UUID, ClusterNode> nodeMap = U.newHashMap(allNodes().size() + daemonNodes.size());

            for (ClusterNode n : F.concat(false, allNodes(), daemonNodes()))
                nodeMap.put(n.id(), n);

            this.nodeMap = nodeMap;
        }

        /**
         * Adds node to map.
         *
         * @param cacheMap Map to add to.
         * @param cacheName Cache name.
         * @param rich Node to add
         */
        private void addToMap(Map<Integer, Collection<ClusterNode>> cacheMap, String cacheName, ClusterNode rich) {
            Collection<ClusterNode> cacheNodes = cacheMap.get(CU.cacheId(cacheName));

            if (cacheNodes == null) {
                cacheNodes = new ArrayList<>(allNodes.size());

                cacheMap.put(CU.cacheId(cacheName), cacheNodes);
            }

            cacheNodes.add(rich);
        }

        /** @return Local node. */
        ClusterNode localNode() {
            return loc;
        }

        /** @return Remote nodes. */
        Collection<ClusterNode> remoteNodes() {
            return rmtNodes;
        }

        /** @return All nodes. */
        Collection<ClusterNode> allNodes() {
            return allNodes;
        }

        /**
         * Gets collection of nodes with at least one cache configured.
         *
         * @param topVer Topology version (maximum allowed node order).
         * @return Collection of nodes.
         */
        Collection<ClusterNode> allNodesWithCaches(final long topVer) {
            return filter(topVer, allNodesWithCaches);
        }

        /**
         * Gets all nodes that have cache with given name.
         *
         * @param cacheName Cache name.
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> cacheNodes(@Nullable String cacheName, final long topVer) {
            return filter(topVer, allCacheNodes.get(CU.cacheId(cacheName)));
        }

        /**
         * Gets all nodes that have cache with given ID.
         *
         * @param cacheId Cache ID.
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> cacheNodes(Integer cacheId, final long topVer) {
            return filter(topVer, allCacheNodes.get(cacheId));
        }

        /**
         * Gets all remote nodes that have at least one cache configured.
         *
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> remoteCacheNodes(final long topVer) {
            return filter(topVer, rmtNodesWithCaches);
        }

        /**
         * Gets all nodes that have cache with given ID and should participate in affinity calculation. With
         * partitioned cache nodes with near-only cache do not participate in affinity node calculation.
         *
         * @param cacheId Cache ID.
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> cacheAffinityNodes(int cacheId, final long topVer) {
            return filter(topVer, affCacheNodes.get(cacheId));
        }

        /**
         * Checks if cache with given ID has at least one node with near cache enabled.
         *
         * @param cacheId Cache ID.
         * @return {@code True} if cache with given name has at least one node with near cache enabled.
         */
        boolean hasNearCache(int cacheId) {
            return nearEnabledCaches.contains(cacheId);
        }

        /**
         * Removes left node from cached alives lists.
         *
         * @param leftNode Left node.
         */
        void updateAlives(ClusterNode leftNode) {
            if (leftNode.order() > maxOrder)
                return;

            aliveSrvNodesWithCaches.remove(leftNode);
        }

        /**
         * @param topVer Topology version.
         * @param nodes Nodes.
         * @return Filtered collection (potentially empty, but never {@code null}).
         */
        private Collection<ClusterNode> filter(final long topVer, @Nullable Collection<ClusterNode> nodes) {
            if (nodes == null)
                return Collections.emptyList();

            // If no filtering needed, return original collection.
            return nodes.isEmpty() || topVer < 0 || topVer >= maxOrder ?
                nodes :
                F.view(nodes, new P1<ClusterNode>() {
                    @Override public boolean apply(ClusterNode node) {
                        return node.order() <= topVer;
                    }
                });
        }

        /** @return Daemon nodes. */
        Collection<ClusterNode> daemonNodes() {
            return daemonNodes;
        }

        /**
         * @param id Node ID.
         * @return Node.
         */
        @Nullable ClusterNode node(UUID id) {
            return nodeMap.get(id);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DiscoCache.class, this, "allNodesWithDaemons", U.toShortString(allNodes));
        }
    }

    /**
     * Cache predicate.
     */
    private static class CachePredicate {
        /** Cache filter. */
        private final IgnitePredicate<ClusterNode> cacheFilter;

        /** If near cache is enabled on data nodes. */
        private final boolean nearEnabled;

        /** Cache mode. */
        private final CacheMode cacheMode;

        /** Collection of client near nodes. */
        private final ConcurrentHashMap<UUID, Boolean> clientNodes;

        /**
         * @param cacheFilter Cache filter.
         * @param nearEnabled Near enabled flag.
         * @param cacheMode Cache mode.
         */
        private CachePredicate(IgnitePredicate<ClusterNode> cacheFilter, boolean nearEnabled, CacheMode cacheMode) {
            assert cacheFilter != null;

            this.cacheFilter = cacheFilter;
            this.nearEnabled = nearEnabled;
            this.cacheMode = cacheMode;

            clientNodes = new ConcurrentHashMap<>();
        }

        /**
         * @param nodeId Near node ID to add.
         * @param nearEnabled Near enabled flag.
         * @return {@code True} if new node ID was added.
         */
        public boolean addClientNode(UUID nodeId, boolean nearEnabled) {
            assert nodeId != null;

            Boolean old = clientNodes.putIfAbsent(nodeId, nearEnabled);

            return old == null;
        }

        /**
         * @param leftNodeId Left node ID.
         * @return {@code True} if existing node ID was removed.
         */
        public boolean onNodeLeft(UUID leftNodeId) {
            assert leftNodeId != null;

            Boolean old = clientNodes.remove(leftNodeId);

            return old != null;
        }

        /**
         * @param node Node to check.
         * @return {@code True} if this node is a data node for given cache.
         */
        public boolean dataNode(ClusterNode node) {
            return !node.isDaemon() && CU.affinityNode(node, cacheFilter);
        }

        /**
         * @param node Node to check.
         * @return {@code True} if cache is accessible on the given node.
         */
        public boolean cacheNode(ClusterNode node) {
            return !node.isDaemon() && (CU.affinityNode(node, cacheFilter) || clientNodes.containsKey(node.id()));
        }

        /**
         * @param node Node to check.
         * @return {@code True} if near cache is present on the given nodes.
         */
        public boolean nearNode(ClusterNode node) {
            if (node.isDaemon())
                return false;

            if (CU.affinityNode(node, cacheFilter))
                return nearEnabled;

            Boolean near = clientNodes.get(node.id());

            return near != null && near;
        }

        /**
         * @param node Node to check.
         * @return {@code True} if client cache is present on the given nodes.
         */
        public boolean clientNode(ClusterNode node) {
            if (node.isDaemon())
                return false;

            Boolean near = clientNodes.get(node.id());

            return near != null && !near;
        }
    }
}
