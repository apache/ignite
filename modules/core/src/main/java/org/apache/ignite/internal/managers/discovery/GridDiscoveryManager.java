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

import java.io.Externalizable;
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
import java.util.Comparator;
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
import java.util.concurrent.ConcurrentSkipListSet;
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
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.jobmetrics.GridJobMetrics;
import org.apache.ignite.internal.processors.security.SecurityContext;
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
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PEER_CLASSLOADING;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_USER_NAME;
import static org.apache.ignite.internal.IgniteVersionUtils.VER;
import static org.apache.ignite.plugin.segmentation.SegmentationPolicy.NOOP;

/**
 * Discovery SPI manager.
 */
public class GridDiscoveryManager extends GridManagerAdapter<DiscoverySpi> {
    /** Fake key for {@code null}-named caches. Used inside {@link DiscoCache}. */
    private static final String NULL_CACHE_NAME = UUID.randomUUID().toString();

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
    protected static final int DISCOVERY_HISTORY_SIZE = 100;

    /** Predicate filtering out daemon nodes. */
    private static final IgnitePredicate<ClusterNode> daemonFilter = new P1<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            return !n.isDaemon();
        }
    };

    /** Predicate filtering client nodes. */
    private static final IgnitePredicate<ClusterNode> clientFilter = new P1<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            return CU.clientNode(n);
        }
    };

    /** Disco history entries comparator. */
    private static final Comparator<Map.Entry<AffinityTopologyVersion, DiscoCache>> histCmp =
        new Comparator<Map.Entry<AffinityTopologyVersion, DiscoCache>>() {
            @Override public int compare(Map.Entry<AffinityTopologyVersion, DiscoCache> o1, Map.Entry<AffinityTopologyVersion, DiscoCache> o2) {
                return o1.getKey().compareTo(o2.getKey());
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
    private final Map<AffinityTopologyVersion, DiscoCache> discoCacheHist =
        new GridBoundedConcurrentLinkedHashMap<>(DISCOVERY_HISTORY_SIZE, DISCOVERY_HISTORY_SIZE, 0.7f, 1);

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
     * @param loc {@code True} if cache is local.
     */
    public void setCacheFilter(
        String cacheName,
        IgnitePredicate<ClusterNode> filter,
        boolean nearEnabled,
        boolean loc
    ) {
        if (!registeredCaches.containsKey(cacheName))
            registeredCaches.put(cacheName, new CachePredicate(filter, nearEnabled, loc));
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
                if (type == EVT_NODE_JOINED && node.isLocal() && ctx.clientDisconnected()) {
                    discoCacheHist.clear();

                    topHist.clear();

                    topSnap.set(new Snapshot(AffinityTopologyVersion.ZERO, null));
                }

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
                else if (type == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                    assert customMsg != null;

                    if (customMsg.incrementMinorTopologyVersion()) {
                        minorTopVer++;

                        verChanged = true;
                    }
                    else
                        verChanged = false;
                }
                else {
                    if (type != EVT_NODE_SEGMENTED &&
                        type != EVT_CLIENT_NODE_DISCONNECTED &&
                        type != EVT_CLIENT_NODE_RECONNECTED) {
                        minorTopVer = 0;

                        verChanged = true;
                    }
                    else
                        verChanged = false;
                }

                final AffinityTopologyVersion nextTopVer = new AffinityTopologyVersion(topVer, minorTopVer);

                if (type == EVT_NODE_FAILED || type == EVT_NODE_LEFT) {
                    for (DiscoCache c : discoCacheHist.values())
                        c.updateAlives(node);

                    updateClientNodes(node.id());
                }

                if (type == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                    for (Class cls = customMsg.getClass(); cls != null; cls = cls.getSuperclass()) {
                        List<CustomEventListener<DiscoveryCustomMessage>> list = customEvtLsnrs.get(cls);

                        if (list != null) {
                            for (CustomEventListener<DiscoveryCustomMessage> lsnr : list) {
                                try {
                                    lsnr.onCustomEvent(node, customMsg);
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
                        new DiscoCache(localNode(), getSpi().getRemoteNodes()));

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
                        }, daemonFilter)));

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
                    else
                        U.warn(log, "Received discovery data for unknown component: " + e.getKey());
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
                nm.setNonHeapMemoryUsed(metrics.getNonHeapMemoryUsed());
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

                        metrics.put(cache.context().cacheId(), cache.metrics());
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
                LT.warn(log, null, "Failed to check network segment (retrying every 2000 ms).");

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

        boolean warned = false;

        for (ClusterNode n : nodes) {
            int rmtJvmMajVer = nodeJavaMajorVersion(n);

            if (locJvmMajVer != rmtJvmMajVer)
                throw new IgniteCheckedException("Local node's java major version is different from remote node's one" +
                    " [locJvmMajVer=" + locJvmMajVer + ", rmtJvmMajVer=" + rmtJvmMajVer + "]");

            String rmtPreferIpV4 = n.attribute("java.net.preferIPv4Stack");

            if (!F.eq(rmtPreferIpV4, locPreferIpV4)) {
                if (!warned)
                    U.warn(log, "Local node's value of 'java.net.preferIPv4Stack' " +
                        "system property differs from remote node's " +
                        "(all nodes in topology should have identical value) " +
                        "[locPreferIpV4=" + locPreferIpV4 + ", rmtPreferIpV4=" + rmtPreferIpV4 +
                        ", locId8=" + U.id8(locNode.id()) + ", rmtId8=" + U.id8(n.id()) +
                        ", rmtAddrs=" + U.addressesAsString(n) + ']',
                        "Local and remote 'java.net.preferIPv4Stack' system properties do not match.");

                warned = true;
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
     */
    public void ackTopology() {
        ackTopology(topSnap.get().topVer.topologyVersion(), false);
    }

    /**
     * Logs grid size for license compliance.
     *
     * @param topVer Topology version.
     * @param throttle Suppress printing if this topology was already printed.
     */
    private void ackTopology(long topVer, boolean throttle) {
        assert !isLocDaemon;

        DiscoCache discoCache = discoCache();

        Collection<ClusterNode> rmtNodes = discoCache.remoteNodes();

        Collection<ClusterNode> srvNodes = F.view(discoCache.allNodes(), F.not(clientFilter));

        Collection<ClusterNode> clientNodes = F.view(discoCache.allNodes(), clientFilter);

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
            U.quiet(false, topologySnapshotMessage(srvNodes.size(), clientNodes.size(), totalCpus, heap));

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
            log.info(topologySnapshotMessage(srvNodes.size(), clientNodes.size(), totalCpus, heap));
    }

    /**
     * @param srvNodesNum Server nodes number.
     * @param clientNodesNum Client nodes number.
     * @param totalCpus Total cpu number.
     * @param heap Heap size.
     * @return Topology snapshot message.
     */
    private String topologySnapshotMessage(int srvNodesNum, int clientNodesNum, int totalCpus, double heap) {
        return PREFIX + " [" +
            (discoOrdered ? "ver=" + topSnap.get().topVer.topologyVersion() + ", " : "") +
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
        catch (IgniteException e) {
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
    public DiscoCache discoCache() {
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

    /**
     * Gets topology grouped by node versions.
     *
     * @return Version to collection of nodes map.
     */
    public NavigableMap<IgniteProductVersion, Collection<ClusterNode>> topologyVersionMap() {
        return discoCache().versionsMap();
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
        return resolveDiscoCache(null, new AffinityTopologyVersion(topVer)).allNodes();
    }

    /**
     * Gets cache nodes for cache with given name.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> cacheNodes(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(cacheName, topVer).cacheNodes(cacheName, topVer.topologyVersion());
    }

    /**
     * Gets all nodes with at least one cache configured.
     *
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> cacheNodes(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(null, topVer).allNodesWithCaches(topVer.topologyVersion());
    }

    /**
     * Gets cache remote nodes for cache with given name.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> remoteCacheNodes(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(cacheName, topVer).remoteCacheNodes(cacheName, topVer.topologyVersion());
    }

    /**
     * Gets cache remote nodes for cache with given name.
     *
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> remoteCacheNodes(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(null, topVer).remoteCacheNodes(topVer.topologyVersion());
    }

    /**
     * Gets cache nodes for cache with given name.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> aliveCacheNodes(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(cacheName, topVer).aliveCacheNodes(cacheName, topVer.topologyVersion());
    }

    /**
     * Gets cache remote nodes for cache with given name.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> aliveRemoteCacheNodes(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(cacheName, topVer).aliveRemoteCacheNodes(cacheName, topVer.topologyVersion());
    }

    /**
     * Gets alive remote server nodes with at least one cache configured.
     *
     * @param topVer Topology version (maximum allowed node order).
     * @return Collection of alive cache nodes.
     */
    public Collection<ClusterNode> aliveRemoteServerNodesWithCaches(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(null, topVer).aliveRemoteServerNodesWithCaches(topVer.topologyVersion());
    }

    /**
     * Gets alive server nodes with at least one cache configured.
     *
     * @param topVer Topology version (maximum allowed node order).
     * @return Collection of alive cache nodes.
     */
    public Collection<ClusterNode> aliveServerNodesWithCaches(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(null, topVer).aliveServerNodesWithCaches(topVer.topologyVersion());
    }

    /**
     * Gets alive nodes with at least one cache configured.
     *
     * @param topVer Topology version (maximum allowed node order).
     * @return Collection of alive cache nodes.
     */
    public Collection<ClusterNode> aliveNodesWithCaches(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(null, topVer).aliveNodesWithCaches(topVer.topologyVersion());
    }

    /**
     * Gets cache nodes for cache with given name that participate in affinity calculation.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Collection of cache affinity nodes.
     */
    public Collection<ClusterNode> cacheAffinityNodes(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(cacheName, topVer).cacheAffinityNodes(cacheName, topVer.topologyVersion());
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
     * Checks if cache with given name has at least one node with near cache enabled.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return {@code True} if cache with given name has at least one node with near cache enabled.
     */
    public boolean hasNearCache(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(cacheName, topVer).hasNearCache(cacheName);
    }

    /**
     * Gets discovery cache for given topology version.
     *
     * @param cacheName Cache name (participates in exception message).
     * @param topVer Topology version.
     * @return Discovery cache.
     */
    private DiscoCache resolveDiscoCache(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        Snapshot snap = topSnap.get();

        DiscoCache cache = AffinityTopologyVersion.NONE.equals(topVer) || topVer.equals(snap.topVer) ?
            snap.discoCache : discoCacheHist.get(topVer);

        if (cache == null) {
            // Find the eldest acceptable discovery cache.
            Map.Entry<AffinityTopologyVersion, DiscoCache> eldest = Collections.min(discoCacheHist.entrySet(), histCmp);

            if (topVer.compareTo(eldest.getKey()) < 0)
                cache = eldest.getValue();
        }

        if (cache == null) {
            throw new IgniteException("Failed to resolve nodes topology [cacheName=" + cacheName +
                ", topVer=" + topVer + ", history=" + discoCacheHist.keySet() +
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

                evt.topologySnapshot(topVer, U.<ClusterNode, ClusterNode>arrayList(topSnapshot, daemonFilter));

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

            evts.add(F.t(type, topVer, node, topSnapshot, data));
        }

        /**
         * @param node Node to get a short description for.
         * @return Short description for the node to be used in 'quiet' mode.
         */
        private String quietNode(ClusterNode node) {
            assert node != null;

            return "nodeId8=" + node.id().toString().substring(0, 8) + ", " +
                "addrs=" + U.addressesAsString(node) + ", " +
                "order=" + node.order() + ", " +
                "CPUs=" + node.metrics().getTotalCpus();
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
                        customEvt.topologySnapshot(topVer.topologyVersion(), null);
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

        /** Empty constructor required by {@link Externalizable}. */
        private DiscoTopologyFuture() {
            // No-op.
        }

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

        /** All nodes with at least one cache configured. */
        @GridToStringInclude
        private final Collection<ClusterNode> allNodesWithCaches;

        /** All nodes with at least one cache configured. */
        @GridToStringInclude
        private final Collection<ClusterNode> rmtNodesWithCaches;

        /** Cache nodes by cache name. */
        @GridToStringInclude
        private final Map<String, Collection<ClusterNode>> allCacheNodes;

        /** Remote cache nodes by cache name. */
        @GridToStringInclude
        private final Map<String, Collection<ClusterNode>> rmtCacheNodes;

        /** Cache nodes by cache name. */
        @GridToStringInclude
        private final Map<String, Collection<ClusterNode>> affCacheNodes;

        /** Caches where at least one node has near cache enabled. */
        @GridToStringInclude
        private final Set<String> nearEnabledCaches;

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
         * Cached alive nodes list. As long as this collection doesn't accept {@code null}s use {@link
         * #maskNull(String)} before passing raw cache names to it.
         */
        private final ConcurrentMap<String, Collection<ClusterNode>> aliveCacheNodes;

        /**
         * Cached alive remote nodes list. As long as this collection doesn't accept {@code null}s use {@link
         * #maskNull(String)} before passing raw cache names to it.
         */
        private final ConcurrentMap<String, Collection<ClusterNode>> aliveRmtCacheNodes;

        /**
         * Cached alive remote nodes with caches.
         */
        private final Collection<ClusterNode> aliveNodesWithCaches;

        /**
         * Cached alive server remote nodes with caches.
         */
        private final Collection<ClusterNode> aliveSrvNodesWithCaches;

        /**
         * Cached alive remote server nodes with caches.
         */
        private final Collection<ClusterNode> aliveRmtSrvNodesWithCaches;

        /**
         * @param loc Local node.
         * @param rmts Remote nodes.
         */
        private DiscoCache(ClusterNode loc, Collection<ClusterNode> rmts) {
            this.loc = loc;

            rmtNodes = Collections.unmodifiableList(new ArrayList<>(F.view(rmts, daemonFilter)));

            assert !rmtNodes.contains(loc) : "Remote nodes collection shouldn't contain local node" +
                " [rmtNodes=" + rmtNodes + ", loc=" + loc + ']';

            List<ClusterNode> all = new ArrayList<>(rmtNodes.size() + 1);

            if (!loc.isDaemon())
                all.add(loc);

            all.addAll(rmtNodes);

            Collections.sort(all, GridNodeOrderComparator.INSTANCE);

            allNodes = Collections.unmodifiableList(all);

            Map<String, Collection<ClusterNode>> cacheMap = new HashMap<>(allNodes.size(), 1.0f);
            Map<String, Collection<ClusterNode>> rmtCacheMap = new HashMap<>(allNodes.size(), 1.0f);
            Map<String, Collection<ClusterNode>> dhtNodesMap =new HashMap<>(allNodes.size(), 1.0f);
            Collection<ClusterNode> nodesWithCaches = new HashSet<>(allNodes.size());
            Collection<ClusterNode> rmtNodesWithCaches = new HashSet<>(allNodes.size());

            aliveCacheNodes = new ConcurrentHashMap8<>(allNodes.size(), 1.0f);
            aliveRmtCacheNodes = new ConcurrentHashMap8<>(allNodes.size(), 1.0f);
            aliveNodesWithCaches = new ConcurrentSkipListSet<>();
            aliveSrvNodesWithCaches = new ConcurrentSkipListSet<>();
            aliveRmtSrvNodesWithCaches = new ConcurrentSkipListSet<>();
            nodesByVer = new TreeMap<>();

            long maxOrder0 = 0;

            Set<String> nearEnabledSet = new HashSet<>();

            for (ClusterNode node : allNodes) {
                assert node.order() != 0 : "Invalid node order [locNode=" + loc + ", node=" + node + ']';

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

                        if (alive(node.id()))
                            addToMap(aliveCacheNodes, maskNull(cacheName), node);

                        if (filter.dataNode(node))
                            addToMap(dhtNodesMap, cacheName, node);

                        if (filter.nearNode(node))
                            nearEnabledSet.add(cacheName);

                        if (!loc.id().equals(node.id())) {
                            addToMap(rmtCacheMap, cacheName, node);

                            if (alive(node.id()))
                                addToMap(aliveRmtCacheNodes, maskNull(cacheName), node);
                        }

                        hasCaches = true;
                    }
                }

                if (hasCaches) {
                    if (alive(node.id())) {
                        aliveNodesWithCaches.add(node);

                        if (!CU.clientNode(node)) {
                            aliveSrvNodesWithCaches.add(node);

                            if (!loc.id().equals(node.id()))
                                aliveRmtSrvNodesWithCaches.add(node);
                        }
                    }
                }

                IgniteProductVersion nodeVer = U.productVersion(node);

                // Create collection for this version if it does not exist.
                Collection<ClusterNode> nodes = nodesByVer.get(nodeVer);

                if (nodes == null) {
                    nodes = new ArrayList<>(allNodes.size());

                    nodesByVer.put(nodeVer, nodes);
                }

                nodes.add(node);
            }

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
            rmtCacheNodes = Collections.unmodifiableMap(rmtCacheMap);
            affCacheNodes = Collections.unmodifiableMap(dhtNodesMap);
            allNodesWithCaches = Collections.unmodifiableCollection(nodesWithCaches);
            this.rmtNodesWithCaches = Collections.unmodifiableCollection(rmtNodesWithCaches);
            nearEnabledCaches = Collections.unmodifiableSet(nearEnabledSet);

            daemonNodes = Collections.unmodifiableList(new ArrayList<>(
                F.view(F.concat(false, loc, rmts), F0.not(daemonFilter))));

            Map<UUID, ClusterNode> nodeMap = new HashMap<>(allNodes().size() + daemonNodes.size(), 1.0f);

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
        private void addToMap(Map<String, Collection<ClusterNode>> cacheMap, String cacheName, ClusterNode rich) {
            Collection<ClusterNode> cacheNodes = cacheMap.get(cacheName);

            if (cacheNodes == null) {
                cacheNodes = new ArrayList<>(allNodes.size());

                cacheMap.put(cacheName, cacheNodes);
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
         * Gets collection of nodes which have version equal or greater than {@code ver}.
         *
         * @param ver Version to check.
         * @return Collection of nodes with version equal or greater than {@code ver}.
         */
        Collection<ClusterNode> elderNodes(IgniteProductVersion ver) {
            Map.Entry<IgniteProductVersion, Collection<ClusterNode>> entry = nodesByVer.ceilingEntry(ver);

            if (entry == null)
                return Collections.emptyList();

            return entry.getValue();
        }

        /**
         * @return Versions map.
         */
        NavigableMap<IgniteProductVersion, Collection<ClusterNode>> versionsMap() {
            return nodesByVer;
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
            return filter(topVer, allCacheNodes.get(cacheName));
        }

        /**
         * Gets all remote nodes that have cache with given name.
         *
         * @param cacheName Cache name.
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> remoteCacheNodes(@Nullable String cacheName, final long topVer) {
            return filter(topVer, rmtCacheNodes.get(cacheName));
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
         * Gets all nodes that have cache with given name and should participate in affinity calculation. With
         * partitioned cache nodes with near-only cache do not participate in affinity node calculation.
         *
         * @param cacheName Cache name.
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> cacheAffinityNodes(@Nullable String cacheName, final long topVer) {
            return filter(topVer, affCacheNodes.get(cacheName));
        }

        /**
         * Gets all alive nodes that have cache with given name.
         *
         * @param cacheName Cache name.
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> aliveCacheNodes(@Nullable String cacheName, final long topVer) {
            return filter(topVer, aliveCacheNodes.get(maskNull(cacheName)));
        }

        /**
         * Gets all alive remote nodes that have cache with given name.
         *
         * @param cacheName Cache name.
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> aliveRemoteCacheNodes(@Nullable String cacheName, final long topVer) {
            return filter(topVer, aliveRmtCacheNodes.get(maskNull(cacheName)));
        }

        /**
         * Gets all alive remote server nodes with at least one cache configured.
         *
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> aliveRemoteServerNodesWithCaches(final long topVer) {
            return filter(topVer, aliveRmtSrvNodesWithCaches);
        }

        /**
         * Gets all alive server nodes with at least one cache configured.
         *
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> aliveServerNodesWithCaches(final long topVer) {
            return filter(topVer, aliveSrvNodesWithCaches);
        }

        /**
         * Gets all alive remote nodes with at least one cache configured.
         *
         * @param topVer Topology version.
         * @return Collection of nodes.
         */
        Collection<ClusterNode> aliveNodesWithCaches(final long topVer) {
            return filter(topVer, aliveNodesWithCaches);
        }

        /**
         * Checks if cache with given name has at least one node with near cache enabled.
         *
         * @param cacheName Cache name.
         * @return {@code True} if cache with given name has at least one node with near cache enabled.
         */
        boolean hasNearCache(@Nullable String cacheName) {
            return nearEnabledCaches.contains(cacheName);
        }

        /**
         * Removes left node from cached alives lists.
         *
         * @param leftNode Left node.
         */
        void updateAlives(ClusterNode leftNode) {
            if (leftNode.order() > maxOrder)
                return;

            filterNodeMap(aliveCacheNodes, leftNode);

            filterNodeMap(aliveRmtCacheNodes, leftNode);

            aliveNodesWithCaches.remove(leftNode);
            aliveSrvNodesWithCaches.remove(leftNode);
            aliveRmtSrvNodesWithCaches.remove(leftNode);
        }

        /**
         * Creates a copy of nodes map without the given node.
         *
         * @param map Map to copy.
         * @param exclNode Node to exclude.
         */
        private void filterNodeMap(ConcurrentMap<String, Collection<ClusterNode>> map, final ClusterNode exclNode) {
            for (String cacheName : registeredCaches.keySet()) {
                String maskedName = maskNull(cacheName);

                while (true) {
                    Collection<ClusterNode> oldNodes = map.get(maskedName);

                    if (oldNodes == null || oldNodes.isEmpty())
                        break;

                    Collection<ClusterNode> newNodes = new ArrayList<>(oldNodes);

                    if (!newNodes.remove(exclNode))
                        break;

                    if (map.replace(maskedName, oldNodes, newNodes))
                        break;
                }
            }
        }

        /**
         * Replaces {@code null} with {@code NULL_CACHE_NAME}.
         *
         * @param cacheName Cache name.
         * @return Masked name.
         */
        private String maskNull(@Nullable String cacheName) {
            return cacheName == null ? NULL_CACHE_NAME : cacheName;
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

        /** Flag indicating if cache is local. */
        private final boolean loc;

        /** Collection of client near nodes. */
        private final ConcurrentHashMap<UUID, Boolean> clientNodes;

        /**
         * @param cacheFilter Cache filter.
         * @param nearEnabled Near enabled flag.
         * @param loc {@code True} if cache is local.
         */
        private CachePredicate(IgnitePredicate<ClusterNode> cacheFilter, boolean nearEnabled, boolean loc) {
            assert cacheFilter != null;

            this.cacheFilter = cacheFilter;
            this.nearEnabled = nearEnabled;
            this.loc = loc;

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
