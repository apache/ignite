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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CommunicationFailureResolver;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DefaultCommunicationFailureResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.RestartProcessFailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.ClientCacheChangeDummyDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.cluster.IGridClusterStateProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridTuple6;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.JoiningNodeDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiHistorySupport;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.DiscoverySpiMutableCustomMessageSupport;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.DiscoverySpiOrderSupport;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.thread.OomExceptionHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SECURITY_COMPATIBILITY_MODE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DATA_REGIONS_OFFHEAP_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_DEPLOYMENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_LATE_AFFINITY_ASSIGNMENT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_BINARY_STRING_SER_VER_2;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MARSHALLER_USE_DFLT_SUID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MVCC_ENABLED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_OFFHEAP_SIZE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PEER_CLASSLOADING;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PHY_RAM;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_COMPATIBILITY_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_USER_NAME;
import static org.apache.ignite.internal.IgniteVersionUtils.VER;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.processors.security.SecurityUtils.SERVICE_PERMISSIONS_SINCE;
import static org.apache.ignite.internal.processors.security.SecurityUtils.isSecurityCompatibilityMode;
import static org.apache.ignite.plugin.segmentation.SegmentationPolicy.NOOP;

/**
 * Discovery SPI manager.
 */
public class GridDiscoveryManager extends GridManagerAdapter<DiscoverySpi> {
    /** Metrics update frequency. */
    private static final long METRICS_UPDATE_FREQ = 3000;

    /** JVM interface to memory consumption info */
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
    private static final int DISCOVERY_HISTORY_SIZE = getInteger(IGNITE_DISCOVERY_HISTORY_SIZE, 500);

    /** Predicate filtering out daemon nodes. */
    private static final IgnitePredicate<ClusterNode> FILTER_NOT_DAEMON = new P1<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            return !n.isDaemon();
        }
    };

    /** Predicate filtering client nodes. */
    private static final IgnitePredicate<ClusterNode> FILTER_CLI = new P1<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            return n.isClient();
        }
    };

    /** */
    private final Object discoEvtMux = new Object();

    /** Discovery event worker. */
    private final DiscoveryWorker discoWrk = new DiscoveryWorker();

    /** Discovery event notyfier worker. */
    private final DiscoveryMessageNotifyerWorker discoNotifierWrk = new DiscoveryMessageNotifyerWorker();

    /** Network segment check worker. */
    private SegmentCheckWorker segChkWrk;

    /** Network segment check thread. */
    private IgniteThread segChkThread;

    /** Last logged topology. */
    private final GridAtomicLong lastLoggedTop = new GridAtomicLong();

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
    private GridFutureAdapter<DiscoveryLocalJoinData> locJoin = new GridFutureAdapter<>();

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
        new ConcurrentHashMap<>();

    /** Local node initialization event listeners. */
    private final Collection<IgniteInClosure<ClusterNode>> locNodeInitLsnrs = new ArrayList<>();

    /** Map of dynamic cache filters. */
    private ConcurrentMap<String, CachePredicate> registeredCaches = new ConcurrentHashMap<>();

    /** */
    private Map<Integer, CacheGroupAffinity> registeredCacheGrps = new HashMap<>();

    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Received custom messages history. */
    private final ArrayDeque<IgniteUuid> rcvdCustomMsgs = new ArrayDeque<>();

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Discovery spi registered flag. */
    private boolean registeredDiscoSpi;

    /** Local node compatibility consistent ID. */
    private Serializable consistentId;

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

    /**
     * Returns the current memory usage of the heap
     * @return memory usage or fake value with zero in case there was exception during take of metrics
     */
    private MemoryUsage getHeapMemoryUsage() {
        // Catch exception here to allow discovery proceed even if metrics are not available
        // java.lang.IllegalArgumentException: committed = 5274103808 should be < max = 5274095616
        // at java.lang.management.MemoryUsage.<init>(Unknown Source)
        try {
            return mem.getHeapMemoryUsage();
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
     *
     */
    public void cleanCachesAndGroups() {
        registeredCacheGrps.clear();
        registeredCaches.clear();
    }

    /**
     * @param grpDesc Cache group descriptor.
     * @param filter Node filter.
     * @param cacheMode Cache mode.
     */
    public void addCacheGroup(CacheGroupDescriptor grpDesc, IgnitePredicate<ClusterNode> filter, CacheMode cacheMode) {
        CacheGroupAffinity old = registeredCacheGrps.put(grpDesc.groupId(),
            new CacheGroupAffinity(grpDesc.cacheOrGroupName(), filter, cacheMode, grpDesc.persistenceEnabled()));

        assert old == null : old;
    }

    /**
     * @param grpDesc Cache group descriptor.
     */
    public void removeCacheGroup(CacheGroupDescriptor grpDesc) {
        CacheGroupAffinity rmvd = registeredCacheGrps.remove(grpDesc.groupId());

        assert rmvd != null : grpDesc.cacheOrGroupName();
    }

    /**
     * Called from discovery thread. Adds dynamic cache filter.
     *
     * @param cacheId Cache ID.
     * @param grpId Cache group ID.
     * @param cacheName Cache name.
     * @param nearEnabled Near enabled flag.
     */
    public void setCacheFilter(
        int cacheId,
        int grpId,
        String cacheName,
        boolean nearEnabled
    ) {
        if (!registeredCaches.containsKey(cacheName)) {
            CacheGroupAffinity grp = registeredCacheGrps.get(grpId);

            assert grp != null : "Failed to find cache group [grpId=" + grpId + ", cache=" + cacheName + ']';

            if (grp.cacheMode == CacheMode.REPLICATED)
                nearEnabled = false;

            registeredCaches.put(cacheName, new CachePredicate(cacheId, grp, nearEnabled));
        }
    }

    /**
     * Called from discovery thread. Removes dynamic cache filter.
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
     * Called from discovery thread. Removes near node ID from cache filter.
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
     * Called from discovery thread.
     *
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

        return res == null ? Collections.<String, Map<UUID,Boolean>>emptyMap() : res;
    }

    /**
     * Called from discovery thread.
     *
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
        if ((getSpi() instanceof TcpDiscoverySpi) && Boolean.TRUE.equals(ctx.config().isClientMode()) && !getSpi().isClientMode())
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

        ctx.addNodeAttribute(ATTR_PHY_RAM, totSysMemory);
        ctx.addNodeAttribute(ATTR_OFFHEAP_SIZE, requiredOffheap());
        ctx.addNodeAttribute(ATTR_DATA_REGIONS_OFFHEAP_SIZE, configuredOffheap());

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
            if (isSecurityCompatibilityMode())
                ctx.addNodeAttribute(ATTR_SECURITY_COMPATIBILITY_MODE, true);

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

        if (ctx.config().getCommunicationFailureResolver() != null)
            ctx.resource().injectGeneric(ctx.config().getCommunicationFailureResolver());

        spi.setListener(new DiscoverySpiListener() {
            private long gridStartTime;

            /** {@inheritDoc} */
            @Override public void onLocalNodeInitialized(ClusterNode locNode) {
                for (IgniteInClosure<ClusterNode> lsnr : locNodeInitLsnrs)
                    lsnr.apply(locNode);

                if (locNode instanceof IgniteClusterNode) {
                    final IgniteClusterNode node = (IgniteClusterNode)locNode;

                    if (consistentId != null)
                        node.setConsistentId(consistentId);
                }
            }

            @Override public IgniteInternalFuture onDiscovery(
                final int type,
                final long topVer,
                final ClusterNode node,
                final Collection<ClusterNode> topSnapshot,
                final Map<Long, Collection<ClusterNode>> snapshots,
                @Nullable DiscoverySpiCustomMessage spiCustomMsg
            ) {
                GridFutureAdapter notificationFut = new GridFutureAdapter();

                discoNotifierWrk.submit(notificationFut, () -> {
                    synchronized (discoEvtMux) {
                        onDiscovery0(type, topVer, node, topSnapshot, snapshots, spiCustomMsg);
                    }
                });

                return notificationFut;
            }

            /**
             * @param type Event type.
             * @param topVer Event topology version.
             * @param node Event node.
             * @param topSnapshot Topology snapsjot.
             * @param snapshots Topology snapshots history.
             * @param spiCustomMsg Custom event.
             */
            private void onDiscovery0(
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
                        type != EVT_DISCOVERY_CUSTOM_EVT) {
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

                ctx.coordinators().onDiscoveryEvent(type, topSnapshot, topVer);

                boolean locJoinEvt = type == EVT_NODE_JOINED && node.id().equals(locNode.id());

                ChangeGlobalStateFinishMessage stateFinishMsg = null;

                if (type == EVT_NODE_FAILED || type == EVT_NODE_LEFT)
                    stateFinishMsg = ctx.state().onNodeLeft(node);

                final AffinityTopologyVersion nextTopVer;

                if (type == EVT_DISCOVERY_CUSTOM_EVT) {
                    assert customMsg != null;

                    boolean incMinorTopVer;

                    if (customMsg instanceof ChangeGlobalStateMessage) {
                        incMinorTopVer = ctx.state().onStateChangeMessage(
                            new AffinityTopologyVersion(topVer, minorTopVer),
                            (ChangeGlobalStateMessage)customMsg,
                            discoCache());
                    }
                    else if (customMsg instanceof ChangeGlobalStateFinishMessage) {
                        ctx.state().onStateFinishMessage((ChangeGlobalStateFinishMessage)customMsg);

                        Snapshot snapshot = topSnap.get();

                        // Topology version does not change, but need create DiscoCache with new state.
                        DiscoCache discoCache = snapshot.discoCache.copy(snapshot.topVer, ctx.state().clusterState());

                        topSnap.set(new Snapshot(snapshot.topVer, discoCache));

                        incMinorTopVer = false;
                    }
                    else {
                        incMinorTopVer = ctx.cache().onCustomEvent(
                            customMsg,
                            new AffinityTopologyVersion(topVer, minorTopVer),
                            node);
                    }

                    if (incMinorTopVer) {
                        minorTopVer++;

                        verChanged = true;
                    }

                    nextTopVer = new AffinityTopologyVersion(topVer, minorTopVer);

                    if (incMinorTopVer)
                        ctx.cache().onDiscoveryEvent(type, customMsg, node, nextTopVer, ctx.state().clusterState());
                }
                else {
                    nextTopVer = new AffinityTopologyVersion(topVer, minorTopVer);

                    ctx.cache().onDiscoveryEvent(type, customMsg, node, nextTopVer, ctx.state().clusterState());
                }

                if (type == EVT_DISCOVERY_CUSTOM_EVT) {
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

                DiscoCache discoCache;

                // Put topology snapshot into discovery history.
                // There is no race possible between history maintenance and concurrent discovery
                // event notifications, since SPI notifies manager about all events from this listener.
                if (verChanged) {
                    Snapshot snapshot = topSnap.get();

                    if (customMsg == null) {
                        discoCache = createDiscoCache(
                            nextTopVer,
                            ctx.state().clusterState(),
                            locNode,
                            topSnapshot);
                    }
                    else if (customMsg instanceof ChangeGlobalStateMessage) {
                        discoCache = createDiscoCache(
                            nextTopVer,
                            ctx.state().pendingState((ChangeGlobalStateMessage)customMsg),
                            locNode,
                            topSnapshot);
                    }
                    else
                        discoCache = customMsg.createDiscoCache(GridDiscoveryManager.this, nextTopVer, snapshot.discoCache);

                    discoCacheHist.put(nextTopVer, discoCache);

                    assert snapshot.topVer.compareTo(nextTopVer) < 0: "Topology version out of order [this.topVer=" +
                        topSnap + ", topVer=" + topVer + ", node=" + node + ", nextTopVer=" + nextTopVer +
                        ", evt=" + U.gridEventName(type) + ']';

                    topSnap.set(new Snapshot(nextTopVer, discoCache));
                }
                else
                    // Current version.
                    discoCache = discoCache();

                final DiscoCache discoCache0 = discoCache;

                // If this is a local join event, just save it and do not notify listeners.
                if (locJoinEvt) {
                    if (gridStartTime == 0)
                        gridStartTime = getSpi().getGridStartTime();

                    topSnap.set(new Snapshot(nextTopVer, discoCache));

                    startLatch.countDown();

                    DiscoveryEvent discoEvt = new DiscoveryEvent();

                    discoEvt.node(ctx.discovery().localNode());
                    discoEvt.eventNode(node);
                    discoEvt.type(EVT_NODE_JOINED);

                    discoEvt.topologySnapshot(topVer, new ArrayList<>(F.view(topSnapshot, FILTER_NOT_DAEMON)));

                    discoWrk.discoCache = discoCache;

                    if (!isLocDaemon && !ctx.clientDisconnected()) {
                        ctx.cache().context().exchange().onLocalJoin(discoEvt, discoCache);

                        ctx.authentication().onLocalJoin();
                    }

                    IgniteInternalFuture<Boolean> transitionWaitFut = ctx.state().onLocalJoin(discoCache);

                    locJoin.onDone(new DiscoveryLocalJoinData(discoEvt,
                        discoCache,
                        transitionWaitFut,
                        ctx.state().clusterState().active()));

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

                    if (!locJoin.isDone())
                        locJoin.onDone(new IgniteCheckedException("Node disconnected"));

                    locJoin = new GridFutureAdapter<>();

                    registeredCaches.clear();
                    registeredCacheGrps.clear();

                    for (AffinityTopologyVersion histVer : discoCacheHist.keySet()) {
                        Object rmvd = discoCacheHist.remove(histVer);

                        assert rmvd != null : histVer;
                    }

                    topHist.clear();

                    topSnap.set(new Snapshot(AffinityTopologyVersion.ZERO,
                        createDiscoCache(AffinityTopologyVersion.ZERO, ctx.state().clusterState(), locNode,
                            Collections.singleton(locNode))
                    ));
                }
                else if (type == EVT_CLIENT_NODE_RECONNECTED) {
                    assert locNode.isClient() : locNode;
                    assert node.isClient() : node;

                    boolean clusterRestarted = gridStartTime != getSpi().getGridStartTime();

                    gridStartTime = getSpi().getGridStartTime();

                    ((IgniteKernal)ctx.grid()).onReconnected(clusterRestarted);

                    ctx.cache().context().exchange().onLocalJoin(localJoinEvent(), discoCache);

                    ctx.cluster().clientReconnectFuture().listen(new CI1<IgniteFuture<?>>() {
                        @Override public void apply(IgniteFuture<?> fut) {
                            try {
                                fut.get();

                                discoWrk.addEvent(type, nextTopVer, node, discoCache0, topSnapshot, null);
                            }
                            catch (IgniteException ignore) {
                                // No-op.
                            }
                        }
                    });

                    return;
                }

                if (type == EVT_CLIENT_NODE_DISCONNECTED || type == EVT_NODE_SEGMENTED || !ctx.clientDisconnected())
                    discoWrk.addEvent(type, nextTopVer, node, discoCache, topSnapshot, customMsg);

                if (stateFinishMsg != null)
                    discoWrk.addEvent(EVT_DISCOVERY_CUSTOM_EVT, nextTopVer, node, discoCache, topSnapshot, stateFinishMsg);
            }
        });

        spi.setDataExchange(new DiscoverySpiDataExchange() {
            @Override public DiscoveryDataBag collect(DiscoveryDataBag dataBag) {
                assert dataBag != null;
                assert dataBag.joiningNodeId() != null;

                if (ctx.localNodeId().equals(dataBag.joiningNodeId())) {
                    for (GridComponent c : ctx.components())
                        c.collectJoiningNodeData(dataBag);
                }
                else {
                    for (GridComponent c : ctx.components())
                        c.collectGridNodeData(dataBag);
                }

                return dataBag;
            }

            @Override public void onExchange(DiscoveryDataBag dataBag) {
                assert dataBag != null;
                assert dataBag.joiningNodeId() != null;

                if (ctx.localNodeId().equals(dataBag.joiningNodeId())) {
                    // NodeAdded msg reached joining node after round-trip over the ring.
                    IGridClusterStateProcessor stateProc = ctx.state();

                    stateProc.onGridDataReceived(dataBag.gridDiscoveryData(
                        stateProc.discoveryDataType().ordinal()));

                    for (GridComponent c : ctx.components()) {
                        if (c.discoveryDataType() != null && c != stateProc)
                            c.onGridDataReceived(dataBag.gridDiscoveryData(c.discoveryDataType().ordinal()));
                    }
                }
                else {
                    // Discovery data from newly joined node has to be applied to the current old node.
                    IGridClusterStateProcessor stateProc = ctx.state();

                    JoiningNodeDiscoveryData data0 = dataBag.newJoinerDiscoveryData(
                        stateProc.discoveryDataType().ordinal());

                    assert data0 != null;

                    stateProc.onJoiningNodeDataReceived(data0);

                    for (GridComponent c : ctx.components()) {
                        if (c.discoveryDataType() != null && c != stateProc) {
                            JoiningNodeDiscoveryData data = dataBag.newJoinerDiscoveryData(
                                c.discoveryDataType().ordinal());

                            if (data != null)
                                c.onJoiningNodeDataReceived(data);
                        }
                    }
                }
            }
        });

        new IgniteThread(discoNotifierWrk).start();

        startSpi();

        registeredDiscoSpi = true;

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

            segChkThread.setUncaughtExceptionHandler(new OomExceptionHandler(ctx));

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
        if (type == EVT_DISCOVERY_CUSTOM_EVT) {
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
     * Adds a listener for local node initialized event.
     *
     * @param lsnr Listener to add.
     */
    public void addLocalNodeInitializedEventListener(IgniteInClosure<ClusterNode> lsnr) {
        locNodeInitLsnrs.add(lsnr);
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
                return getHeapMemoryUsage().getInit();
            }

            @Override public long getHeapMemoryUsed() {
                return getHeapMemoryUsage().getUsed();
            }

            @Override public long getHeapMemoryCommitted() {
                return getHeapMemoryUsage().getCommitted();
            }

            @Override public long getHeapMemoryMaximum() {
                return getHeapMemoryUsage().getMax();
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
    public DiscoveryMetricsProvider createMetricsProvider() {
        return new DiscoveryMetricsProvider() {
            /** */
            private final long startTime = U.currentTimeMillis();

            /** {@inheritDoc} */
            @Override public ClusterMetrics metrics() {
                return new ClusterMetricsImpl(ctx, metrics, startTime);
            }

            /** {@inheritDoc} */
            @Override public Map<Integer, CacheMetrics> cacheMetrics() {
                try {
                    /** Caches should not be accessed while state transition is in progress. */
                    if (ctx.state().clusterState().transition())
                        return Collections.emptyMap();

                    Collection<GridCacheAdapter<?, ?>> caches = ctx.cache().internalCaches();

                    if (!F.isEmpty(caches)) {
                        Map<Integer, CacheMetrics> metrics = U.newHashMap(caches.size());

                        for (GridCacheAdapter<?, ?> cache : caches) {
                            if (cache.context().statisticsEnabled() &&
                                cache.context().started() &&
                                cache.context().affinity().affinityTopologyVersion().topologyVersion() > 0)
                                metrics.put(cache.context().cacheId(), cache.localMetrics());
                        }

                        return metrics;
                    }
                } catch (Exception e) {
                    U.warn(log, "Failed to compute cache metrics", e);
                }

                return Collections.emptyMap();
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
        Boolean locSecurityCompatibilityEnabled = locNode.attribute(ATTR_SECURITY_COMPATIBILITY_MODE);

        Boolean locMvccEnabled = locNode.attribute(ATTR_MVCC_ENABLED);

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
                        ", rmtAddrs=" + U.addressesAsString(n) + ", rmtNode=" + U.toShortString(n) + "]");

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
                        ", rmtAddrs=" + U.addressesAsString(n) + ", rmtNode=" + U.toShortString(n) + "]");

                boolean rmtP2pEnabled = n.attribute(ATTR_PEER_CLASSLOADING);

                if (locP2pEnabled != rmtP2pEnabled)
                    throw new IgniteCheckedException("Remote node has peer class loading enabled flag different from" +
                        " local [locId8=" + U.id8(locNode.id()) + ", locPeerClassLoading=" + locP2pEnabled +
                        ", rmtId8=" + U.id8(n.id()) + ", rmtPeerClassLoading=" + rmtP2pEnabled +
                        ", rmtAddrs=" + U.addressesAsString(n) + ", rmtNode=" + U.toShortString(n) + "]");
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
                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + n.id() + ", rmtNode=" + U.toShortString(n) + "]");
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
                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + n.id() + ", rmtNode=" + U.toShortString(n) + "]");
            }

            boolean rmtLateAssign = n.attribute(ATTR_LATE_AFFINITY_ASSIGNMENT);

            if (locDelayAssign != rmtLateAssign) {
                throw new IgniteCheckedException("Remote node has cache affinity assignment mode different from local " +
                    "[locId8=" +  U.id8(locNode.id()) +
                    ", locDelayAssign=" + locDelayAssign +
                    ", rmtId8=" + U.id8(n.id()) +
                    ", rmtLateAssign=" + rmtLateAssign +
                    ", rmtAddrs=" + U.addressesAsString(n) + ", rmtNode=" + U.toShortString(n) + "]");
            }

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
                    ", locNodeId=" + locNode.id() + ", rmtNode=" + U.toShortString(n) + "]");
            }

            Boolean rmtMvccEnabled = n.attribute(ATTR_MVCC_ENABLED);

            if (!F.eq(locMvccEnabled, rmtMvccEnabled)) {
                throw new IgniteCheckedException("Remote node has MVCC mode different from local " +
                    "[locId8=" +  U.id8(locNode.id()) +
                    ", locMvccMode=" + (Boolean.TRUE.equals(locMvccEnabled) ? "ENABLED" : "DISABLED") +
                    ", rmtId8=" + U.id8(n.id()) +
                    ", rmtMvccMode=" + (Boolean.TRUE.equals(rmtMvccEnabled) ? "ENABLED" : "DISABLED") +
                    ", rmtAddrs=" + U.addressesAsString(n) + ", rmtNode=" + U.toShortString(n) + "]");
            }

            if (n.version().compareToIgnoreTimestamp(SERVICE_PERMISSIONS_SINCE) >= 0
                && ctx.security().enabled() // Matters only if security enabled.
               ) {
                Boolean rmtSecurityCompatibilityEnabled = n.attribute(ATTR_SECURITY_COMPATIBILITY_MODE);

                if (!F.eq(locSecurityCompatibilityEnabled, rmtSecurityCompatibilityEnabled)) {
                    throw new IgniteCheckedException("Local node's " + IGNITE_SECURITY_COMPATIBILITY_MODE +
                        " property value differs from remote node's value " +
                        "(to make sure all nodes in topology have identical Ignite security compatibility mode enabled, " +
                        "configure system property explicitly) " +
                        "[locSecurityCompatibilityEnabled=" + locSecurityCompatibilityEnabled +
                        ", rmtSecurityCompatibilityEnabled=" + rmtSecurityCompatibilityEnabled +
                        ", locNodeAddrs=" + U.addressesAsString(locNode) +
                        ", rmtNodeAddrs=" + U.addressesAsString(n) +
                        ", locNodeId=" + locNode.id() + ", rmtNode=" + U.toShortString(n) + "]");
                }
            }

            if (n.version().compareToIgnoreTimestamp(SERVICE_PERMISSIONS_SINCE) < 0
                && ctx.security().enabled() // Matters only if security enabled.
                && (locSecurityCompatibilityEnabled == null || !locSecurityCompatibilityEnabled)) {
                throw new IgniteCheckedException("Remote node does not support service security permissions. " +
                    "To be able to join to it, local node must be started with " + IGNITE_SECURITY_COMPATIBILITY_MODE +
                    " system property set to \"true\". " +
                    "[locSecurityCompatibilityEnabled=" + locSecurityCompatibilityEnabled +
                    ", locNodeAddrs=" + U.addressesAsString(locNode) +
                    ", rmtNodeAddrs=" + U.addressesAsString(n) +
                    ", locNodeId=" + locNode.id() + ", rmtNodeId=" + n.id() + ", " +
                    ", rmtNodeVer" + n.version() + ", rmtNode=" + U.toShortString(n) + "]");
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
        String verStr = node.<String>attribute("java.version");

        int res = U.majorJavaVersion(verStr);

        if (res == 0) {
            U.error(log, "Failed to get java major version (unknown 'java.version' format) [ver=" +
                node.<String>attribute("java.version") + "]");
        }

        return res;
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
     * @param evtType Event type.
     * @param evtNode Event node.
     */
    public void ackTopology(long topVer, int evtType, ClusterNode evtNode) {
        ackTopology(topVer, evtType, evtNode, false);
    }

    /**
     * Logs grid size for license compliance.
     *
     * @param topVer Topology version.
     * @param evtType Event type.
     * @param evtNode Event node.
     * @param throttle Suppress printing if this topology was already printed.
     */
    private void ackTopology(long topVer, int evtType, ClusterNode evtNode, boolean throttle) {
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

        // Prevent ack-ing topology for the same topology.
        // Can happen only during node startup.
        if (throttle && !lastLoggedTop.setIfGreater(topVer))
            return;

        int totalCpus = cpus(allNodes);

        double heap = U.heapSize(allNodes, 2);
        double offheap = U.offheapSize(allNodes, 2);

        if (log.isQuiet())
            topologySnapshotMessage(new IgniteClosure<String, Void>() {
                @Override public Void apply(String msg) {
                    U.quiet(false, msg);

                    return null;
                }
            }, topVer, discoCache, evtType, evtNode, srvNodes.size(), clientNodes.size(), totalCpus, heap, offheap);

        if (log.isDebugEnabled()) {
            String dbg = "";

            dbg += U.nl() + U.nl() +
                ">>> +----------------+" + U.nl() +
                ">>> " + PREFIX + "." + U.nl() +
                ">>> +----------------+" + U.nl() +
                ">>> Ignite instance name: " +
                (ctx.igniteInstanceName() == null ? "default" : ctx.igniteInstanceName()) + U.nl() +
                ">>> Number of server nodes: " + srvNodes.size() + U.nl() +
                ">>> Number of client nodes: " + clientNodes.size() + U.nl() +
                (discoOrdered ? ">>> Topology version: " + topVer + U.nl() : "");

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
            dbg += ">>> Total offheap size: " + offheap + "GB" + U.nl();

            log.debug(dbg);
        }
        else if (log.isInfoEnabled())
            topologySnapshotMessage(new IgniteClosure<String, Void>() {
                @Override public Void apply(String msg) {
                    log.info(msg);

                    return null;
                }
            }, topVer, discoCache, evtType, evtNode, srvNodes.size(), clientNodes.size(), totalCpus, heap, offheap);
    }

    /**
     * @return Required offheap memory in bytes.
     */
    private long requiredOffheap() {
        if(ctx.config().isClientMode())
            return 0;

        DataStorageConfiguration memCfg = ctx.config().getDataStorageConfiguration();

        assert memCfg != null;

        long res = memCfg.getSystemRegionMaxSize();

        // Add memory policies.
        DataRegionConfiguration[] dataRegions = memCfg.getDataRegionConfigurations();

        if (dataRegions != null) {
            for (DataRegionConfiguration dataReg : dataRegions) {
                res += dataReg.getMaxSize();

                res += GridCacheDatabaseSharedManager.checkpointBufferSize(dataReg);
            }
        }

        res += memCfg.getDefaultDataRegionConfiguration().getMaxSize();

        res += GridCacheDatabaseSharedManager.checkpointBufferSize(memCfg.getDefaultDataRegionConfiguration());

        return res;
    }

    /**
     * @return Configured data regions offheap memory in bytes.
     */
    private long configuredOffheap() {
        DataStorageConfiguration memCfg = ctx.config().getDataStorageConfiguration();

        if (memCfg == null)
            return 0;

        long res = memCfg.getDefaultDataRegionConfiguration().getMaxSize();

        DataRegionConfiguration[] dataRegions = memCfg.getDataRegionConfigurations();

        if (dataRegions != null) {
            for (DataRegionConfiguration dataReg : dataRegions)
                res += dataReg.getMaxSize();
        }

        return res;
    }

    /**
     * @param regCfg Data region configuration.
     * @return Data region message.
     */
    private String dataRegionConfigurationMessage(DataRegionConfiguration regCfg) {
        if (regCfg == null)
            return null;

        SB m = new SB();

        m.a("  ^-- ").a(regCfg.getName()).a(" [");
        m.a("initSize=").a(U.readableSize(regCfg.getInitialSize(), false));
        m.a(", maxSize=").a(U.readableSize(regCfg.getMaxSize(), false));
        m.a(", persistenceEnabled=" + regCfg.isPersistenceEnabled()).a(']');

        return m.toString();
    }

    /**
     * @param clo Wrapper of logger.
     * @param topVer Topology version.
     * @param discoCache Discovery cache.
     * @param evtType Event type.
     * @param evtNode Event node.
     * @param srvNodesNum Server nodes number.
     * @param clientNodesNum Client nodes number.
     * @param totalCpus Total cpu number.
     * @param heap Heap size.
     * @param offheap Offheap size.
     */
    private void topologySnapshotMessage(IgniteClosure<String, Void> clo, long topVer, DiscoCache discoCache,
        int evtType, ClusterNode evtNode, int srvNodesNum, int clientNodesNum, int totalCpus, double heap,
        double offheap) {
        String summary = PREFIX + " [" +
            (discoOrdered ? "ver=" + topVer + ", " : "") +
            "servers=" + srvNodesNum +
            ", clients=" + clientNodesNum +
            ", CPUs=" + totalCpus +
            ", offheap=" + offheap + "GB" +
            ", heap=" + heap + "GB]";

        clo.apply(summary);

        ClusterNode currCrd = discoCache.oldestServerNode();

        if ((evtType == EventType.EVT_NODE_FAILED || evtType == EventType.EVT_NODE_LEFT) &&
                currCrd != null && currCrd.order() > evtNode.order())
            clo.apply("Coordinator changed [prev=" + evtNode + ", cur=" + currCrd + "]");

        DiscoveryDataClusterState state = discoCache.state();

        clo.apply("  ^-- Node [id=" + discoCache.localNode().id().toString().toUpperCase() + ", clusterState="
            + (state.active() ? "ACTIVE" : "INACTIVE") + ']');

        BaselineTopology blt = state.baselineTopology();

        if (blt != null && discoCache.baselineNodes() != null) {
            int bltSize = discoCache.baselineNodes().size();
            int bltOnline = discoCache.aliveBaselineNodes().size();
            int bltOffline = bltSize - bltOnline;

            clo.apply("  ^-- Baseline [id=" + blt.id() + ", size=" + bltSize + ", online=" + bltOnline
                + ", offline=" + bltOffline + ']');

            if (!state.active() && ctx.config().isAutoActivationEnabled()) {
                String offlineConsistentIds = "";

                if (bltOffline > 0 && bltOffline <= 5) {
                    Collection<BaselineNode> offlineNodes = new HashSet<>(discoCache.baselineNodes());

                    offlineNodes.removeAll(discoCache.aliveBaselineNodes());

                    offlineConsistentIds = ' ' + F.nodeConsistentIds(offlineNodes).toString();
                }

                if (bltOffline == 0) {
                    if (evtType == EVT_NODE_JOINED && discoCache.baselineNode(evtNode))
                        clo.apply("  ^-- All baseline nodes are online, will start auto-activation");
                }
                else
                    clo.apply("  ^-- " + bltOffline + " nodes left for auto-activation" + offlineConsistentIds);
            }
        }

        DataStorageConfiguration memCfg = ctx.config().getDataStorageConfiguration();

        if (memCfg == null)
            return;

        clo.apply("Data Regions Configured:");
        clo.apply(dataRegionConfigurationMessage(memCfg.getDefaultDataRegionConfiguration()));

        DataRegionConfiguration[] dataRegions = memCfg.getDataRegionConfigurations();

        if (dataRegions != null) {
            for (int i = 0; i < dataRegions.length; ++i) {
                String msg = dataRegionConfigurationMessage(dataRegions[i]);

                if (msg != null)
                    clo.apply(msg);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop0(boolean cancel) {
        startLatch.countDown();

        // Stop segment check worker.
        if (segChkWrk != null) {
            segChkWrk.cancel();

            U.join(segChkThread, log);
        }

        if (!locJoin.isDone())
            locJoin.onDone(new NodeStoppingException("Failed to wait for local node joined event (grid is stopping)."));
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

        U.cancel(discoNotifierWrk);

        U.join(discoNotifierWrk, log);

        // Stop SPI itself.
        stopSpi();

        // Stop spi if was not add in spi map but port was open.
        if (!registeredDiscoSpi)
            getSpi().spiStop();

        registeredDiscoSpi = false;

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
            if (e.hasCause(IgniteClientDisconnectedCheckedException.class, IgniteClientDisconnectedException.class)) {
                IgniteFuture<?> reconnectFut = ctx.cluster().clientReconnectFuture();

                throw new IgniteClientDisconnectedCheckedException(reconnectFut, e.getMessage());
            }

            LT.warn(log, "Ping failed with error [node=" + nodeId + ", err=" + e + ']');

            return true;
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

    /** @return all alive server nodes is topology */
    public Collection<ClusterNode> aliveServerNodes() {
        return discoCache().aliveServerNodes();
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
        return resolveDiscoCache(CU.cacheId(null), topVer).serverNodes();
    }

    /**
     * @param topVer Topology version.
     * @return All baseline nodes for given topology version or {@code null} if baseline was not set for the
     *      given topology version.
     */
    @Nullable public List<? extends BaselineNode> baselineNodes(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(null), topVer).baselineNodes();
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
     * Gets consistentId from history for given topology version.
     *
     * @param topVer Topology version.
     * @return Compacted consistent id.
     */
    public Map<UUID, Short> consistentId(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(null), topVer).consistentIdMap();
    }

    /**
     * Gets consistentId from history for given topology version.
     *
     * @param topVer Topology version.
     * @return Compacted consistent id map.
     */
    public  Map<Short, UUID> nodeIdMap(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(null), topVer).nodeIdMap();
    }

    /**
     * Gets cache nodes for cache with given name.
     *
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> cacheNodes(@Nullable String cacheName, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(cacheName), topVer).cacheNodes(cacheName);
    }

    /**
     * Gets cache remote nodes for cache with given name.
     *
     * @param topVer Topology version.
     * @return Collection of cache nodes.
     */
    public Collection<ClusterNode> remoteAliveNodesWithCaches(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(null), topVer).remoteAliveNodesWithCaches();
    }

    /**
     * @param topVer Topology version (maximum allowed node order).
     * @return Oldest alive server nodes with at least one cache configured.
     */
    @Nullable public ClusterNode oldestAliveServerNode(AffinityTopologyVersion topVer) {
        return resolveDiscoCache(CU.cacheId(null), topVer).oldestAliveServerNode();
    }

    /**
     * Gets cache nodes for cache with given ID that participate in affinity calculation.
     *
     * @param grpId Cache group ID.
     * @param topVer Topology version.
     * @return Collection of cache affinity nodes.
     */
    public Collection<ClusterNode> cacheGroupAffinityNodes(int grpId, AffinityTopologyVersion topVer) {
        return resolveDiscoCache(grpId, topVer).cacheGroupAffinityNodes(grpId);
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
     * Checks if node is a data node for the given cache group.
     *
     * @param node Node to check.
     * @param grpId Cache group ID.
     * @return {@code True} if node is a cache data node.
     */
    public boolean cacheGroupAffinityNode(ClusterNode node, int grpId) {
        CacheGroupAffinity aff = registeredCacheGrps.get(grpId);

        return CU.affinityNode(node, aff.cacheFilter);
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
    public Map<String, CacheConfiguration> nodePublicCaches(ClusterNode node) {
        Map<String, CacheConfiguration> caches = U.newHashMap(registeredCaches.size());

        for (DynamicCacheDescriptor cacheDesc : ctx.cache().cacheDescriptors().values()) {
            if (!cacheDesc.cacheType().userCache())
                continue;

            CachePredicate p = registeredCaches.get(cacheDesc.cacheName());

            if (p != null && p.cacheNode(node))
                caches.put(cacheDesc.cacheName(), cacheDesc.cacheConfiguration());
        }

        return caches;
    }

    /**
     * Gets discovery cache for given topology version.
     *
     * @param grpId Cache group ID (participates in exception message).
     * @param topVer Topology version.
     * @return Discovery cache.
     */
    private DiscoCache resolveDiscoCache(int grpId, AffinityTopologyVersion topVer) {
        Snapshot snap = topSnap.get();

        DiscoCache cache = AffinityTopologyVersion.NONE.equals(topVer) || topVer.equals(snap.topVer) ?
            snap.discoCache : discoCacheHist.get(topVer);

        if (cache == null) {
            CacheGroupDescriptor desc = ctx.cache().cacheGroupDescriptors().get(grpId);

            throw new IgniteException("Failed to resolve nodes topology [" +
                "cacheGrp=" + (desc != null ? desc.cacheOrGroupName() : "N/A") +
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

        Collection<ClusterNode> nodes = snapshots.get(topVer);

        if (nodes == null) {
            DiscoCache cache = discoCacheHist.get(new AffinityTopologyVersion(topVer, 0));

            if (cache != null)
                nodes = cache.allNodes();
        }

        return nodes;
    }

    /**
     * Gets server nodes topology by specified version from snapshots history storage.
     *
     * @param topVer Topology version.
     * @return Server topology nodes.
     */
    public Collection<ClusterNode> serverTopologyNodes(long topVer) {
        return F.view(topology(topVer), F.not(FILTER_CLI), FILTER_NOT_DAEMON);
    }

    /** @return All daemon nodes in topology. */
    public Collection<ClusterNode> daemonNodes() {
        return discoCache().daemonNodes();
    }

    /** @return Local node. */
    public ClusterNode localNode() {
        return locNode == null ? getSpi().getLocalNode() : locNode;
    }

    /**
     * @return Consistent ID.
     * @deprecated Use {@link ClusterNode#consistentId()} of local node to get actual consistent ID.
     */
    @Deprecated
    public Serializable consistentId() {
        if (consistentId == null)
            consistentId = getInjectedDiscoverySpi().consistentId();

        return consistentId;
    }

    /**
     * Performs injection of discovery SPI if needed, then provides DiscoverySpi SPI.
     * Manual injection is required because normal startup of SPI is done after processor started.
     *
     * @return Wrapped DiscoverySpi SPI.
     */
    public DiscoverySpi getInjectedDiscoverySpi() {
        try {
            inject();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to init consistent ID.", e);
        }
        return getSpi();
    }

    /**
     * Sets TCP local node consistent ID. This setter is to be called before node init in SPI
     *
     * @param consistentId New value of consistent ID to be used in local node initialization
     */
    public void consistentId(final Serializable consistentId) {
        this.consistentId = consistentId;
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
            return locJoin.get().event();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @return Tuple that consists of a local join event and discovery cache at the join time.
     */
    public DiscoveryLocalJoinData localJoin() {
        try {
            return locJoin.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @return Local join future.
     */
    public GridFutureAdapter<DiscoveryLocalJoinData> localJoinFuture() {
        return locJoin;
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
     * @param reqId Start request ID.
     * @param startReqs Cache start requests.
     * @param cachesToClose Cache to close.
     */
    public void clientCacheStartEvent(UUID reqId,
        @Nullable Map<String, DynamicCacheChangeRequest> startReqs,
        @Nullable Set<String> cachesToClose) {
        // Prevent race when discovery message was processed, but was passed to discoWrk.
        synchronized (discoEvtMux) {
            discoWrk.addEvent(EVT_DISCOVERY_CUSTOM_EVT,
                AffinityTopologyVersion.NONE,
                localNode(),
                null,
                Collections.<ClusterNode>emptyList(),
                new ClientCacheChangeDummyDiscoveryMessage(reqId, startReqs, cachesToClose));
        }
    }

    /**
     * @param discoCache
     * @param node
     */
    public void metricsUpdateEvent(DiscoCache discoCache, ClusterNode node) {
        discoWrk.addEvent(EVT_NODE_METRICS_UPDATED,
            discoCache.version(),
            node,
            discoCache,
            discoCache.nodeMap.values(),
            null);
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
     * @return {@code True} if local node client and discovery SPI supports reconnect.
     */
    public boolean reconnectSupported() {
        DiscoverySpi spi = getSpi();

        ClusterNode clusterNode = ctx.discovery().localNode();

        boolean client = (clusterNode instanceof TcpDiscoveryNode) ?
                (((TcpDiscoveryNode) clusterNode).clientRouterNodeId() != null) : clusterNode.isClient();

        return client && (spi instanceof IgniteDiscoverySpi) &&
            ((IgniteDiscoverySpi)spi).clientReconnectSupported();
    }

    /**
     * Leave cluster and try to join again.
     *
     * @throws IgniteSpiException If failed.
     */
    public void reconnect() {
        assert reconnectSupported();

        DiscoverySpi discoverySpi = getSpi();

        ((IgniteDiscoverySpi)discoverySpi).clientReconnect();
    }

    /**
     * Called from discovery thread.
     *
     * @param topVer Topology version.
     * @param state Current state.
     * @param loc Local node.
     * @param topSnapshot Topology snapshot.
     * @return Newly created discovery cache.
     */
    @NotNull private DiscoCache createDiscoCache(
        AffinityTopologyVersion topVer,
        DiscoveryDataClusterState state,
        ClusterNode loc,
        Collection<ClusterNode> topSnapshot) {
        assert topSnapshot.contains(loc);

        MvccCoordinator mvccCrd = ctx.coordinators().coordinatorFromDiscoveryEvent();

        HashSet<UUID> alives = U.newHashSet(topSnapshot.size());
        HashMap<UUID, ClusterNode> nodeMap = U.newHashMap(topSnapshot.size());

        ArrayList<ClusterNode> daemonNodes = new ArrayList<>(topSnapshot.size());
        ArrayList<ClusterNode> srvNodes = new ArrayList<>(topSnapshot.size());
        ArrayList<ClusterNode> rmtNodes = new ArrayList<>(topSnapshot.size());
        ArrayList<ClusterNode> allNodes = new ArrayList<>(topSnapshot.size());

        Map<UUID, Short> nodeIdToConsIdx;
        Map<Short, UUID> consIdxToNodeId;
        List<? extends BaselineNode> baselineNodes;

        IgniteProductVersion minVer = null;
        IgniteProductVersion minSrvVer = null;

        for (ClusterNode node : topSnapshot) {
            if (alive(node))
                alives.add(node.id());

            if (node.isDaemon())
                daemonNodes.add(node);
            else {
                allNodes.add(node);

                if (!node.isLocal())
                    rmtNodes.add(node);

                if (!node.isClient()) {
                    srvNodes.add(node);

                    if (minSrvVer == null)
                        minSrvVer = node.version();
                    else if (node.version().compareTo(minSrvVer) < 0)
                        minSrvVer = node.version();
                }
            }

            nodeMap.put(node.id(), node);

            if (minVer == null)
                minVer = node.version();
            else if (node.version().compareTo(minVer) < 0)
                minVer = node.version();
        }

        assert !rmtNodes.contains(loc) : "Remote nodes collection shouldn't contain local node" +
            " [rmtNodes=" + rmtNodes + ", loc=" + loc + ']';

        BaselineTopology blt = state.baselineTopology();

        if (blt != null) {
            nodeIdToConsIdx = U.newHashMap(srvNodes.size());
            consIdxToNodeId = U.newHashMap(srvNodes.size());

            Map<Object, Short> m = blt.consistentIdMapping();

            Map<Object, ClusterNode> aliveNodesByConsId = U.newHashMap(srvNodes.size());

            for (ClusterNode node : srvNodes) {
                Short compactedId = m.get(node.consistentId());

                if (compactedId != null) {
                    nodeIdToConsIdx.put(node.id(), compactedId);

                    consIdxToNodeId.put(compactedId, node.id());
                }

                aliveNodesByConsId.put(node.consistentId(), node);
            }

            List<BaselineNode >baselineNodes0 = new ArrayList<>(blt.size());

            for (Object consId : blt.consistentIds()) {
                ClusterNode srvNode = aliveNodesByConsId.get(consId);

                if (srvNode != null)
                    baselineNodes0.add(srvNode);
                else
                    baselineNodes0.add(blt.baselineNode(consId));
            }

            baselineNodes = baselineNodes0;
        }
        else {
            nodeIdToConsIdx = null;
            consIdxToNodeId = null;

            baselineNodes = null;
        }

        Map<Integer, List<ClusterNode>> allCacheNodes = U.newHashMap(allNodes.size());
        Map<Integer, List<ClusterNode>> cacheGrpAffNodes = U.newHashMap(allNodes.size());
        Set<ClusterNode> rmtNodesWithCaches = new TreeSet<>(NodeOrderComparator.getInstance());

        fillAffinityNodeCaches(allNodes, allCacheNodes, cacheGrpAffNodes, rmtNodesWithCaches,
                nodeIdToConsIdx == null ? null : nodeIdToConsIdx.keySet());

        return new DiscoCache(
            topVer,
            state,
            loc,
            mvccCrd,
            Collections.unmodifiableList(rmtNodes),
            Collections.unmodifiableList(allNodes),
            Collections.unmodifiableList(srvNodes),
            Collections.unmodifiableList(daemonNodes),
            U.sealList(rmtNodesWithCaches),
            baselineNodes == null ? null : Collections.unmodifiableList(baselineNodes),
            Collections.unmodifiableMap(allCacheNodes),
            Collections.unmodifiableMap(cacheGrpAffNodes),
            Collections.unmodifiableMap(nodeMap),
            alives,
            nodeIdToConsIdx == null ? null : Collections.unmodifiableMap(nodeIdToConsIdx),
            consIdxToNodeId == null ? null : Collections.unmodifiableMap(consIdxToNodeId),
            minVer,
            minSrvVer);
    }

    /**
     * Adds node to map.
     *
     * @param cacheMap Map to add to.
     * @param cacheName Cache name.
     * @param rich Node to add
     */
    private void addToMap(Map<Integer, List<ClusterNode>> cacheMap, String cacheName, ClusterNode rich) {
        List<ClusterNode> cacheNodes = cacheMap.get(CU.cacheId(cacheName));

        if (cacheNodes == null) {
            cacheNodes = new ArrayList<>();

            cacheMap.put(CU.cacheId(cacheName), cacheNodes);
        }

        cacheNodes.add(rich);
    }

    /**
     * @param cfg Configuration.
     * @throws IgniteCheckedException If configuration is not valid.
     */
    public static void initCommunicationErrorResolveConfiguration(IgniteConfiguration cfg) throws IgniteCheckedException {
        CommunicationFailureResolver rslvr = cfg.getCommunicationFailureResolver();
        CommunicationSpi commSpi = cfg.getCommunicationSpi();
        DiscoverySpi discoverySpi = cfg.getDiscoverySpi();

        if (rslvr != null) {
            if (!supportsCommunicationErrorResolve(commSpi))
                throw new IgniteCheckedException("CommunicationFailureResolver is configured, but CommunicationSpi does not support communication" +
                    "problem resolve: " + commSpi.getClass().getName());

            if (!supportsCommunicationErrorResolve(discoverySpi))
                throw new IgniteCheckedException("CommunicationFailureResolver is configured, but DiscoverySpi does not support communication" +
                    "problem resolve: " + discoverySpi.getClass().getName());
        }
        else {
            if (supportsCommunicationErrorResolve(commSpi) && supportsCommunicationErrorResolve(discoverySpi))
                cfg.setCommunicationFailureResolver(new DefaultCommunicationFailureResolver());
        }
    }

    /**
     * @param spi Discovery SPI.
     * @return {@code True} if SPI supports communication error resolve.
     */
    private static boolean supportsCommunicationErrorResolve(DiscoverySpi spi) {
        return spi instanceof IgniteDiscoverySpi && ((IgniteDiscoverySpi)spi).supportsCommunicationFailureResolve();
    }

    /**
     * @param spi Discovery SPI.
     * @return {@code True} if SPI supports communication error resolve.
     */
    private static boolean supportsCommunicationErrorResolve(CommunicationSpi spi) {
        return spi instanceof TcpCommunicationSpi;
    }

    /**
     * @return {@code True} if communication error resolve is supported.
     */
    public boolean communicationErrorResolveSupported() {
        return ctx.config().getCommunicationFailureResolver() != null;
    }

    /**
     * @return {@code True} if configured {@link DiscoverySpi} supports mutable custom messages.
     */
    public boolean mutableCustomMessages() {
        DiscoverySpiMutableCustomMessageSupport ann = U.getAnnotation(ctx.config().getDiscoverySpi().getClass(),
            DiscoverySpiMutableCustomMessageSupport.class);

        return ann != null && ann.value();
    }

    /**
     * @param node Problem node.
     * @param err Error.
     */
    public void resolveCommunicationError(ClusterNode node, Exception err) {
        DiscoverySpi spi = getSpi();

        if (!supportsCommunicationErrorResolve(spi) || !supportsCommunicationErrorResolve(ctx.config().getCommunicationSpi()))
            throw new UnsupportedOperationException();

        ((IgniteDiscoverySpi)spi).resolveCommunicationFailure(node, err);
    }

    /** Worker for network segment checks. */
    private class SegmentCheckWorker extends GridWorker {
        /** */
        private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

        /**
         *
         */
        private SegmentCheckWorker() {
            super(ctx.igniteInstanceName(), "disco-net-seg-chk-worker", GridDiscoveryManager.this.log);

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
                        ClusterNode node = getSpi().getLocalNode();

                        Collection<ClusterNode> locNodeOnlyTop = Collections.singleton(node);

                        discoWrk.addEvent(EVT_NODE_SEGMENTED,
                            AffinityTopologyVersion.NONE,
                            node,
                            createDiscoCache(
                                AffinityTopologyVersion.NONE,
                                ctx.state().clusterState(),
                                node,
                                locNodeOnlyTop
                            ), locNodeOnlyTop,
                            null);

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

    /**
     *
     */
    private class DiscoveryMessageNotifyerWorker extends GridWorker {
        /** Queue. */
        private final BlockingQueue<T2<GridFutureAdapter, Runnable>> queue = new LinkedBlockingQueue<>();

        /**
         * Default constructor.
         */
        protected DiscoveryMessageNotifyerWorker() {
            super(ctx.igniteInstanceName(), "disco-notyfier-worker", GridDiscoveryManager.this.log, ctx.workersRegistry());
        }

        /**
         *
         */
        private void body0() throws InterruptedException {
            T2<GridFutureAdapter, Runnable> notification = queue.take();

            try {
                notification.get2().run();
            }
            finally {
                notification.get1().onDone();
            }
        }

        /**
         * @param cmd Command.
         */
        public synchronized void submit(GridFutureAdapter notificationFut, Runnable cmd) {
            if (isCancelled()) {
                notificationFut.onDone();

                return;
            }

            queue.add(new T2<>(notificationFut, cmd));
        }

        /**
         * Cancel thread execution and completes all notification futures.
         */
        @Override public synchronized void cancel() {
            super.cancel();

            while (!queue.isEmpty()) {
                T2<GridFutureAdapter, Runnable> notification = queue.poll();

                if (notification != null)
                    notification.get1().onDone();
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                try {
                    body0();
                }
                catch (InterruptedException e) {
                    if (!isCancelled)
                        ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, e));

                    throw e;
                }
                catch (Throwable t) {
                    U.error(log, "Exception in discovery notyfier worker thread.", t);

                    FailureType type = t instanceof OutOfMemoryError ? CRITICAL_ERROR : SYSTEM_WORKER_TERMINATION;

                    ctx.failure().process(new FailureContext(type, t));

                    throw t;
                }
            }
        }
    }

    /** Worker for discovery events. */
    private class DiscoveryWorker extends GridWorker {
        /** */
        private DiscoCache discoCache;

        /** Event queue. */
        private final BlockingQueue<GridTuple6<Integer, AffinityTopologyVersion, ClusterNode,
            DiscoCache, Collection<ClusterNode>, DiscoveryCustomMessage>> evts = new LinkedBlockingQueue<>();

        /** Restart process handler. */
        private final RestartProcessFailureHandler restartProcHnd = new RestartProcessFailureHandler();

        /** Stop node handler. */
        private final StopNodeFailureHandler stopNodeHnd = new StopNodeFailureHandler();

        /** Node segmented event fired flag. */
        private boolean nodeSegFired;

        /**
         *
         */
        private DiscoveryWorker() {
            super(ctx.igniteInstanceName(), "disco-event-worker", GridDiscoveryManager.this.log, ctx.workersRegistry());
        }

        /**
         * Method is called when any discovery event occurs.
         *
         * @param type Discovery event type. See {@link DiscoveryEvent} for more details.
         * @param topVer Topology version.
         * @param node Remote node this event is connected with.
         * @param discoCache Discovery cache.
         * @param topSnapshot Topology snapshot.
         */
        @SuppressWarnings("RedundantTypeArguments")
        private void recordEvent(int type, long topVer, ClusterNode node, DiscoCache discoCache, Collection<ClusterNode> topSnapshot) {
            assert node != null;

            if (ctx.event().isRecordable(type)) {
                DiscoveryEvent evt = new DiscoveryEvent();

                evt.node(ctx.discovery().localNode());
                evt.eventNode(node);
                evt.type(type);
                evt.topologySnapshot(topVer, U.<ClusterNode, ClusterNode>arrayList(topSnapshot, FILTER_NOT_DAEMON));

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

                ctx.event().record(evt, discoCache);
            }
        }

        /**
         * @param type Event type.
         * @param topVer Topology version.
         * @param node Node.
         * @param discoCache Discovery cache.
         * @param topSnapshot Topology snapshot.
         * @param data Custom message.
         */
        void addEvent(
            int type,
            AffinityTopologyVersion topVer,
            ClusterNode node,
            DiscoCache discoCache,
            Collection<ClusterNode> topSnapshot,
            @Nullable DiscoveryCustomMessage data
        ) {
            assert node != null : data;

            evts.add(new GridTuple6<>(type, topVer, node, discoCache, topSnapshot, data));
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            while (!isCancelled()) {
                try {
                    body0();
                }
                catch (InterruptedException e) {
                    if (!isCancelled)
                        ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, e));

                    throw e;
                }
                catch (Throwable t) {
                    U.error(log, "Exception in discovery event worker thread.", t);

                    FailureType type = t instanceof OutOfMemoryError ? CRITICAL_ERROR : SYSTEM_WORKER_TERMINATION;

                    ctx.failure().process(new FailureContext(type, t));

                    throw t;
                }
            }
        }

        /** @throws InterruptedException If interrupted. */
        @SuppressWarnings("DuplicateCondition")
        private void body0() throws InterruptedException {
            GridTuple6<Integer, AffinityTopologyVersion, ClusterNode, DiscoCache, Collection<ClusterNode>,
                DiscoveryCustomMessage> evt = evts.take();

            int type = evt.get1();

            AffinityTopologyVersion topVer = evt.get2();

            if (type == EVT_NODE_METRICS_UPDATED && (discoCache == null || topVer.compareTo(discoCache.version()) < 0))
                return;

            ClusterNode node = evt.get3();

            boolean isDaemon = node.isDaemon();

            boolean segmented = false;

            if (evt.get4() != null)
                discoCache = evt.get4();

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

                            ackTopology(topVer.topologyVersion(), type, node, true);
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

                            ackTopology(topVer.topologyVersion(), type, node, true);
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
                        ackTopology(topVer.topologyVersion(), type, node, true);

                    break;
                }

                case EVT_NODE_FAILED: {
                    // Check only if resolvers were configured.
                    if (hasRslvrs)
                        segChkWrk.scheduleSegmentCheck();

                    if (!isDaemon) {
                        if (!isLocDaemon) {
                            U.warn(log, "Node FAILED: " + node);

                            ackTopology(topVer.topologyVersion(), type, node, true);
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

                case EVT_DISCOVERY_CUSTOM_EVT: {
                    if (ctx.event().isRecordable(EVT_DISCOVERY_CUSTOM_EVT)) {
                        DiscoveryCustomEvent customEvt = new DiscoveryCustomEvent();

                        customEvt.node(ctx.discovery().localNode());
                        customEvt.eventNode(node);
                        customEvt.type(type);
                        customEvt.topologySnapshot(topVer.topologyVersion(), evt.get5());
                        customEvt.affinityTopologyVersion(topVer);
                        customEvt.customMessage(evt.get6());

                        if (evt.get4() == null) {
                            assert discoCache != null : evt.get6();

                            evt.set4(discoCache);
                        }

                        ctx.event().record(customEvt, evt.get4());
                    }

                    return;
                }

                // Don't log metric update to avoid flooding the log.
                case EVT_NODE_METRICS_UPDATED:
                    break;

                default:
                    assert false : "Invalid discovery event: " + type;
            }

            recordEvent(type, topVer.topologyVersion(), node, evt.get4(), evt.get5());

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
                    ctx.failure().process(new FailureContext(FailureType.SEGMENTATION, null), restartProcHnd);

                    break;

                case STOP:
                    ctx.failure().process(new FailureContext(FailureType.SEGMENTATION, null), stopNodeHnd);

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

    /**
     *
     */
    private static class CacheGroupAffinity {
        /** */
        private final String name;

        /** Nodes filter. */
        private final IgnitePredicate<ClusterNode> cacheFilter;

        /** Cache mode. */
        private final CacheMode cacheMode;

        /** Persistent cache group or not. */
        private final boolean persistentCacheGrp;

        /**
         * @param name Name.
         * @param cacheFilter Node filter.
         * @param cacheMode Cache mode.
         * @param persistentCacheGrp Persistence is configured for cache or not.
         */
        CacheGroupAffinity(
                String name,
                IgnitePredicate<ClusterNode> cacheFilter,
                CacheMode cacheMode,
                boolean persistentCacheGrp) {
            this.name = name;
            this.cacheFilter = cacheFilter;
            this.cacheMode = cacheMode;
            this.persistentCacheGrp = persistentCacheGrp;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheGroupAffinity.class, this);
        }
    }

    /**
     * Cache predicate.
     */
    private class CachePredicate {
        /** */
        private final int cacheId;

        /** Cache filter. */
        private final CacheGroupAffinity aff;

        /** If near cache is enabled on data nodes. */
        private final boolean nearEnabled;

        /**
         * Collection of client nodes.
         *
         * Note: if client cache started/closed this map is updated asynchronously.
         */
        private final ConcurrentHashMap<UUID, Boolean> clientNodes;

        /**
         * @param cacheId Cache ID.
         * @param aff Cache group affinity.
         * @param nearEnabled Near enabled flag.
         */
        private CachePredicate(int cacheId, CacheGroupAffinity aff, boolean nearEnabled) {
            assert aff != null;

            this.cacheId = cacheId;
            this.aff = aff;
            this.nearEnabled = nearEnabled;

            clientNodes = new ConcurrentHashMap<>();
        }

        /**
         * @param nodeId Near node ID to add.
         * @param nearEnabled Near enabled flag.
         * @return {@code True} if new node ID was added.
         */
        boolean addClientNode(UUID nodeId, boolean nearEnabled) {
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
            return CU.affinityNode(node, aff.cacheFilter);
        }

        /**
         * @param node Node to check.
         * @return {@code True} if cache is accessible on the given node.
         */
        boolean cacheNode(ClusterNode node) {
            return !node.isDaemon() && (CU.affinityNode(node, aff.cacheFilter) ||
                cacheClientNode(node) != null);
        }

        /**
         * @param node Node to check.
         * @return {@code True} if near cache is present on the given nodes.
         */
        boolean nearNode(ClusterNode node) {
            if (CU.affinityNode(node, aff.cacheFilter))
                return nearEnabled;

            Boolean near = cacheClientNode(node);

            return near != null && near;
        }

        /**
         * @param node Node to check.
         * @return {@code True} if client cache is present on the given nodes.
         */
        public boolean clientNode(ClusterNode node) {
            if (node.isDaemon())
                return false;

            Boolean near = cacheClientNode(node);

            return near != null && !near;
        }

        /**
         * @param node Node.
         * @return {@code Null} if client cache does not exist, otherwise cache near enabled flag.
         */
        private Boolean cacheClientNode(ClusterNode node) {
            // On local node check actual cache state since clientNodes map is updated asynchronously.
            if (ctx.localNodeId().equals(node.id())) {
                GridCacheContext cctx = ctx.cache().context().cacheContext(cacheId);

                return cctx != null ? CU.isNearEnabled(cctx) : null;
            }

            return clientNodes.get(node.id());
        }
    }

    /**
     * Fills affinity node caches.
     * @param allNodes All nodes.
     * @param allCacheNodes All cache nodes.
     * @param cacheGrpAffNodes Cache group aff nodes.
     * @param rmtNodesWithCaches Rmt nodes with caches.
     * @param bltNodes Baseline node ids.
     */
    private void fillAffinityNodeCaches(
        List<ClusterNode> allNodes,
        Map<Integer, List<ClusterNode>> allCacheNodes,
        Map<Integer, List<ClusterNode>> cacheGrpAffNodes,
        Set<ClusterNode> rmtNodesWithCaches,
        Set<UUID> bltNodes
    ) {
        for (ClusterNode node : allNodes) {
            assert node.order() != 0 : "Invalid node order [locNode=" + localNode() + ", node=" + node + ']';
            assert !node.isDaemon();

            for (Map.Entry<Integer, CacheGroupAffinity> e : registeredCacheGrps.entrySet()) {
                CacheGroupAffinity grpAff = e.getValue();
                Integer grpId = e.getKey();

                if (CU.affinityNode(node, grpAff.cacheFilter)) {
                    if (grpAff.persistentCacheGrp && bltNodes != null && !bltNodes.contains(node.id())) // Filter out.
                        continue;

                    List<ClusterNode> nodes = cacheGrpAffNodes.get(grpId);

                    if (nodes == null)
                        cacheGrpAffNodes.put(grpId, nodes = new ArrayList<>());

                    nodes.add(node);
                }
            }

            for (Map.Entry<String, CachePredicate> entry : registeredCaches.entrySet()) {
                String cacheName = entry.getKey();
                CachePredicate filter = entry.getValue();

                if (filter.cacheNode(node)) {
                    if (!node.isLocal())
                        rmtNodesWithCaches.add(node);

                    addToMap(allCacheNodes, cacheName, node);
                }
            }
        }
    }

    /**
     * Creates discovery cache after {@link DynamicCacheChangeBatch} received.
     *
     * @param topVer Topology version.
     * @param discoCache Current disco cache.
     * @return New discovery cache.
     */
    public DiscoCache createDiscoCacheOnCacheChange(
        AffinityTopologyVersion topVer,
        DiscoCache discoCache
    ) {
        List<ClusterNode> allNodes = discoCache.allNodes();
        Map<Integer, List<ClusterNode>> allCacheNodes = U.newHashMap(allNodes.size());
        Map<Integer, List<ClusterNode>> cacheGrpAffNodes = U.newHashMap(allNodes.size());
        Set<ClusterNode> rmtNodesWithCaches = new TreeSet<>(NodeOrderComparator.getInstance());

        Map<UUID, Short> nodeIdToConsIdx = discoCache.nodeIdToConsIdx;

        fillAffinityNodeCaches(allNodes, allCacheNodes, cacheGrpAffNodes, rmtNodesWithCaches,
                nodeIdToConsIdx == null ? null : nodeIdToConsIdx.keySet());

        return new DiscoCache(
            topVer,
            discoCache.state(),
            discoCache.localNode(),
            discoCache.mvccCoordinator(),
            discoCache.remoteNodes(),
            allNodes,
            discoCache.serverNodes(),
            discoCache.daemonNodes(),
            U.sealList(rmtNodesWithCaches),
            discoCache.baselineNodes(),
            allCacheNodes,
            cacheGrpAffNodes,
            discoCache.nodeMap,
            discoCache.alives,
            nodeIdToConsIdx,
            discoCache.consIdxToNodeId,
            discoCache.minimumNodeVersion(),
            discoCache.minimumServerNodeVersion());
    }
}
