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

package org.apache.ignite.testframework.junits.multijvm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.cache.CacheException;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataRegionMetricsAdapter;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.DataStorageMetricsAdapter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite proxy for ignite instance at another JVM.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public class IgniteProcessProxy implements IgniteEx {
    /** Grid proxies. */
    private static final transient ConcurrentMap<String, IgniteProcessProxy> gridProxies = new ConcurrentHashMap<>();

    /** Property that specify alternative {@code JAVA_HOME}. */
    private static final String TEST_MULTIJVM_JAVA_HOME = "test.multijvm.java.home";

    /** Waiting milliseconds of the left of a node to topology. */
    private static final long NODE_LEFT_TIMEOUT = 30_000L;

    /** Jvm process with ignite instance. */
    private final transient GridJavaProcess proc;

    /** Configuration. */
    private final transient IgniteConfiguration cfg;

    /** Local JVM grid. */
    private final transient Function<Void, Ignite> locJvmGrid;

    /** Logger. */
    private final transient IgniteLogger log;

    /** Grid id. */
    private final UUID id;

    /**
     * @param cfg Configuration.
     * @param log Logger.
     * @param locJvmGrid Local JVM grid.
     * @throws Exception On error.
     */
    public IgniteProcessProxy(IgniteConfiguration cfg, IgniteLogger log, Ignite locJvmGrid)
        throws Exception {
        this(cfg, log, (Function<Void, Ignite>)locJvmGrid, true);
    }

    /**
     * @param cfg Configuration.
     * @param log Logger.
     * @param locJvmGrid Local JVM grid.
     * @throws Exception On error.
     */
    public IgniteProcessProxy(IgniteConfiguration cfg, IgniteLogger log, Function<Void, Ignite> locJvmGrid, boolean discovery)
        throws Exception {
        this(cfg, log, locJvmGrid, discovery, Collections.emptyList());
    }

    /**
     * @param cfg Configuration.
     * @param log Logger.
     * @param locJvmGrid Local JVM grid.
     * @param resetDiscovery Reset DiscoverySpi at the configuration.
     * @throws Exception On error.
     */
    public IgniteProcessProxy(
        IgniteConfiguration cfg,
        IgniteLogger log,
        Function<Void, Ignite> locJvmGrid,
        boolean resetDiscovery,
        List<String> additionalArgs
    )
        throws Exception {
        this.cfg = cfg;
        this.id = cfg.getNodeId() == null ? UUID.randomUUID() : cfg.getNodeId();
        this.locJvmGrid = locJvmGrid;
        this.log = logger(log, "jvm-" + id.toString().substring(0, id.toString().indexOf('-')));

        String params = params(cfg, resetDiscovery);

        Collection<String> filteredJvmArgs = filteredJvmArgs();
        filteredJvmArgs.addAll(additionalArgs);

        final CountDownLatch rmtNodeStartedLatch = new CountDownLatch(1);

        if (locJvmGrid != null)
            locJvmGrid.apply(null).events()
                .localListen(new NodeStartedListener(id, rmtNodeStartedLatch), EventType.EVT_NODE_JOINED);

        proc = GridJavaProcess.exec(
            igniteNodeRunnerClassName(),
            params,
            this.log,
            // Optional closure to be called each time wrapped process prints line to system.out or system.err.
            new IgniteInClosure<String>() {
                @Override public void apply(String s) {
                    IgniteProcessProxy.this.log.info(s);
                }
            },
            null,
            System.getProperty(TEST_MULTIJVM_JAVA_HOME),
            filteredJvmArgs, // JVM Args.
            System.getProperty("surefire.test.class.path")
        );

        if (locJvmGrid != null)
            assert rmtNodeStartedLatch.await(30, TimeUnit.SECONDS) : "Remote node has not joined [id=" + id + ']';

        IgniteProcessProxy prevVal = gridProxies.putIfAbsent(cfg.getIgniteInstanceName(), this);

        if (prevVal != null) {
            remoteCompute().run(new StopGridTask(cfg.getIgniteInstanceName(), true));

            throw new IllegalStateException("There was found instance assotiated with " + cfg.getIgniteInstanceName() +
                ", instance= " + prevVal + ". New started node was stopped.");
        }
    }

    /**
     * Creates new logger instance based on given logger and given category.
     *
     * @param log Base logger.
     * @param ctgr Category.
     * @return Initiated logger.
     * @throws Exception In case of an error.
     */
    protected IgniteLogger logger(IgniteLogger log, Object ctgr) throws Exception {
        return log.getLogger(ctgr);
    }

    /**
     * Gets Ignite node runner class name.
     *
     * @return Node runner class name.
     * @throws Exception In case of an error.
     */
    protected String igniteNodeRunnerClassName() throws Exception {
        return IgniteNodeRunner.class.getCanonicalName();
    }

    /**
     * Creates parameters which will be passed to new Ignite Process as command line arguments.
     *
     * @param cfg Configuration.
     * @param resetDiscovery Reset DiscoverySpi at the configuration.
     * @return Params to be passed to new Ignite process.
     * @throws Exception In case of an error.
     */
    protected String params(IgniteConfiguration cfg, boolean resetDiscovery) throws Exception {
        return IgniteNodeRunner.storeToFile(cfg.setNodeId(id), resetDiscovery);
    }

    /**
     * Creates list of JVM arguments to be used to start new Ignite process in separate JVM.
     *
     * @return JVM arguments.
     * @throws Exception In case of an error.
     */
    protected Collection<String> filteredJvmArgs() throws Exception {
        Collection<String> filteredJvmArgs = new ArrayList<>();

        filteredJvmArgs.add("-ea");

        Marshaller marsh = cfg.getMarshaller();

        if (marsh != null)
            filteredJvmArgs.add("-D" + IgniteTestResources.MARSH_CLASS_NAME + "=" + marsh.getClass().getName());
        else
            filteredJvmArgs.add("-D" + IgniteTestResources.MARSH_CLASS_NAME + "=" + BinaryMarshaller.class.getName());

        for (String arg : U.jvmArgs()) {
            if (arg.startsWith("-Xmx") || arg.startsWith("-Xms") ||
                arg.startsWith("-cp") || arg.startsWith("-classpath") ||
                (marsh != null && arg.startsWith("-D" + IgniteTestResources.MARSH_CLASS_NAME)) ||
                arg.startsWith("--add-opens") || arg.startsWith("--add-exports") || arg.startsWith("--add-modules") ||
                arg.startsWith("--patch-module") || arg.startsWith("--add-reads") ||
                arg.startsWith("-XX:+IgnoreUnrecognizedVMOptions") ||
                arg.startsWith(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS))
                filteredJvmArgs.add(arg);
        }

        return filteredJvmArgs;
    }

    /**
     */
    private static class NodeStartedListener extends IgnitePredicateX<Event> {
        /** Id. */
        private final UUID id;

        /** Remote node started latch. */
        private final CountDownLatch rmtNodeStartedLatch;

        /**
         * @param id Id.
         * @param rmtNodeStartedLatch Remote node started latch.
         */
        NodeStartedListener(UUID id, CountDownLatch rmtNodeStartedLatch) {
            this.id = id;
            this.rmtNodeStartedLatch = rmtNodeStartedLatch;
        }

        /** {@inheritDoc} */
        @Override public boolean applyx(Event e) {
            if (((DiscoveryEvent)e).eventNode().id().equals(id)) {
                rmtNodeStartedLatch.countDown();

                return false;
            }

            return true;
        }
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Instance by name or exception wiil be thrown.
     */
    public static IgniteProcessProxy ignite(String igniteInstanceName) {
        IgniteProcessProxy res = gridProxies.get(igniteInstanceName);

        if (res == null)
            throw new IgniteIllegalStateException("Grid instance was not properly started " +
                "or was already stopped: " + igniteInstanceName + ". All known grid instances: " +
                gridProxies.keySet());

        return res;
    }

    /**
     * Gracefully shut down the Grid.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param cancel If {@code true} then all jobs currently will be cancelled.
     * @throws Exception In case of the node stopping error.
     */
    public static void stop(String igniteInstanceName, boolean cancel) throws Exception {
        final IgniteProcessProxy proxy = gridProxies.get(igniteInstanceName);

        if (proxy != null) {
            final CountDownLatch rmtNodeStoppedLatch = new CountDownLatch(1);
            final UUID rmNodeId = proxy.getId();

            proxy.localJvmGrid().events().localListen(new IgnitePredicateX<Event>() {
                @Override public boolean applyx(Event e) {
                    if (((DiscoveryEvent)e).eventNode().id().equals(rmNodeId)) {
                        rmtNodeStoppedLatch.countDown();

                        return false;
                    }

                    return true;
                }
            }, EventType.EVT_NODE_LEFT);

            try {
                proxy.remoteCompute().runAsync(new StopGridTask(igniteInstanceName, cancel));

                if (!rmtNodeStoppedLatch.await(NODE_LEFT_TIMEOUT, TimeUnit.MILLISECONDS))
                    throw new IllegalStateException("Remote node has not stopped [id=" + rmNodeId + ']');
            }
            catch (Throwable t) {
                proxy.log().error("Failed to stop grid [igniteInstanceName=" + igniteInstanceName +
                    ", cancel=" + cancel + ']', t);

                throw t;
            }

            proxy.getProcess().kill();

            gridProxies.remove(igniteInstanceName, proxy);
        }
    }

    /**
     * Forcefully shut down the Grid.
     *
     * @param igniteInstanceName Ignite instance name.
     */
    public static void kill(String igniteInstanceName) {
        A.notNull(igniteInstanceName, "igniteInstanceName");

        IgniteProcessProxy proxy = gridProxies.get(igniteInstanceName);

        if (proxy == null)
            return;

        if (proxy == null)
            return;

        try {
            proxy.getProcess().kill();
        }
        catch (Exception e) {
            U.error(proxy.log, "Exception while killing " + igniteInstanceName, e);
        }

        gridProxies.remove(igniteInstanceName, proxy);
    }

    /**
     * @param locNodeId ID of local node the requested grid instance is managing.
     * @return An instance of named grid. This method never returns {@code null}.
     * @throws IgniteIllegalStateException Thrown if grid was not properly initialized or grid instance was stopped or
     * was not started.
     */
    public static Ignite ignite(UUID locNodeId) {
        A.notNull(locNodeId, "locNodeId");

        for (IgniteProcessProxy ignite : gridProxies.values()) {
            if (ignite.getId().equals(locNodeId))
                return ignite;
        }

        throw new IgniteIllegalStateException("Grid instance with given local node ID was not properly " +
            "started or was stopped: " + locNodeId);
    }

    /**
     * Kill all running processes.
     */
    public static void killAll() {
        for (IgniteProcessProxy ignite : gridProxies.values()) {
            try {
                ignite.getProcess().kill();
            }
            catch (Exception e) {
                U.error(ignite.log, "Killing failed.", e);
            }
        }

        gridProxies.clear();
    }

    /**
     * @return Local JVM grid instance.
     */
    public Ignite localJvmGrid() {
        return locJvmGrid.apply(null);
    }

    /**
     * @return Grid id.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @throws Exception If failed to kill.
     */
    public void kill() throws Exception {
        getProcess().kill();

        gridProxies.remove(cfg.getGridName(), this);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cfg.getIgniteInstanceName();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        return log;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public <K extends GridCacheUtilityKey, V> IgniteInternalCache<K, V> utilityCache() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> IgniteInternalCache<K, V> cachex(@Nullable String name) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteInternalCache<?, ?>> cachesx(
        @Nullable IgnitePredicate<? super IgniteInternalCache<?, ?>>... p) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean eventUserRecordable(int type) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean allEventsUserRecordable(int[] types) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean isJmxRemoteEnabled() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean isRestartEnabled() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteClusterEx cluster() {
        return new IgniteClusterProcessProxy(this);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String latestVersion() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return remoteCompute().call(new NodeTask());
    }

    /** {@inheritDoc} */
    @Override public GridKernalContext context() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    @Override public boolean isRebalanceEnabled() {
        return true;
    }

    @Override public void rebalanceEnabled(boolean rebalanceEnabled) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        return new IgniteEventsProcessProxy(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> createCaches(Collection<CacheConfiguration> cacheCfgs) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> cacheCfgs) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createNearCache(
        @Nullable String cacheName,
        NearCacheConfiguration<K, V> nearCfg)
    {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String cacheName,
        NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteBiTuple<IgniteCache<K, V>, Boolean> getOrCreateCache0(
        CacheConfiguration<K, V> cacheCfg, boolean sql) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean destroyCache0(String cacheName, boolean sql) throws CacheException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void destroyCaches(Collection<String> cacheNames) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(@Nullable final String name) {
        return new IgniteCacheProcessProxy<>(name, this);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        return localJvmGrid().cacheNames();
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        throw new UnsupportedOperationException("Transactions can't be supported automatically in multi JVM mode.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create)
        throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicSequence atomicSequence(String name, AtomicConfiguration cfg, long initVal,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name, AtomicConfiguration cfg, long initVal,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name, @Nullable T initVal,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name, AtomicConfiguration cfg,
        @Nullable T initVal, boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(
        String name,
        @Nullable T initVal,
        @Nullable S initStamp,
        boolean create) throws IgniteException
    {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, AtomicConfiguration cfg,
        @Nullable T initVal, @Nullable S initStamp, boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteSemaphore semaphore(String name, int cnt, boolean failoverSafe,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteLock reentrantLock(String name, boolean failoverSafe,
        boolean fair, boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteQueue<T> queue(String name, int cap,
        @Nullable CollectionConfiguration cfg) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteSet<T> set(String name, @Nullable CollectionConfiguration cfg) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void resetLostPartitions(Collection<String> cacheNames) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRegionMetrics> dataRegionMetrics() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public DataRegionMetrics dataRegionMetrics(String memPlcName) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public DataStorageMetrics dataStorageMetrics() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteEncryption encryption() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteSnapshot snapshot() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<MemoryMetrics> memoryMetrics() {
        return DataRegionMetricsAdapter.collectionOf(dataRegionMetrics());
    }

    /** {@inheritDoc} */
    @Nullable @Override public MemoryMetrics memoryMetrics(String memPlcName) {
        return DataRegionMetricsAdapter.valueOf(dataRegionMetrics(memPlcName));
    }

    /** {@inheritDoc} */
    @Override public PersistenceMetrics persistentStoreMetrics() {
        return DataStorageMetricsAdapter.valueOf(dataStorageMetrics());
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationManager tracingConfiguration() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        if (localJvmGrid() != null) {
            final CountDownLatch rmtNodeStoppedLatch = new CountDownLatch(1);

            localJvmGrid().events().localListen(new IgnitePredicateX<Event>() {
                @Override public boolean applyx(Event e) {
                    if (((DiscoveryEvent)e).eventNode().id().equals(id)) {
                        rmtNodeStoppedLatch.countDown();

                        return false;
                    }

                    return true;
                }
            }, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);

            compute().run(new StopGridTask(localJvmGrid().name(), true));

            try {
                assert U.await(rmtNodeStoppedLatch, 15, TimeUnit.SECONDS) : "NodeId=" + id;
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteException(e);
            }
        }

        try {
            getProcess().kill();
        }
        catch (Exception e) {
            X.printerr("Could not kill process after close.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        return new AffinityProcessProxy<>(cacheName, this);
    }

    /** {@inheritDoc} */
    @Override public boolean active() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void active(boolean active) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /**
     * @return Jvm process in which grid node started.
     */
    public GridJavaProcess getProcess() {
        return proc;
    }

    /**
     * @return {@link IgniteCompute} instance to communicate with remote node.
     */
    public IgniteCompute remoteCompute() {
        Ignite localJvmGrid = localJvmGrid();

        if (localJvmGrid == null)
            return null;

        ClusterGroup grp = localJvmGrid.cluster().forNodeId(id);

        if (grp.nodes().isEmpty())
            throw new IllegalStateException("Could not found node with id=" + id + ".");

        return localJvmGrid.compute(grp);
    }

    /**
     * Executes {@link Ignition#stop(String, boolean)} with given arguments in a separated thread, doesn't wait up the
     * fulfillment.
     */
    private static class StopGridTask implements IgniteRunnable {
        /** Ignite instance name. */
        private final String igniteInstanceName;

        /** Cancel. */
        private final boolean cancel;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param cancel Cancel.
         */
        public StopGridTask(String igniteInstanceName, boolean cancel) {
            this.igniteInstanceName = igniteInstanceName;
            this.cancel = cancel;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            CompletableFuture.runAsync(() -> G.stop(igniteInstanceName, cancel));
        }
    }

    /**
     *
     */
    private static class NodeTask implements IgniteCallable<ClusterNode> {
        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public ClusterNode call() throws Exception {
            return ((IgniteEx)ignite).localNode();
        }
    }
}
