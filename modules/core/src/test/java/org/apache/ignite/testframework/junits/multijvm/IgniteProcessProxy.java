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

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Ignite proxy for ignite instance at another JVM.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public class IgniteProcessProxy implements IgniteEx {
    /** Grid proxies. */
    private static final transient Map<String, IgniteProcessProxy> gridProxies = new HashMap<>();

    /** Jvm process with ignite instance. */
    private final transient GridJavaProcess proc;

    /** Configuration. */
    private final transient IgniteConfiguration cfg;

    /** Local jvm grid. */
    private final transient Ignite locJvmGrid;

    /** Logger. */
    private final transient IgniteLogger log;

    /** Grid id. */
    private final UUID id = UUID.randomUUID();

    /** Remote ignite instance started latch. */
    private final transient CountDownLatch rmtNodeStartedLatch = new CountDownLatch(1);

    /**
     * @param cfg Configuration.
     * @param log Logger.
     * @param locJvmGrid Local jvm grid.
     */
    public IgniteProcessProxy(final IgniteConfiguration cfg, final IgniteLogger log, final Ignite locJvmGrid)
        throws Exception {
        this.cfg = cfg;
        this.locJvmGrid = locJvmGrid;
        this.log = log.getLogger("jvm-" + id.toString().substring(0, id.toString().indexOf('-')));

        String cfgFileName = IgniteNodeRunner.storeToFile(cfg.setNodeId(id));

        List<String> jvmArgs = U.jvmArgs();

        List<String> filteredJvmArgs = new ArrayList<>();

        for (String arg : jvmArgs) {
            if(!arg.toLowerCase().startsWith("-agentlib"))
                filteredJvmArgs.add(arg);
        }

        locJvmGrid.events().localListen(new IgnitePredicateX<Event>() {
            @Override public boolean applyx(Event e) {
                if (((DiscoveryEvent)e).eventNode().id().equals(id)) {
                    rmtNodeStartedLatch.countDown();

                    return false;
                }

                return true;
            }
        }, EventType.EVT_NODE_JOINED);

        proc = GridJavaProcess.exec(
            IgniteNodeRunner.class,
            cfgFileName, // Params.
            this.log,
            // Optional closure to be called each time wrapped process prints line to system.out or system.err.
            new IgniteInClosure<String>() {
                @Override public void apply(String s) {
                    IgniteProcessProxy.this.log.info(s);
                }
            },
            null,
            filteredJvmArgs, // JVM Args.
            System.getProperty("surefire.test.class.path")
        );

        assert rmtNodeStartedLatch.await(30, TimeUnit.SECONDS): "Remote node with id=" + id + " didn't join.";

        gridProxies.put(cfg.getGridName(), this);
    }

    /**
     * @param gridName Grid name.
     * @return Instance by name or exception wiil be thrown.
     */
    public static IgniteProcessProxy ignite(String gridName) {
        IgniteProcessProxy res = gridProxies.get(gridName);

        if (res == null)
            throw new IgniteIllegalStateException("Grid instance was not properly started " +
                "or was already stopped: " + gridName + ". All known grid instances: " + gridProxies.keySet());

        return res;
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
        return locJvmGrid;
    }

    /**
     * @return Grid id.
     */
    public UUID getId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cfg.getGridName();
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
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> IgniteInternalCache<K, V> cachex(@Nullable String name) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> IgniteInternalCache<K, V> cachex() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteInternalCache<?, ?>> cachesx(
        @Nullable IgnitePredicate<? super IgniteInternalCache<?, ?>>... p) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean eventUserRecordable(int type) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean allEventsUserRecordable(int[] types) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean isJmxRemoteEnabled() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean isRestartEnabled() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFileSystem igfsx(@Nullable String name) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Hadoop hadoop() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteClusterEx cluster() {
        return (IgniteClusterEx)locJvmGrid.cluster();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String latestVersion() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return remoteCompute().call(new IgniteCallable<ClusterNode>() {
            @Override public ClusterNode call() throws Exception {
                return ((IgniteEx)Ignition.ignite(id)).localNode();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridKernalContext context() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        return new IgniteEventsProcessProxy(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override  public <K, V> IgniteCache<K, V> createNearCache(@Nullable String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String cacheName,
        NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(@Nullable final String name) {
        return new IgniteCacheProcessProxy(name, this);
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        throw new UnsupportedOperationException("Transactions are not supported in multi JVM mode.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem fileSystem(String name) {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> fileSystems() {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override  public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name, @Nullable T initVal,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override  public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, @Nullable T initVal, @Nullable S initStamp,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteQueue<T> queue(String name, int cap,
        @Nullable CollectionConfiguration cfg) throws IgniteException {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteSet<T> set(String name, @Nullable CollectionConfiguration cfg) throws IgniteException {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        try {
            getProcess().kill();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        return new AffinityProcessProxy(cacheName, this);
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
        ClusterGroup grp = locJvmGrid.cluster().forNodeId(id);

        if (grp.nodes().isEmpty())
            throw new IllegalStateException("Could not found node with id=" + id + ".");

        return locJvmGrid.compute(grp);
    }
}
