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

package org.apache.ignite.internal.processors.cache.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class IgniteProcessProxy implements IgniteEx {
    private static final Map<String, IgniteProcessProxy> gridProxies = new HashMap<>();
    
    private final GridJavaProcess proc;
    private final IgniteConfiguration cfg;
    private final IgniteLogger log;
    private final UUID id = UUID.randomUUID();// TODO send.

    public IgniteProcessProxy(final IgniteConfiguration cfg, final IgniteLogger log) throws Exception {
        this.cfg = cfg;
        this.log = log;

        String cfgAsString = IgniteNodeRunner.asParams(id, cfg);
        
        proc = GridJavaProcess.exec(
            IgniteNodeRunner.class,
            cfgAsString, // Params.
            log,
            // Optional closure to be called each time wrapped process prints line to system.out or system.err.
            new IgniteInClosure<String>() {
                @Override public void apply(String s) {
                    log.info("[" + cfg.getGridName() + "] " + s);
                }
            },
            null,
            Collections.<String>emptyList(), // JVM Args.
            System.getProperty("surefire.test.class.path")
        );
        
        Thread.sleep(3_000);
        
        gridProxies.put(cfg.getGridName(), this);
    }

    @Override public String name() {
        return cfg.getGridName();
    }

    @Override public IgniteLogger log() {
        return log;
    }

    @Override public IgniteConfiguration configuration() {
        return cfg;
    }

    @Override public <K extends GridCacheUtilityKey, V> GridCacheProjectionEx<K, V> utilityCache() {
        return null; // TODO: CODE: implement.
    }

    @Nullable @Override public <K, V> GridCache<K, V> cachex(@Nullable String name) {
        return null; // TODO: CODE: implement.
    }

    @Nullable @Override public <K, V> GridCache<K, V> cachex() {
        return null; // TODO: CODE: implement.
    }

    @Override public Collection<GridCache<?, ?>> cachesx(@Nullable IgnitePredicate<? super GridCache<?, ?>>... p) {
        return null; // TODO: CODE: implement.
    }

    @Override public boolean eventUserRecordable(int type) {
        return false; // TODO: CODE: implement.
    }

    @Override public boolean allEventsUserRecordable(int[] types) {
        return false; // TODO: CODE: implement.
    }

    @Override public boolean isJmxRemoteEnabled() {
        return false; // TODO: CODE: implement.
    }

    @Override public boolean isRestartEnabled() {
        return false; // TODO: CODE: implement.
    }

    @Nullable @Override public IgniteFileSystem igfsx(@Nullable String name) {
        return null; // TODO: CODE: implement.
    }

    @Override public Hadoop hadoop() {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteClusterEx cluster() {
//        Ignition.setClientMode(true);
        
        // TODO cfg.
        try(Ignite ignite = Ignition.start(IgniteNodeRunner.configuration(null))) {
            // TODO
            ClusterGroup grp = ignite.cluster().forNodeId(id);

            return ignite.compute(grp).apply(new C1<Set<String>, IgniteClusterEx>() {
                @Override public IgniteClusterEx apply(Set<String> objects) {
                    X.println(">>>>> Cluster 1");
                    Ignite ignite1 = Ignition.ignite();
                    X.println(">>>>> Cluster 2");
                    IgniteCluster cluster = ignite1.cluster();
                    X.println(">>>>> Cluster 3");
                    IgniteClusterEx cluster1 = (IgniteClusterEx)cluster;
                    X.println(">>>>> Cluster 4");
                    return cluster1;
                }
            }, Collections.<String>emptySet());
        }
    }

    @Nullable @Override public String latestVersion() {
        return null; // TODO: CODE: implement.
    }

    @Override public ClusterNode localNode() {
        return null; // TODO: CODE: implement.
    }

    @Override public GridKernalContext context() {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteCompute compute() {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteCompute compute(ClusterGroup grp) {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteMessaging message() {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteMessaging message(ClusterGroup grp) {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteEvents events() {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteEvents events(ClusterGroup grp) {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteServices services() {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteServices services(ClusterGroup grp) {
        return null; // TODO: CODE: implement.
    }

    @Override public ExecutorService executorService() {
        return null; // TODO: CODE: implement.
    }

    @Override public ExecutorService executorService(ClusterGroup grp) {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteProductVersion version() {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteScheduler scheduler() {
        return null; // TODO: CODE: implement.
    }

    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        return null; // TODO: CODE: implement.
    }

    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        return null; // TODO: CODE: implement.
    }

    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        return null; // TODO: CODE: implement.
    }

    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        return null; // TODO: CODE: implement.
    }

    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        // TODO: CODE: implement.
    }

    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        return null; // TODO: CODE: implement.
    }

    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        return null; // TODO: CODE: implement.
    }

    @Override
    public <K, V> IgniteCache<K, V> createNearCache(@Nullable String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        return null; // TODO: CODE: implement.
    }

    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String cacheName,
        NearCacheConfiguration<K, V> nearCfg) {
        return null; // TODO: CODE: implement.
    }

    @Override public void destroyCache(String cacheName) {
        // TODO: CODE: implement.
    }

    @Override public <K, V> IgniteCache<K, V> cache(@Nullable String name) {
        Ignition.setClientMode(true);
        
        try(Ignite ignite = Ignition.start(cfg)) {
            return ignite.compute(ignite.cluster().forNodeId(id)).apply(new C1<Set<?>, IgniteCache<K,V>>() {
                @Override public IgniteCache<K, V> apply(Set<?> objects) {
                    return Ignition.ignite().cache(null);
                }
            }, Collections.emptySet());
        }      
    }

    @Override public IgniteTransactions transactions() {
        return null; // TODO: CODE: implement.
    }

    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName) {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteFileSystem fileSystem(String name) {
        return null; // TODO: CODE: implement.
    }

    @Override public Collection<IgniteFileSystem> fileSystems() {
        return null; // TODO: CODE: implement.
    }

    @Override
    public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) throws IgniteException {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException {
        return null; // TODO: CODE: implement.
    }

    @Override public <T> IgniteAtomicReference<T> atomicReference(String name, @Nullable T initVal,
        boolean create) throws IgniteException {
        return null; // TODO: CODE: implement.
    }

    @Override
    public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, @Nullable T initVal, @Nullable S initStamp,
        boolean create) throws IgniteException {
        return null; // TODO: CODE: implement.
    }

    @Override public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
        boolean create) throws IgniteException {
        return null; // TODO: CODE: implement.
    }

    @Override public <T> IgniteQueue<T> queue(String name, int cap,
        @Nullable CollectionConfiguration cfg) throws IgniteException {
        return null; // TODO: CODE: implement.
    }

    @Override public <T> IgniteSet<T> set(String name, @Nullable CollectionConfiguration cfg) throws IgniteException {
        return null; // TODO: CODE: implement.
    }

    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        return null; // TODO: CODE: implement.
    }

    @Override public void close() throws IgniteException {
        // TODO: CODE: implement.
    }

    @Override public <K> Affinity<K> affinity(String cacheName) {
        return null; // TODO: CODE: implement.
    }

    public GridJavaProcess getProcess() {
        return proc;
    }

    public static Ignite grid(@Nullable String name) {
//        IgniteNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        Ignite res = gridProxies.get(name);

        if (res == null)
            throw new IgniteIllegalStateException("Grid instance was not properly started " +
                "or was already stopped: " + name);

        return res;
    }
}
