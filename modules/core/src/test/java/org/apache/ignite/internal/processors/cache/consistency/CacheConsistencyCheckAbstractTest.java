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

package org.apache.ignite.internal.processors.cache.consistency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.consistency.IgniteConsistencyViolationException;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class CacheConsistencyCheckAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    protected static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Get and check and fail. */
    protected static final Consumer<T3<
        IgniteCache<Integer, Integer> /*initiator's cache*/,
        Map<Integer /*key*/, T3<Map<Ignite, Integer> /*mapping*/, Integer /*primary*/, Integer /*latest*/>>,
        Boolean /*raw*/>> GET_CHECK_AND_FAIL =
        (t) -> {
            IgniteCache<Integer, Integer> cache = t.get1();
            Set<Integer> keys = t.get2().keySet();
            Boolean raw = t.get3();

            assert keys.size() == 1;

            for (Integer key : keys) { // once
                try {
                    if (raw)
                        cache.withConsistencyCheck().getEntry(key);
                    else
                        cache.withConsistencyCheck().get(key);

                    fail("Should not happen.");
                }
                catch (CacheException e) {
                    assertTrue(e.getCause() instanceof IgniteConsistencyViolationException);
                }
            }
        };

    /** GetAll and check and fail. */
    protected static final Consumer<T3<
        IgniteCache<Integer, Integer> /*initiator's cache*/,
        Map<Integer /*key*/, T3<Map<Ignite, Integer> /*mapping*/, Integer /*primary*/, Integer /*latest*/>>,
        Boolean /*raw*/>> GETALL_CHECK_AND_FAIL =
        (t) -> {
            IgniteCache<Integer, Integer> cache = t.get1();
            Set<Integer> keys = t.get2().keySet();
            Boolean raw = t.get3();

            try {
                if (raw)
                    cache.withConsistencyCheck().getEntries(keys);
                else
                    cache.withConsistencyCheck().getAll(keys);

                fail("Should not happen.");
            }
            catch (CacheException e) {
                assertTrue(e.getCause() instanceof IgniteConsistencyViolationException);
            }

        };

    /** Key. */
    protected static int iterableKey;

    /**
     *
     */
    protected Integer backupsCount(){
        return 3;
    }

    /** Is client flag. */
    protected boolean client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(7);

        grid(0).getOrCreateCache(cacheConfiguration());

        client = true;

        startGrid(G.allGrids().size() + 1);
        startGrid(G.allGrids().size() + 1);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        log.info("Checked " + iterableKey + " keys");

        stopAllGrids();
    }

    /**
     *
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     *
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(atomicityMode());
        cfg.setBackups(backupsCount());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     *
     */
    protected void prepareAndCheck(
        Ignite initiator,
        Integer cnt,
        Consumer<T3<IgniteCache<Integer, Integer>, Map<Integer, T3<Map<Ignite, Integer>, Integer, Integer>>, Boolean>> c,
        boolean raw)
        throws Exception {
        IgniteCache<Integer, Integer> cache = initiator.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 20; i++) {
            Map<Integer /*key*/, T3<Map<Ignite, Integer>, Integer, Integer> /*result*/> results = new HashMap<>();

            for (int j = 0; j < cnt; j++) {
                T3<Map<Ignite, Integer>, Integer, Integer> res = setDifferentValuesForSameKey(++iterableKey);

                results.put(iterableKey, res);
            }

            for (Ignite node : G.allGrids()) {
                Map<Integer, Integer> all =
                    node.<Integer, Integer>getOrCreateCache(DEFAULT_CACHE_NAME).getAll(results.keySet());

                for (Map.Entry<Integer, Integer> entry : all.entrySet()) {
                    Integer exp = results.get(entry.getKey()).get1().get(node); // Reads from itself (backup or primary).

                    if (exp == null)
                        exp = results.get(entry.getKey()).get2(); // Reads from primary (not a partition owner).

                    assertEquals(exp, entry.getValue());
                }
            }

            c.accept(new T3<>(cache, results, raw)); // Code checks here.
        }
    }

    /**
     *
     */
    private T3<Map<Ignite, Integer> /*mapping*/, Integer /*primary*/, Integer /*latest*/> setDifferentValuesForSameKey(
        int key) throws Exception {
        List<Ignite> nodes = new ArrayList<>();
        Map<Ignite, Integer> mapping = new HashMap<>();

        boolean reverse = ThreadLocalRandom.current().nextBoolean();
        boolean random = ThreadLocalRandom.current().nextBoolean();

        Ignite primary = primaryNode(key, DEFAULT_CACHE_NAME);

        if (reverse) {
            nodes.addAll(backupNodes(key, DEFAULT_CACHE_NAME));
            nodes.add(primary);
        }
        else {
            nodes.add(primary);
            nodes.addAll(backupNodes(key, DEFAULT_CACHE_NAME));
        }

        if (random) {
            Map<Integer, Ignite> nodes0 = new HashMap<>();

            for (Ignite node : nodes) {
                while (true) {
                    int idx = ThreadLocalRandom.current().nextInt(nodes.size());

                    if (nodes0.get(idx) == null) {
                        nodes0.put(idx, node);

                        break;
                    }
                }
            }

            nodes = new ArrayList<>();

            for (int i = 0; i < nodes0.size(); i++)
                nodes.add(nodes0.get(i));
        }

        GridCacheVersionManager mgr = ((GridCacheAdapter)(grid(1)).cachex(DEFAULT_CACHE_NAME)
            .cache()).context().shared().versions();

        int val = 0;
        int primVal = -1;

        for (Ignite node : nodes) {
            IgniteInternalCache intCache =
                ((IgniteEx)node).cachex(DEFAULT_CACHE_NAME);

            GridCacheAdapter adapter = ((GridCacheAdapter)intCache.cache());

            GridCacheEntryEx entry = adapter.entryEx(key);

            boolean init = entry.initialValue(new CacheObjectImpl(++val, null), // Incremental value.
                mgr.next(), // Incremental version.
                0,
                0,
                false,
                AffinityTopologyVersion.NONE,
                GridDrType.DR_NONE,
                false);

            mapping.put(node, val);

            assertTrue("iterableKey " + key + " already inited", init);

            if (node.equals(primary))
                primVal = val;
        }

        assert primVal != -1;

        return new T3<>(mapping, primVal, val);
    }
}
