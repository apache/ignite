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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.cache.Cache.Entry;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests consistency of entry's versions after invokeAll.
 */
public class EntryVersionConsistencyReadThroughTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 5;

    /**
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private CacheConfiguration<String, List<Double>> createCacheConfiguration(CacheAtomicityMode atomicityMode) {
        CacheConfiguration<String, List<Double>> cc = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(atomicityMode);
        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setReadThrough(true);
        cc.setWriteThrough(true);

        Factory cacheStoreFactory = new FactoryBuilder.SingletonFactory(new DummyCacheStore());

        cc.setCacheStoreFactory(cacheStoreFactory);

        cc.setBackups(2);

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES_CNT - 1);

        startClientGrid(NODES_CNT - 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAllTransactionalCache() throws Exception {
        check(false, createCacheConfiguration(TRANSACTIONAL));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAllMvccTxCache() throws Exception {
        Assume.assumeTrue("https://issues.apache.org/jira/browse/IGNITE-8582",
            MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.CACHE_STORE));

        check(false, createCacheConfiguration(TRANSACTIONAL_SNAPSHOT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAllAtomicCache() throws Exception {
        check(false, createCacheConfiguration(ATOMIC));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAtomicCache() throws Exception {
        check(true, createCacheConfiguration(ATOMIC));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeTransactionalCache() throws Exception {
        check(true, createCacheConfiguration(TRANSACTIONAL));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeMvccTxCache() throws Exception {
        Assume.assumeTrue("https://issues.apache.org/jira/browse/IGNITE-8582",
            MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.CACHE_STORE));

        check(true, createCacheConfiguration(TRANSACTIONAL_SNAPSHOT));
    }

    /**
     * Tests entry's versions consistency after invokeAll.
     *
     * @param single Single invoke or invokeAll.
     * @param cc Cache configuration.
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    private void check(boolean single, CacheConfiguration cc) throws Exception {
        grid(0).getOrCreateCache(cc);

        try {
            final int cnt = 100;

            for (int i = 0; i < NODES_CNT; i++) {
                final int iter = i;

                final Set<String> keys = new LinkedHashSet<String>() {{
                    for (int i = 0; i < cnt; i++)
                        add("key-" + iter + "-" + i);
                }};

                IgniteEx grid = grid(i);

                final IgniteCache<String, Integer> cache = grid.cache(DEFAULT_CACHE_NAME);

                if (single)
                    for (String key : keys)
                        cache.invoke(key, new DummyEntryProcessor());
                else
                    cache.invokeAll(keys, new DummyEntryProcessor());

                // Check entry versions consistency.
                for (String key : keys) {
                    Collection<ClusterNode> nodes = grid.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key);

                    List<IgniteEx> grids = grids(nodes);

                    GridCacheVersion ver0 = null;
                    Object val0 = null;

                    for (IgniteEx g : grids) {
                        GridCacheAdapter<Object, Object> cx = g.context().cache().internalCache(DEFAULT_CACHE_NAME);

                        GridCacheEntryEx e = cx.entryEx(key);

                        e.unswap();

                        assertNotNull("Failed to find entry on primary/backup node.", e.rawGet());

                        GridCacheVersion ver = e.version();
                        Object val = e.rawGet().value(cx.context().cacheObjectContext(), true);

                        if (ver0 == null) {
                            ver0 = ver;
                            val0 = val;
                        }

                        assertEquals("Invalid version for key: " + key, ver0, ver);

                        assertNotNull("No value for key: " + key, val);
                        assertEquals("Invalid value for key: " + key, val0, val);
                    }
                }
            }
        }
        finally {
            grid(0).destroyCache(DEFAULT_CACHE_NAME);
        }
    }

    /**
     * @param nodes Nodes.
     * @return Grids.
     */
    private List<IgniteEx> grids(Collection<ClusterNode> nodes) {
        List<IgniteEx> grids = new ArrayList<>();

        for (ClusterNode node : nodes) {
            for (int i = 0; i < NODES_CNT; i++) {
                if (grid(i).cluster().localNode().id().equals(node.id())) {
                    grids.add(grid(i));

                    break;
                }
            }
        }

        return grids;
    }

    /**
     *
     */
    private static class DummyEntryProcessor implements EntryProcessor<String, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<String, Integer> entry, Object... arguments) {
            Integer currVal = entry.getValue();

            if (currVal == null)
                entry.setValue(0);
            else
                entry.setValue(currVal + 1);

            return null;
        }
    }

    /**
     *
     */
    @SuppressWarnings("serial")
    private static class DummyCacheStore extends CacheStoreAdapter<String, Integer> implements Serializable {
        /** {@inheritDoc} */
        @Override public Integer load(String key) throws CacheLoaderException {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public void write(Entry<? extends String, ? extends Integer> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }
}
