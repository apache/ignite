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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteErrorOnRebalanceTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
            .setPageSize(1024)
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setRebalanceMode(ASYNC);
        ccfg.setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(ccfg);

        cfg.setIndexingSpi(new ErrorOnRebalanceIndexingSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9842")
    @Test
    public void testErrorOnRebalance() throws Exception {
        Ignite srv0 = startGrid(0);

        srv0.active(true);

        {
            IgniteCache<Object, Object> cache0 = srv0.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 5; i++)
                cache0.put(i, i);
        }

        Ignite srv1 = startGrid(1);

        U.sleep(3000);

        info("Stop node0.");

        stopGrid(0);

        awaitPartitionMapExchange();

        info("Restart node0.");

        srv0 = startGrid(0);

        awaitPartitionMapExchange();

        srv1.cluster().setBaselineTopology(srv1.cluster().topologyVersion());

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache0 = srv0.cache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cache1 = srv1.cache(DEFAULT_CACHE_NAME);

        Map<Object, Object> map0 = new HashMap<>();
        Map<Object, Object> map1 = new HashMap<>();

        for (int i = 0; i < 5; i++) {
            map0.put(i, cache0.localPeek(i));
            map1.put(i, cache1.localPeek(i));
        }

        assertEquals(map0, map1);
    }

    /**
     *
     */
    static class ErrorOnRebalanceIndexingSpi extends IgniteSpiAdapter implements IndexingSpi {
        /** */
        volatile boolean err = true;

        /**
         * @param node Node.
         * @return SPI.
         */
        static ErrorOnRebalanceIndexingSpi spi(Ignite node) {
            return (ErrorOnRebalanceIndexingSpi)node.configuration().getIndexingSpi();
        }

        /** {@inheritDoc} */
        @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String cacheName,
            Collection<Object> params, @Nullable IndexingQueryFilter filters) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void store(@Nullable String cacheName, Object key, Object val, long expirationTime) {
            if (err && ignite.name().endsWith("IgniteErrorOnRebalanceTest1")) {
                ignite.log().warning("Test error on store [cache=" + cacheName + ", key=" + key + ']');

                throw new IgniteSpiException("Test error");
            }
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String cacheName, Object key) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() {
            // No-op.
        }
    }
}
