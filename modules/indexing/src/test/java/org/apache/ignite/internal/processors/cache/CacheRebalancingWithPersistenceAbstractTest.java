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
 *
 */

package org.apache.ignite.internal.processors.cache;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DatabaseConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public abstract class CacheRebalancingWithPersistenceAbstractTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(null));

        DatabaseConfiguration dbCfg = new DatabaseConfiguration();

        dbCfg.setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);
        dbCfg.setPageSize(1024);
        dbCfg.setPageCacheSize(10 * 1024 * 1024);
        dbCfg.setFileCacheAllocationPath("test-db");

        cfg.setDatabaseConfiguration(dbCfg);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration(String cacheName);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        G.stopAll(true);

        U.delete(new File(U.getIgniteHome(), "test-db"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(true);

        U.delete(new File(U.getIgniteHome(), "test-db"));
    }

    public void testRebalancingOnRestart() throws Exception {
        IgniteEx ignite1 = (IgniteEx)G.start(getConfiguration("test1"));
        IgniteEx ignite2 = (IgniteEx)G.start(getConfiguration("test2"));
        IgniteEx ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        IgniteEx ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        awaitPartitionMapExchange();

        IgniteCache cache1 = ignite1.cache(null);

        for (int i = 0; i < 10000; i++) {
            cache1.put(i, i);
        }

        ignite3.close();
        ignite4.close();

        awaitPartitionMapExchange();

        assert !cache1.lostPartitions().isEmpty();

        cache1.recoverPartitions(cache1.lostPartitions()).get();

        assert cache1.lostPartitions().isEmpty();

        for (int i = 0; i < 10000; i++) {
            cache1.put(i, i * 2);
        }

        ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        awaitPartitionMapExchange();

        IgniteCache cache3 = ignite3.cache(null);
        IgniteCache cache4 = ignite4.cache(null);

        for (int i = 0; i < 10000; i++)
            assert cache3.get(i).equals(i * 2) && cache4.get(i).equals(i * 2);
    }

    public void testNoRebalancingOnRestartDeactivated() throws Exception {
        IgniteEx ignite1 = (IgniteEx)G.start(getConfiguration("test1"));
        IgniteEx ignite2 = (IgniteEx)G.start(getConfiguration("test2"));
        IgniteEx ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        IgniteEx ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        awaitPartitionMapExchange();

        IgniteCache cache1 = ignite1.cache(null);

        for (int i = 0; i < 10000; i++) {
            cache1.put(i, i);
        }

        cache1.active(false).get();

        ignite1.close();
        ignite2.close();
        ignite3.close();
        ignite4.close();

        ignite1 = (IgniteEx)G.start(getConfiguration("test1"));
        ignite2 = (IgniteEx)G.start(getConfiguration("test2"));
        ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        awaitPartitionMapExchange();

        cache1 = ignite1.cache(null);
        IgniteCache cache2 = ignite2.cache(null);
        IgniteCache cache3 = ignite3.cache(null);
        IgniteCache cache4 = ignite4.cache(null);

        for (int i = 0; i < 10000; i++) {
            assert cache1.get(i).equals(i);
            assert cache2.get(i).equals(i);
            assert cache3.get(i).equals(i);
            assert cache4.get(i).equals(i);
        }
    }

    public void testContentsCorrectnessAfterRestart() throws Exception {
        IgniteEx ignite1 = (IgniteEx)G.start(getConfiguration("test1"));
        IgniteEx ignite2 = (IgniteEx)G.start(getConfiguration("test2"));
        IgniteEx ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        IgniteEx ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        awaitPartitionMapExchange();

        IgniteCache cache1 = ignite1.cache(null);

        for (int i = 0; i < 10000; i++) {
            cache1.put(i, i);
        }

        ignite1.close();
        ignite2.close();
        ignite3.close();
        ignite4.close();

        ignite1 = (IgniteEx)G.start(getConfiguration("test1"));
        ignite2 = (IgniteEx)G.start(getConfiguration("test2"));
        ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        awaitPartitionMapExchange();

        cache1 = ignite1.cache(null);
        IgniteCache cache2 = ignite2.cache(null);
        IgniteCache cache3 = ignite3.cache(null);
        IgniteCache cache4 = ignite4.cache(null);

        for (int i = 0; i < 10000; i++) {
            assert cache1.get(i).equals(i);
            assert cache2.get(i).equals(i);
            assert cache3.get(i).equals(i);
            assert cache4.get(i).equals(i);
        }
    }

    public void testPartitionLossAndRecover() throws Exception {
        IgniteEx ignite1 = (IgniteEx)G.start(getConfiguration("test1"));
        IgniteEx ignite2 = (IgniteEx)G.start(getConfiguration("test2"));
        IgniteEx ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        IgniteEx ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        awaitPartitionMapExchange();

        IgniteCache cache1 = ignite1.cache(null);

        for (int i = 0; i < 10000; i++) {
            cache1.put(i, i);
        }

        ignite3.close();
        ignite4.close();

        awaitPartitionMapExchange();

        assert !cache1.lostPartitions().isEmpty();

        ignite3 = (IgniteEx)G.start(getConfiguration("test3"));
        ignite4 = (IgniteEx)G.start(getConfiguration("test4"));

        awaitPartitionMapExchange();

        cache1.recoverPartitions(cache1.lostPartitions());

        IgniteCache cache2 = ignite2.cache(null);
        IgniteCache cache3 = ignite3.cache(null);
        IgniteCache cache4 = ignite4.cache(null);

        for (int i = 0; i < 10000; i++) {
            assert cache1.get(i).equals(i);
            assert cache2.get(i).equals(i);
            assert cache3.get(i).equals(i);
            assert cache4.get(i).equals(i);
        }
    }

}
