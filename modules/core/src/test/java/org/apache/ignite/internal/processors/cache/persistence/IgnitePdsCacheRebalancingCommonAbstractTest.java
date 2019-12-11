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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base prmitives for testing persistence rebalancing.
 */
public abstract class IgnitePdsCacheRebalancingCommonAbstractTest extends GridCommonAbstractTest {
    /** Default cache. */
    protected static final String CACHE = "cache";

    /** Cache with node filter. */
    protected static final String FILTERED_CACHE = "filtered";

    /** Cache with enabled indexes. */
    protected static final String INDEXED_CACHE = "indexed";

    /** */
    protected boolean explicitTx;

    /** Set to enable filtered cache on topology. */
    protected boolean filteredCacheEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setRebalanceThreadPoolSize(2);

        CacheConfiguration ccfg1 = cacheConfiguration(CACHE)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
            .setBackups(2)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setIndexedTypes(Integer.class, Integer.class)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setRebalanceBatchesPrefetchCount(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        CacheConfiguration ccfg2 = cacheConfiguration(INDEXED_CACHE)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        QueryEntity qryEntity = new QueryEntity(Integer.class, TestValue.class);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("v1", Integer.class.getName());
        fields.put("v2", Integer.class.getName());

        qryEntity.setFields(fields);

        QueryIndex qryIdx = new QueryIndex("v1", true);

        qryEntity.setIndexes(Collections.singleton(qryIdx));

        ccfg2.setQueryEntities(Collections.singleton(qryEntity));

        List<CacheConfiguration> cacheCfgs = new ArrayList<>();
        cacheCfgs.add(ccfg1);
        cacheCfgs.add(ccfg2);

        if (filteredCacheEnabled && !gridName.endsWith("0")) {
            CacheConfiguration ccfg3 = cacheConfiguration(FILTERED_CACHE)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE)
                .setBackups(2)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setNodeFilter(new CoordinatorNodeFilter());

            cacheCfgs.add(ccfg3);
        }

        cfg.setCacheConfiguration(asArray(cacheCfgs));

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
            .setCheckpointFrequency(checkpointFrequency())
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(1024)
            .setWalSegmentSize(8 * 1024 * 1024) // For faster node restarts with enabled persistence.
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setName("dfltDataRegion")
                .setPersistenceEnabled(true)
                .setMaxSize(512 * 1024 * 1024)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * @param cacheCfgs Cache cfgs.
     */
    private static CacheConfiguration[] asArray(List<CacheConfiguration> cacheCfgs) {
        CacheConfiguration[] res = new CacheConfiguration[cacheCfgs.size()];
        for (int i = 0; i < res.length; i++)
            res[i] = cacheCfgs.get(i);

        return res;
    }

    /**
     * @return Checkpoint frequency;
     */
    protected long checkpointFrequency() {
        return DataStorageConfiguration.DFLT_CHECKPOINT_FREQ;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return 60 * 1000;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration(String cacheName);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    static class TestValue implements Serializable {
        /** Operation order. */
        final long order;

        /** V 1. */
        final int v1;

        /** V 2. */
        final int v2;

        /** Flag indicates that value has removed. */
        final boolean removed;

        TestValue(long order, int v1, int v2) {
            this(order, v1, v2, false);
        }

        TestValue(long order, int v1, int v2, boolean removed) {
            this.order = order;
            this.v1 = v1;
            this.v2 = v2;
            this.removed = removed;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            TestValue testValue = (TestValue) o;

            return order == testValue.order &&
                v1 == testValue.v1 &&
                v2 == testValue.v2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(order, v1, v2);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestValue{" +
                "order=" + order +
                ", v1=" + v1 +
                ", v2=" + v2 +
                '}';
        }
    }

    /**
     *
     */
    private static class CoordinatorNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            // Do not start cache on coordinator.
            return !node.<String>attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME).endsWith("0");
        }
    }
}
