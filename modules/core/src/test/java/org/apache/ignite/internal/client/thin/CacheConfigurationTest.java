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

package org.apache.ignite.internal.client.thin;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.Comparers;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicy;
import org.junit.Test;

/**
 * Thin client cache configuration tests.
 */
public class CacheConfigurationTest extends AbstractThinClientTest {
    /**
     * Tested API:
     * <ul>
     * <li>{@link ClientCache#getName()}</li>
     * <li>{@link ClientCache#getConfiguration()}</li>
     * </ul>
     */
    @Test
    public void testCacheConfiguration() throws Exception {
        final String dataRegionName = "functional-test-data-region";

        IgniteConfiguration cfg = getConfiguration()
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setName(dataRegionName)));

        try (Ignite ignite = startGrid(cfg); IgniteClient client = startClient(ignite)) {
            final String CACHE_NAME = "testCacheConfiguration";

            ClientCacheConfiguration cacheCfgTemplate = new ClientCacheConfiguration().setName(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(3)
                .setCacheMode(CacheMode.PARTITIONED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setEagerTtl(false)
                .setGroupName("FunctionalTest")
                .setDefaultLockTimeout(12345)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
                .setReadFromBackup(true)
                .setRebalanceBatchSize(67890)
                .setRebalanceBatchesPrefetchCount(102938)
                .setRebalanceDelay(54321)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setRebalanceOrder(2)
                .setRebalanceThrottle(564738)
                .setRebalanceTimeout(142536)
                .setKeyConfiguration(new CacheKeyConfiguration("Employee", "orgId"))
                .setQueryEntities(new QueryEntity(int.class.getName(), "Employee")
                    .setTableName("EMPLOYEE")
                    .setFields(
                        Stream.of(
                            new AbstractMap.SimpleEntry<>("id", Integer.class.getName()),
                            new AbstractMap.SimpleEntry<>("orgId", Integer.class.getName())
                        ).collect(Collectors.toMap(
                            AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue, (a, b) -> a, LinkedHashMap::new
                        ))
                    )
                    // During query normalization null keyFields become empty set.
                    // Set empty collection for comparator.
                    .setKeyFields(Collections.emptySet())
                    .setKeyFieldName("id")
                    .setNotNullFields(Collections.singleton("id"))
                    .setDefaultFieldValues(Collections.singletonMap("id", 0))
                    .setIndexes(Collections.singletonList(new QueryIndex("id", true, "IDX_EMPLOYEE_ID")))
                    .setAliases(Stream.of("id", "orgId").collect(Collectors.toMap(f -> f, String::toUpperCase)))
                )
                .setExpiryPolicy(new PlatformExpiryPolicy(10, 20, 30))
                .setCopyOnRead(!CacheConfiguration.DFLT_COPY_ON_READ)
                .setDataRegionName(dataRegionName)
                .setMaxConcurrentAsyncOperations(4)
                .setMaxQueryIteratorsCount(4)
                .setOnheapCacheEnabled(true)
                .setQueryDetailMetricsSize(1024)
                .setQueryParallelism(4)
                .setSqlEscapeAll(true)
                .setSqlIndexMaxInlineSize(1024)
                .setSqlSchema("functional-test-schema")
                .setStatisticsEnabled(true);

            ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration(cacheCfgTemplate);

            ClientCache<Object, Object> cache = client.createCache(cacheCfg);

            assertEquals(CACHE_NAME, cache.getName());

            assertTrue(Comparers.equal(cacheCfgTemplate, cache.getConfiguration()));
        }
    }

    /** Tests cache partitions configuration. */
    @Test
    public void testCachePartitionsConfiguration() throws Exception {
        try (Ignite ignite = startGrid(0); IgniteClient client = startClient(ignite)) {
            // Explicit partitions count test.
            String cacheName = "test";

            // Client to server propagation.
            client.createCache(new ClientCacheConfiguration().setName(cacheName).setPartitions(100));

            assertEquals(100, ignite.cache(cacheName)
                .getConfiguration(CacheConfiguration.class).getAffinity().partitions());

            // Server to client propagation.
            assertEquals(100, client.cache(cacheName).getConfiguration().getPartitions());

            // Implicit partitions count test.
            cacheName = "test2";

            client.createCache(new ClientCacheConfiguration().setName(cacheName));

            assertEquals(ignite.cache(cacheName).getConfiguration(CacheConfiguration.class)
                .getAffinity().partitions(), client.cache(cacheName).getConfiguration().getPartitions());
        }
    }
}
