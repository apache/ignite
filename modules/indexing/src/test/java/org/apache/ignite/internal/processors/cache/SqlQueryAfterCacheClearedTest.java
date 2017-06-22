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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;

import static java.util.Collections.singletonList;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class SqlQueryAfterCacheClearedTest extends GridCommonAbstractTest {
    /** */
    public static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "propertyCache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        return new IgniteConfiguration()
            .setGridName(gridName)
            .setPeerClassLoadingEnabled(false)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setCacheConfiguration(cacheCfg());
    }

    /** */
    private static CacheConfiguration cacheCfg() {
        return new CacheConfiguration(CACHE_NAME)
        .setCacheMode(PARTITIONED)
        .setMemoryMode(OFFHEAP_TIERED)
        .setQueryEntities(singletonList(createQueryEntityConfig()));
    }

    /** */
    public void testQueryCacheWasCleared() throws InterruptedException {
        IgniteCache<PropertyAffinityKey, Property> cache = grid(0).cache(CACHE_NAME);

        Property prop1 = new Property(1, 2);
        Property prop2 = new Property(2, 2);

        cache.put(prop1.getKey(), prop1);
        cache.put(prop2.getKey(), prop2);

        assertEquals(cache.size(),2);
        assertEquals(cache.query(selectAllQuery()).getAll().size(), 2);

        cache.clear();

        assertEquals(0, cache.size());
        assertEquals(0, cache.query(selectAllQuery()).getAll().size());
    }

    /** */
    public void testQueryEntriesWereRemoved() {
        IgniteCache<PropertyAffinityKey, Property> cache = grid(0).cache(CACHE_NAME);

        Property prop1 = new Property(1, 2);
        Property prop2 = new Property(2, 2);

        cache.put(prop1.getKey(), prop1);
        cache.put(prop2.getKey(), prop2);

        assertEquals(cache.size(),2);
        assertEquals(cache.query(selectAllQuery()).getAll().size(), 2);

        cache.remove(new PropertyAffinityKey(1, 2));
        cache.remove(new PropertyAffinityKey(2, 2));

        assertEquals(0, cache.size());
        assertEquals(0, cache.query(selectAllQuery()).getAll().size());
    }

    /** */
    @NotNull private SqlQuery<PropertyAffinityKey, Property> selectAllQuery() {
        return new SqlQuery<>(Property.class, "from Property");
    }

    /** */
    private static QueryEntity createQueryEntityConfig() {
        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setKeyType(PropertyAffinityKey.class.getName());
        qryEntity.setValueType(Property.class.getName());
        qryEntity.setFields(getMapOfFields());
        return qryEntity;
    }

    /** */
    @NotNull private static LinkedHashMap<String, String> getMapOfFields() {
        LinkedHashMap<String, String> mapOfFields = new LinkedHashMap<>();
        mapOfFields.put("id", Integer.class.getName());
        mapOfFields.put("region", Integer.class.getName());
        mapOfFields.put("key", PropertyAffinityKey.class.getName());
        return mapOfFields;
    }

    /**
     *
     */
    private static class Property {
        /** Id. */
        private final int id;

        /** Region. */
        private final int region;

        /** */
        Property(int id, int region) {
            this.id = id;
            this.region = region;
        }

        /** */
        public PropertyAffinityKey getKey() {
            return new PropertyAffinityKey(id, region);
        }

        /** */
        public int getId() {
            return id;
        }
    }

    /**
     *
     */
    private static class PropertyAffinityKey extends AffinityKey<Integer> {
        /** */
        PropertyAffinityKey(final int thirdPartyPropId, final int region) {
            super(thirdPartyPropId, region);
        }

        /** */
        public PropertyAffinityKey() {
        }
    }
}
