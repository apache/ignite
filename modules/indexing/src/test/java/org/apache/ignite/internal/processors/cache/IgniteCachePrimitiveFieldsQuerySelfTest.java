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
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.LinkedHashMap;
import java.util.List;

/**
 *
 */
public class IgniteCachePrimitiveFieldsQuerySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME));

        // Force BinaryMarshaller.
        cfg.setMarshaller(null);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, IndexedType> cacheConfiguration(String cacheName) {
        CacheConfiguration<Integer, IndexedType> ccfg = new CacheConfiguration<>(cacheName);

        QueryEntity entity = new QueryEntity(Integer.class.getName(), IndexedType.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        // Test will pass if we use java.lang.Integer instead of int.
        fields.put("iVal", "int");

        entity.setFields(fields);

        entity.setIndexes(F.asList(
            new QueryIndex("iVal")
        ));

        ccfg.setQueryEntities(F.asList(entity));

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testStaticCache() throws Exception {
        checkCache(ignite(0).<Integer, IndexedType>cache(CACHE_NAME));
    }

    /**
     * @throws Exception if failed.
     */
    private void checkCache(IgniteCache<Integer, IndexedType> cache) throws Exception {
        for (int i = 0; i < 1000; i++)
            cache.put(i, new IndexedType(i));

        List<List<?>> res = cache.query(new SqlFieldsQuery("select avg(iVal) from IndexedType where iVal > ?")
            .setArgs(499)).getAll();

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
    }

    /**
     * 
     */
    @SuppressWarnings("unused")
    private static class IndexedType {
        /** */
        private int iVal;

        /**
         * @param iVal Value.
         */
        private IndexedType(int iVal) {
            this.iVal = iVal;
        }
    }
}
