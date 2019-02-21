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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test type inference for SQL parameters.
 */
public class ParameterTypeInferenceTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache. */
    private static final String CACHE_NAME = "cache";

    /** Number of nodes. */
    private static final int NODE_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setLocalHost("127.0.0.1");
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGrids(NODE_CNT);

        QueryEntity qryEntity = new QueryEntity(InferenceKey.class, InferenceValue.class).setTableName("cache");

        IgniteCache<InferenceKey, InferenceValue> cache = node.createCache(
            new CacheConfiguration<InferenceKey, InferenceValue>()
                .setName(CACHE_NAME)
                .setQueryEntities(Collections.singletonList(qryEntity))
        );

        for (int i = 0; i < 10; i++)
            cache.put(new InferenceKey(i), new InferenceValue(i));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        clearParserCache();
    }

    /**
     * Test type inference for local query.
     */
    @Test
    public void testInferenceLocal() {
        check("SELECT ? FROM cache", true);
        check("SELECT ? FROM cache ORDER BY val", true);
    }

    /**
     * Test type inference for query without reducer.
     */
    @Test
    public void testInferenceNoReduce() {
        check("SELECT ? FROM cache", false);
    }

    /**
     * Test type inference for query with reducer.
     */
    @Test
    public void testInferenceReduce() {
        check("SELECT ? FROM cache ORDER BY val", false);
    }

    /**
     * Execute query.
     *
     * @param qry Query.
     * @param loc Local flag.
     */
    private void check(String qry, boolean loc) {
        List<Object[]> argss = new ArrayList<>();

        argss.add(new Object[] { null });
        argss.add(new Object[] { "STRING" });
        argss.add(new Object[] { 1 });
        argss.add(new Object[] { 1L });
        argss.add(new Object[] { new BigDecimal("12.12") });
        argss.add(new Object[] { UUID.randomUUID() });

        clearParserCache();

        for (int i = 0; i < argss.size(); i++) {
            for (Object[] args : argss) {
                for (int j = 0; j < 2; j++) {
                    SqlFieldsQuery qry0 = new SqlFieldsQuery(qry).setLocal(loc).setArgs(args);

                    try (QueryCursorEx<List<?>> cur = (QueryCursorEx<List<?>>)grid(0).cache(CACHE_NAME).query(qry0)) {
                        GridQueryFieldMetadata meta = cur.fieldsMeta().get(0);

                        cur.getAll();

                        String errMsg = "Failure on i=" + i + ", j=" + j + ": " + meta.fieldTypeName();

                        assertEquals(errMsg, Object.class.getName(), meta.fieldTypeName());
                    }
                }
            }

            argss.add(argss.remove(0));
        }
    }

    /**
     * Clear parser cache.
     */
    private void clearParserCache() {
        for (int i = 0; i < NODE_CNT; i++)
            ((IgniteH2Indexing)grid(i).context().query().getIndexing()).parser().clearCache();
    }


    /**
     * Key class.
     */
    @SuppressWarnings("unused")
    private static class InferenceKey {
        /** Key. */
        @QuerySqlField
        private int key;

        /**
         * @param key Key.
         */
        private InferenceKey(int key) {
            this.key = key;
        }
    }

    /**
     * Value class.
     */
    @SuppressWarnings("unused")
    private static class InferenceValue {
        /** Value. */
        @QuerySqlField
        private int val;

        /**
         * @param val Value.
         */
        private InferenceValue(int val) {
            this.val = val;
        }
    }
}
