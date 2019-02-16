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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test type inference for SQL parameters.
 */
public class ParameterTypeInferenceTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setLocalHost("127.0.0.1");
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGrids(2);

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

    /**
     * Test type inference for local query.
     */
    @Test
    public void testInferenceLocal() {
        check("SELECT ? FROM cache ORDER BY val", true, new Object[] { null }, Object.class);
        //check("SELECT val, ? FROM cache", true, new Object[] { "test" }, String.class);
    }

    /**
     * Test type inference for query without reducer.
     */
    @Test
    public void testInferenceNoReduce() {

    }

    /**
     * Test type inference for query with reducer.
     */
    @Test
    public void testInferenceReduce() {

    }

    /**
     * Execute query.
     *
     * @param qry Query.
     * @param loc Local flag.
     * @param args Arugments.
     * @param expCls Expected class.
     */
    private void check(String qry, boolean loc, Object[] args, Class expCls) {
        QueryCursorEx<List<?>> cur =
            (QueryCursorEx<List<?>>)cache().query(new SqlFieldsQuery(qry).setLocal(loc).setArgs(args));

        GridQueryFieldMetadata field = cur.fieldsMeta().get(0);

        cur.getAll();

        assertEquals(expCls.getName(), field.fieldTypeName());

//        List<List<?>> rows = cache().query(new SqlFieldsQuery(qry).setLocal(loc).setArgs(args)).getAll();
//
//        assert !rows.isEmpty();
//
//        List<?> row = rows.get(0);
//
//        assert !row.isEmpty();
//
//        Object val = row.get(0);
//
//        if (expCls == null)
//            assert val == null;
//        else
//            assertEquals(expCls, val.getClass());
    }

    /**
     * @return Cache.
     */
    private IgniteCache<InferenceKey, InferenceValue> cache() {
        return grid(0).cache(CACHE_NAME);
    }

    /**
     * Key class.
     */
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
