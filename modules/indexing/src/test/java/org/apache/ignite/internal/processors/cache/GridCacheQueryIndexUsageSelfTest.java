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

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static java.util.Calendar.*;

/**
 * Tests for cache query results serialization.
 */
public class GridCacheQueryIndexUsageSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 1;

    /** */
    private static final String CACHE_NAME = "A";

    /** */
    private static final CacheMode CACHE_MODE = PARTITIONED;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        MemoryPolicyConfiguration mpcfg = new MemoryPolicyConfiguration();
        //mpcfg.setMaxSize(2 * 1024 * 1024 * 1024L);
        mpcfg.setName("def");

        MemoryConfiguration mcfg = new MemoryConfiguration();
        mcfg.setDefaultMemoryPolicyName("def");
        mcfg.setMemoryPolicies(mpcfg);

        cfg.setMemoryConfiguration(mcfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(CACHE_MODE);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity qe = new QueryEntity();
        qe.setKeyType(Long.class.getName());
        qe.setValueType(IndexedValue.class.getName());

        LinkedHashMap<String, String> fields = U.newLinkedHashMap(3);

        fields.put("id", Long.class.getName());
        fields.put("startDate", Date.class.getName());
        qe.setFields(fields);

        QueryIndex idx1 = new QueryIndex();
        idx1.setIndexType(QueryIndexType.SORTED);
        LinkedHashMap<String, Boolean> idx1Fields = U.newLinkedHashMap(3);

        idx1Fields.put("startDate", Boolean.FALSE);

        idx1.setFields(idx1Fields);

     /*   QueryIndex idx2 = new QueryIndex();
        idx2.setIndexType(QueryIndexType.SORTED);
        LinkedHashMap<String, Boolean> idx2Fields = U.newLinkedHashMap(3);

        idx2Fields.put("startDate", Boolean.TRUE);

        idx2.setFields(idx2Fields);*/

        qe.setIndexes(Arrays.asList(idx1));

        cacheCfg.setQueryEntities(Collections.singleton(qe));

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** */
    public void testIndexUsageAscSort() {
        testIndexUsage0(true);
    }

    /** */
    public void testIndexUsageDescSort() {
        testIndexUsage0(false);
    }

    /** */
    private void testIndexUsage0(boolean asc) {
        Calendar start = Calendar.getInstance();

        start.set(2017, APRIL, 1, 0, 0, 0);

        IgniteCache<Object, Object> cache = ignite(0).cache(CACHE_NAME);

        int step = 50;

        for (long i = 0; i < step; i++) {
            Date date = start.getTime();

            for (long j = 0; j < step; j++) {
                IndexedValue v = new IndexedValue(i * step + j, date);

                cache.put(i * step + j, v);
            }

            start.add(Calendar.DAY_OF_MONTH, 1);
        }

        start.set(2017, APRIL, 10, 0, 0, 0);
        Date from = start.getTime();

        start.add(DAY_OF_MONTH, step * step / 2);

        int limit = 500;

        Date to = start.getTime();

        SqlFieldsQuery q = new SqlFieldsQuery("explain analyze select _KEY, _VAL, startdate " +
            "from IndexedValue where startDate >= ? and startDate < ? " +
            "order by startdate " + (asc ? "asc" : "desc") + " limit " + limit);
        q.setLocal(true);
        q.setArgs(from, to);

        List<List<?>> all = cache.query(q).getAll();

        String plan = all.toString();

        System.out.println("plan=" + plan);

        final String str = "scanCount: ";

        int off = plan.indexOf(str);

        assertTrue(off != -1);

        off += "scanCount:".length() + 1;
        int st = off;

        while(Character.isDigit(plan.charAt(off++)));

        int scanCnt = Integer.parseInt(plan.substring(st, off - 1));

        assertEquals(limit, scanCnt);
    }

    /** */
    public static class IndexedValue {
        /** */
        private Long id;

        /** */
        private Date startDate;

        /**
         * @param id Id.
         * @param startDate Start date.
         */
        public IndexedValue(Long id, Date startDate) {
            this.id = id;
            this.startDate = startDate;
        }

        /** */
        public Long getId() {
            return id;
        }

        /** */
        public void setId(Long id) {
            this.id = id;
        }

        /** */
        public Date getStartDate() {
            return startDate;
        }

        /** */
        public void setStartDate(Date startDate) {
            this.startDate = startDate;
        }

        @Override public String toString() {
            return "IndexedValue{" +
                "id=" + id +
                ", startDate=" + startDate +
                '}';
        }
    }
}