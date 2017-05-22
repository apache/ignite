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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheGroupsSqlTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String GROUP1 = "grp1";

    /** */
    private static final String GROUP2 = "grp2";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQuery() throws Exception {
        Ignite node = ignite(0);

        IgniteCache c1 = node.createCache(cacheConfiguration(GROUP1, "c1"));
        IgniteCache c2 = node.createCache(cacheConfiguration(GROUP1, "c2"));

        SqlFieldsQuery qry = new SqlFieldsQuery("select name from Person where name=?");
        qry.setArgs("p1");

        assertEquals(0, c1.query(qry).getAll().size());
        assertEquals(0, c2.query(qry).getAll().size());

        c1.put(1, new Person("p1"));

        assertEquals(1, c1.query(qry).getAll().size());
        assertEquals(0, c2.query(qry).getAll().size());

        c2.put(2, new Person("p1"));

        assertEquals(1, c1.query(qry).getAll().size());
        assertEquals(1, c2.query(qry).getAll().size());
    }

    /**
     * @param grpName Group name.
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String grpName, String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setGroupName(grpName);
        ccfg.setName(cacheName);

        QueryEntity entity = new QueryEntity();
        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());
        entity.addQueryField("name", String.class.getName(), null);

        ccfg.setQueryEntities(F.asList(entity));

        return ccfg;
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        String name;

        /**
         * @param name Name.
         */
        public Person(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
