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
import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
@SuppressWarnings("unchecked")
public abstract class IgniteCacheAbstractSqlDmlQuerySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3, true);

        ignite(0).createCache(cacheConfig());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        ignite(0).cache("S2P").put("FirstKey", createPerson(1, "John", "White"));
        ignite(0).cache("S2P").put("SecondKey", createPerson(2, "Joe", "Black"));
        ignite(0).cache("S2P").put("k3", createPerson(3, "Sylvia", "Green"));
        ignite(0).cache("S2P").put("f0u4thk3y", createPerson(4, "Jane", "Silver"));
    }

    /** */
    protected Object createPerson(int id, String name, String secondName) {
        return new Person(id, name, secondName);
    }

    /** */
    protected IgniteCache<?, ?> cache() {
        return ignite(0).cache("S2P");
    }

    /** */
    protected CacheConfiguration cacheConfig() {
        return cacheConfig("S2P", true, false).setIndexedTypes(String.class, Person.class);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param escapeSql whether identifiers should be quoted - see {@link CacheConfiguration#setSqlEscapeAll}
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfig(String name, boolean partitioned, boolean escapeSql) {
        return new CacheConfiguration()
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setSqlEscapeAll(escapeSql);
    }

    /**
     *
     */
    static CacheConfiguration createBinCacheConfig() {
        CacheConfiguration ccfg = cacheConfig("S2P", true, false);

        QueryEntity e = new QueryEntity(String.class.getName(), "Person");

        e.setKeyFields(Collections.<String>emptySet());

        LinkedHashMap<String, String> flds = new LinkedHashMap<>();

        flds.put("id", Integer.class.getName());
        flds.put("name", String.class.getName());
        flds.put("secondName", String.class.getName());

        e.setFields(flds);

        e.setIndexes(Collections.<QueryIndex>emptyList());

        ccfg.setQueryEntities(Collections.singletonList(e));

        return ccfg;
    }

    /**
     *
     */
    BinaryObject createPersonBinary(int id, String name, String secondName) {
        BinaryObjectBuilder bldr = ignite(0).binary().builder("Person");

        bldr.setField("id", id);
        bldr.setField("name", name);
        bldr.setField("secondName", secondName);

        return bldr.build();
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        public Person(int id, String name, String secondName) {
            this.id = id;
            this.name = name;
            this.secondName = secondName;
        }

        /** */
        @QuerySqlField
        protected int id;

        /** */
        @QuerySqlField
        protected final String name;

        /** */
        @QuerySqlField
        final String secondName;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            return id == person.id && name.equals(person.name) && secondName.equals(person.secondName);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = id;
            res = 31 * res + name.hashCode();
            res = 31 * res + secondName.hashCode();
            return res;
        }
    }
}
