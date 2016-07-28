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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
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
public class IgniteCacheMergeSqlQuerySelfTest extends GridCommonAbstractTest {
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
        startGridsMultiThreaded(1, false);

        ignite(0).createCache(cacheConfig("S2P", true, false, String.class, Person.class));
        ignite(0).createCache(cacheConfig("I2P", true, false, Integer.class, Person.class));
        ignite(0).createCache(cacheConfig("K2P", true, false, Key.class, Person.class));
        ignite(0).createCache(cacheConfig("K22P", true, true, Key2.class, Person.class));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param escapeSql whether identifiers should be quoted - see {@link CacheConfiguration#setSqlEscapeAll}
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfig(String name, boolean partitioned, boolean escapeSql, Class<?>... idxTypes) {
        return new CacheConfiguration()
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setSqlEscapeAll(escapeSql)
            .setIndexedTypes(idxTypes);
    }

    /**
     *
     */
    public void testMergeWithExplicitKey() {
        IgniteCache<String, Person> p = ignite(0).cache("S2P");

        QueryCursor c = p.query(new SqlFieldsQuery("merge into Person (_key, id, name) values ('s', ?, ?), " +
            "('a', 2, 'Alex')").setArgs(1, "Sergi"));

        c.iterator();

        Person p1 = new Person(1);
        p1.name = "Sergi";

        assertEquals(p1, p.get("s"));

        Person p2 = new Person(2);
        p2.name = "Alex";

        assertEquals(p2, p.get("a"));
    }

    /**
     *
     */
    public void testMergeWithExplicitPrimitiveKey() {
        IgniteCache<Integer, Person> p = ignite(0).cache("I2P");

        QueryCursor c = p.query(new SqlFieldsQuery(
            "merge into Person (_key, id, name) values (1, ?, ?), (2, 2, 'Alex')").setArgs(1, "Sergi"));

        c.iterator();

        Person p1 = new Person(1);
        p1.name = "Sergi";

        assertEquals(p1, p.get(1));

        Person p2 = new Person(2);
        p2.name = "Alex";

        assertEquals(p2, p.get(2));
    }

    /**
     *
     */
    public void testMergeWithDynamicKeyInstantiation() {
        IgniteCache<Key, Person> p = ignite(0).cache("K2P");

        QueryCursor c = p.query(new SqlFieldsQuery(
            "merge into Person (key, id, name) values (1, ?, ?), (2, 2, 'Alex')").setArgs(1, "Sergi"));

        c.iterator();

        Person p1 = new Person(1);
        p1.name = "Sergi";

        assertEquals(p1, p.get(new Key(1)));

        Person p2 = new Person(2);
        p2.name = "Alex";

        assertEquals(p2, p.get(new Key(2)));
    }

    /**
     *
     */
    public void testFieldsCaseSensitivity() {
        IgniteCache<Key2, Person> p = ignite(0).cache("K22P");

        QueryCursor c = p.query(new SqlFieldsQuery("merge into \"Person\" (\"Id\", \"id\", \"name\") values (1, ?, ?), " +
            "(2, 3, 'Alex')").setArgs(4, "Sergi"));

        c.iterator();

        Person p1 = new Person(4);
        p1.name = "Sergi";

        assertEquals(p1, p.get(new Key2(1)));

        Person p2 = new Person(3);
        p2.name = "Alex";

        assertEquals(p2, p.get(new Key2(2)));
    }

    /**
     *
     */
    private final static class Key {
        /** */
        public Key(int key) {
            this.key = key;
        }

        /** */
        @QuerySqlField
        public final int key;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key1 = (Key) o;

            return key == key1.key;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }
    }

    /**
     *
     */
    private final static class Key2 {
        /** */
        public Key2(int Id) {
            this.Id = Id;
        }

        /** */
        @QuerySqlField
        public final int Id;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key2 key1 = (Key2) o;

            return Id == key1.Id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Id;
        }
    }

    /**
     *
     */
    private static class Person {
        /** */
        @SuppressWarnings("unused")
        private Person() {
            // No-op.
        }

        /** */
        public Person(int id) {
            this.id = id;
        }

        /** */
        @QuerySqlField
        protected int id;

        /** */
        @QuerySqlField
        protected String name;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            if (id != person.id) return false;
            return name != null ? name.equals(person.name) : person.name == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }
    }
}
