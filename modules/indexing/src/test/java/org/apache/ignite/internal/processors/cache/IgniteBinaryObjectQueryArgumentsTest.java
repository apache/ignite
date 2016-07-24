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

import java.util.Collections;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteBinaryObjectQueryArgumentsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 3;

    /** */
    public static final String PRIM_CACHE = "prim-cache";

    /** */
    public static final String STR_CACHE = "str-cache";

    /** */
    public static final String ENUM_CACHE = "enum-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheConfiguration ccfg = getCacheConfiguration();

        CacheConfiguration primCcfg = getPrimitiveCacheConfiguration();

        CacheConfiguration strCcfg = getStringCacheConfiguration();

        CacheConfiguration enumCCfg = getEnumCacheConfiguration();

        cfg.setCacheConfiguration(ccfg, primCcfg, strCcfg, enumCCfg);

        cfg.setMarshaller(null);

        return cfg;
    }

    /**
     * @return Cache config.
     */
    private CacheConfiguration getStringCacheConfiguration() {
        CacheConfiguration strCcfg = new CacheConfiguration();

        strCcfg.setName(STR_CACHE);

        QueryEntity strPerson = new QueryEntity();
        strPerson.setKeyType(String.class.getName());
        strPerson.setValueType(String.class.getName());

        strCcfg.setQueryEntities(Collections.singletonList(strPerson));

        return strCcfg;
    }

    /**
     * @return Cache config.
     */
    private CacheConfiguration getPrimitiveCacheConfiguration() {
        CacheConfiguration primCcfg = new CacheConfiguration();

        primCcfg.setName(PRIM_CACHE);

        QueryEntity primPerson = new QueryEntity();
        primPerson.setKeyType(Integer.class.getName());
        primPerson.setValueType(String.class.getName());

        primCcfg.setQueryEntities(Collections.singletonList(primPerson));

        return primCcfg;
    }

    /**
     * @return Cache config.
     */
    private CacheConfiguration getCacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity person = new QueryEntity();
        person.setKeyType(TestKey.class.getName());
        person.setValueType(Person.class.getName());
        person.addQueryField("name", String.class.getName(), null);

        ccfg.setQueryEntities(Collections.singletonList(person));

        return ccfg;
    }

    /**
     * @return Cache config.
     */
    private CacheConfiguration getEnumCacheConfiguration() {
        CacheConfiguration strCcfg = new CacheConfiguration();

        strCcfg.setName(ENUM_CACHE);

        QueryEntity enumPerson = new QueryEntity();
        enumPerson.setKeyType(EnumKey.class.getName());
        enumPerson.setValueType(String.class.getName());

        strCcfg.setQueryEntities(Collections.singletonList(enumPerson));

        return strCcfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectArgument() throws Exception {
        IgniteCache<TestKey, Person> cache = ignite(0).cache(null);

        for (int i = 0; i < 100; i++)
            cache.put(new TestKey(i), new Person("name-" + i));

        final SqlQuery<TestKey, Person> qry = new SqlQuery<>(Person.class, "where _key=?");

        final SqlFieldsQuery fieldsQry = new SqlFieldsQuery("select * from Person where _key=?");

        for (int i = 0; i < 100; i++) {
            Object key = new TestKey(i);

            qry.setArgs(key);
            fieldsQry.setArgs(key);

            List<Cache.Entry<TestKey, Person>> res = cache.query(qry).getAll();

            final List<List<?>> fieldsRes = cache.query(fieldsQry).getAll();

            assertEquals(1, res.size());
            assertEquals(1, fieldsRes.size());

            Person p = res.get(0).getValue();

            assertEquals("name-" + i, p.name);

            assertEquals(3, fieldsRes.get(0).size());
            assertEquals(key, fieldsRes.get(0).get(0));
            assertEquals(p, fieldsRes.get(0).get(1));

            assertEquals(p.name, fieldsRes.get(0).get(2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimitiveObjectArgument() throws Exception {
        IgniteCache<Integer, String> cache = ignite(0).cache(PRIM_CACHE);

        for (int i = 0; i < 100; i++)
            cache.put(i, "name-" + i);

        SqlQuery<Integer, String> primQry = new SqlQuery<>(String.class, "where _key=?");

        for (int i = 0; i < 100; i++) {
            primQry.setArgs(i);

            final List<Cache.Entry<Integer, String>> primRes = cache.query(primQry).getAll();

            assertEquals(1, primRes.size());

            assertEquals("name-" + i, primRes.get(0).getValue());
            assertEquals(i, primRes.get(0).getKey().intValue());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringObjectArgument() throws Exception {
        IgniteCache<String, String> cache = ignite(0).cache(STR_CACHE);

        for (int i = 0; i < 100; i++)
            cache.put(String.valueOf(i), "name-" + i);

        SqlQuery<String, String> qry = new SqlQuery<>(String.class, "where _key=?");

        for (int i = 0; i < 100; i++) {
            qry.setArgs(String.valueOf(i));

            final List<Cache.Entry<String, String>> primRes = cache.query(qry).getAll();

            assertEquals(1, primRes.size());

            assertEquals("name-" + i, primRes.get(0).getValue());
            assertEquals(String.valueOf(i), primRes.get(0).getKey());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEnumObjectArgument() throws Exception {
        IgniteCache<EnumKey, String> cache = ignite(0).cache(ENUM_CACHE);

        cache.put(EnumKey.KEY1, EnumKey.KEY1.name());
        cache.put(EnumKey.KEY2, EnumKey.KEY2.name());

        SqlQuery<String, String> qry = new SqlQuery<>(String.class, "where _key=?");

        qry.setArgs(EnumKey.KEY1);

        final List<Cache.Entry<String, String>> res = cache.query(qry).getAll();

        assertEquals(1, res.size());

        assertEquals(EnumKey.KEY1.name(), res.get(0).getValue());
    }


    /**
     *
     */
    private static class Person {
        /** */
        String name;

        /**
         * @param name Name.
         */
        public Person(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final Person person = (Person) o;

            return name != null ? name.equals(person.name) : person.name == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }

    /**
     *
     */
    public static class TestKey {
        /** */
        private int id;

        /**
         * @param id Key.
         */
        public TestKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey other = (TestKey)o;

            return id == other.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    private enum EnumKey {
        /** */
        KEY1,

        /** */
        KEY2
    }
}
