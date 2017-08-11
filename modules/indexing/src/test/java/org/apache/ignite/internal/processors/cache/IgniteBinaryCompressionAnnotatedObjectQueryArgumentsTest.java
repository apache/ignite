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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.compression.BinaryCompression;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

public class IgniteBinaryCompressionAnnotatedObjectQueryArgumentsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 3;

    /** */
    private static final String PRIM_CACHE = "prim-cache";

    /** */
    private static final String STR_CACHE = "str-cache";

    /** */
    private static final String ENUM_CACHE = "enum-cache";

    /** */
    private static final String UUID_CACHE = "uuid-cache";

    /** */
    private static final String DATE_CACHE = "date-cache";

    /** */
    private static final String TIMESTAMP_CACHE = "timestamp-cache";

    /** */
    private static final String BIG_DECIMAL_CACHE = "decimal-cache";

    /** */
    private static final String OBJECT_CACHE = "obj-cache";

    /** */
    private static final String FIELD_CACHE = "field-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(getCacheConfigurations());

        cfg.setMarshaller(null);

        return cfg;
    }

    /**
     * @return {@code True} If query is local.
     */
    protected boolean isLocal() {
        return false;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache config.
     */
    protected CacheConfiguration getCacheConfiguration(final String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        QueryEntity person = new QueryEntity();
        person.setKeyType(TestKey.class.getName());
        person.setValueType(Person.class.getName());
        person.addQueryField("name", String.class.getName(), null);

        ccfg.setQueryEntities(Collections.singletonList(person));

        ccfg.setName(cacheName);

        return ccfg;
    }

    /**
     * @return Cache configurations.
     */
    private CacheConfiguration[] getCacheConfigurations() {
        final ArrayList<CacheConfiguration> ccfgs = new ArrayList<>();

        ccfgs.add(getCacheConfiguration(OBJECT_CACHE));
        ccfgs.addAll(getCacheConfigurations(STR_CACHE, String.class, Person.class));
        ccfgs.addAll(getCacheConfigurations(PRIM_CACHE, Integer.class, Person.class));
        ccfgs.addAll(getCacheConfigurations(ENUM_CACHE, EnumKey.class, Person.class));
        ccfgs.addAll(getCacheConfigurations(UUID_CACHE, UUID.class, Person.class));
        ccfgs.addAll(getCacheConfigurations(DATE_CACHE, Date.class, Person.class));
        ccfgs.addAll(getCacheConfigurations(TIMESTAMP_CACHE, Timestamp.class, Person.class));
        ccfgs.addAll(getCacheConfigurations(BIG_DECIMAL_CACHE, BigDecimal.class, Person.class));
        ccfgs.add(getCacheConfiguration(FIELD_CACHE, Integer.class, SearchValue.class));

        return ccfgs.toArray(new CacheConfiguration[ccfgs.size()]);
    }

    /**
     * @param cacheName Cache name.
     * @param key Key type.
     * @param val Value type.
     * @return Configurations.
     */
    private List<CacheConfiguration> getCacheConfigurations(final String cacheName, final Class<?> key,
        final Class<?> val) {
        final List<CacheConfiguration> res = new ArrayList<>();

        res.add(getCacheConfiguration(cacheName, key, val));
        res.add(getCacheConfiguration(cacheName + "-val", val, key));

        return res;
    }

    /**
     * @param cacheName Cache name.
     * @param key Key type.
     * @param val Value type
     * @return Configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration getCacheConfiguration(final String cacheName, final Class<?> key, final Class<?> val) {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setName(cacheName);

        cfg.setIndexedTypes(key, val);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        final int nodes = isLocal() ? 1 : NODES;

        startGridsMultiThreaded(nodes);
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
        testKeyQuery(OBJECT_CACHE, new TestKey(1), new TestKey(2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimitiveObjectArgument() throws Exception {
        testKeyValQuery(PRIM_CACHE, 1, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringObjectArgument() throws Exception {
        testKeyValQuery(STR_CACHE, "str1", "str2");
    }

    /**
     * @throws Exception If failed.
     */
    public void testEnumObjectArgument() throws Exception {
        testKeyValQuery(ENUM_CACHE, EnumKey.KEY1, EnumKey.KEY2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUuidObjectArgument() throws Exception {
        final UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();

        while (uuid1.equals(uuid2))
            uuid2 = UUID.randomUUID();

        testKeyValQuery(UUID_CACHE, uuid1, uuid2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDateObjectArgument() throws Exception {
        testKeyValQuery(DATE_CACHE, new Date(0), new Date(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimestampArgument() throws Exception {
        testKeyValQuery(TIMESTAMP_CACHE, new Timestamp(0), new Timestamp(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBigDecimalArgument() throws Exception {
        final ThreadLocalRandom rnd = ThreadLocalRandom.current();

        final BigDecimal bd1 = new BigDecimal(rnd.nextDouble());
        BigDecimal bd2 = new BigDecimal(rnd.nextDouble());

        while (bd1.equals(bd2))
            bd2 = new BigDecimal(rnd.nextDouble());

        testKeyValQuery(BIG_DECIMAL_CACHE, bd1, bd2);
    }

    /**
     * Test simple queries.
     *
     * @param cacheName Cache name.
     * @param key1 Key 1.
     * @param key2 Key 2.
     * @param <T> Key type.
     */
    private <T> void testKeyValQuery(final String cacheName, final T key1, final T key2) {
        testKeyQuery(cacheName, key1, key2);
        testValQuery(cacheName + "-val", key1, key2);
    }

    /**
     * Test simple query by key.
     *
     * @param cacheName Cache name.
     * @param key1 Key 1.
     * @param key2 Key 2.
     * @param <T> Key type.
     */
    private <T> void testKeyQuery(final String cacheName, final T key1, final T key2) {
        final IgniteCache<T, Person> cache = ignite(0).cache(cacheName);

        final Person p1 = new Person("p1");
        final Person p2 = new Person("p2");

        cache.put(key1, p1);
        cache.put(key2, p2);

        final SqlQuery<T, Person> qry = new SqlQuery<>(Person.class, "where _key=?");

        final SqlFieldsQuery fieldsQry = new SqlFieldsQuery("select _key, _val, * from Person where _key=?");

        qry.setLocal(isLocal());
        fieldsQry.setLocal(isLocal());

        qry.setArgs(key1);
        fieldsQry.setArgs(key1);

        final List<Cache.Entry<T, Person>> res = cache.query(qry).getAll();
        final List<List<?>> fieldsRes = cache.query(fieldsQry).getAll();

        assertEquals(1, res.size());
        assertEquals(1, fieldsRes.size());

        assertEquals(p1, res.get(0).getValue());
        assertEquals(key1, res.get(0).getKey());

        assertTrue(fieldsRes.get(0).size() >= 2);
        assertEquals(key1, fieldsRes.get(0).get(0));
        assertEquals(p1, fieldsRes.get(0).get(1));
    }

    /**
     * Test simple query by value.
     *
     * @param cacheName Cache name.
     * @param val1 Value 1.
     * @param val2 Value 2.
     * @param <T> Value type.
     */
    private <T> void testValQuery(final String cacheName, final T val1, final T val2) {
        final IgniteCache<Person, T> cache = ignite(0).cache(cacheName);

        final Class<?> valType = val1.getClass();

        final Person p1 = new Person("p1");
        final Person p2 = new Person("p2");

        cache.put(p1, val1);
        cache.put(p2, val2);

        final SqlQuery<Person, T> qry = new SqlQuery<>(valType, "where _val=?");

        final SqlFieldsQuery fieldsQry = new SqlFieldsQuery("select _key, _val, * from " + valType.getSimpleName() + " where _val=?");

        qry.setLocal(isLocal());
        fieldsQry.setLocal(isLocal());

        qry.setArgs(val1);
        fieldsQry.setArgs(val1);

        final List<Cache.Entry<Person, T>> res = cache.query(qry).getAll();
        final List<List<?>> fieldsRes = cache.query(fieldsQry).getAll();

        assertEquals(1, res.size());
        assertEquals(1, fieldsRes.size());

        assertEquals(p1, res.get(0).getKey());
        assertEquals(val1, res.get(0).getValue());

        assertTrue(fieldsRes.get(0).size() >= 2);
        assertEquals(p1, fieldsRes.get(0).get(0));
        assertEquals(val1, fieldsRes.get(0).get(1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldSearch() throws Exception {
        final IgniteCache<Integer, SearchValue> cache = ignite(0).cache(FIELD_CACHE);

        final Map<Integer, SearchValue> map = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            map.put(i,
                new SearchValue(
                    UUID.randomUUID(),
                    String.valueOf(i),
                    new BigDecimal(i * 0.1),
                    i,
                    new Date(i),
                    new Timestamp(i),
                    new Person(String.valueOf("name-" + i)),
                    i % 2 == 0 ? EnumKey.KEY1 : EnumKey.KEY2)
            );
        }

        cache.putAll(map);

        SqlQuery<Integer, SearchValue> qry = new SqlQuery<>(SearchValue.class,
            "where uuid=? and str=? and decimal=? and integer=? and date=? and ts=? and person=? and enumKey=?");

        final int k = ThreadLocalRandom.current().nextInt(10);

        final SearchValue val = map.get(k);

        qry.setLocal(isLocal());
        qry.setArgs(val.uuid, val.str, val.decimal, val.integer, val.date, val.ts, val.person, val.enumKey);

        final List<Cache.Entry<Integer, SearchValue>> res = cache.query(qry).getAll();

        assertEquals(1, res.size());

        assertEquals(val.integer, res.get(0).getKey());
        assertEquals(val, res.get(0).getValue());
    }

    /** */
    private static class Person {
        /** */
        @BinaryCompression
        String name;

        /**
         * @param name Name.
         */
        public Person(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final Person person = (Person)o;

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

    /** */
    private static class TestKey {
        /** */
        @BinaryCompression
        private int id;

        /**
         * @param id Key.
         */
        TestKey(int id) {
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

    /** */
    private static class SearchValue {
        /** */
        @BinaryCompression
        @QuerySqlField
        private UUID uuid;

        /** */
        @BinaryCompression
        @QuerySqlField
        private String str;

        /** */
        @BinaryCompression
        @QuerySqlField
        private BigDecimal decimal;

        /** */
        @BinaryCompression
        @QuerySqlField
        private Integer integer;

        /** */
        @BinaryCompression
        @QuerySqlField
        private Date date;

        /** */
        @BinaryCompression
        @QuerySqlField
        private Timestamp ts;

        /** */
        @BinaryCompression
        @QuerySqlField
        private Person person;

        /** */
        @BinaryCompression
        @QuerySqlField
        private EnumKey enumKey;

        /**
         * @param uuid UUID.
         * @param str String.
         * @param decimal Decimal.
         * @param integer Integer.
         * @param date Date.
         * @param ts Timestamp.
         * @param person Person.
         * @param enumKey Enum.
         */
        SearchValue(
            final UUID uuid,
            final String str,
            final BigDecimal decimal,
            final Integer integer,
            final Date date,
            final Timestamp ts,
            final Person person,
            final EnumKey enumKey
        ) {
            this.uuid = uuid;
            this.str = str;
            this.decimal = decimal;
            this.integer = integer;
            this.date = date;
            this.ts = ts;
            this.person = person;
            this.enumKey = enumKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final SearchValue that = (SearchValue)o;

            if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null)
                return false;
            if (str != null ? !str.equals(that.str) : that.str != null)
                return false;
            if (decimal != null ? !decimal.equals(that.decimal) : that.decimal != null)
                return false;
            if (integer != null ? !integer.equals(that.integer) : that.integer != null)
                return false;
            if (date != null ? !date.equals(that.date) : that.date != null)
                return false;
            if (ts != null ? !ts.equals(that.ts) : that.ts != null)
                return false;
            if (person != null ? !person.equals(that.person) : that.person != null)
                return false;
            return enumKey == that.enumKey;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = uuid != null ? uuid.hashCode() : 0;
            res = 31 * res + (str != null ? str.hashCode() : 0);
            res = 31 * res + (decimal != null ? decimal.hashCode() : 0);
            res = 31 * res + (integer != null ? integer.hashCode() : 0);
            res = 31 * res + (date != null ? date.hashCode() : 0);
            res = 31 * res + (ts != null ? ts.hashCode() : 0);
            res = 31 * res + (person != null ? person.hashCode() : 0);
            res = 31 * res + (enumKey != null ? enumKey.hashCode() : 0);
            return res;
        }
    }

    /** */
    private enum EnumKey {
        /** */
        @BinaryCompression
        KEY1,

        /** */
        @BinaryCompression
        KEY2
    }
}
