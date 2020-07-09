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
 *
 */

package org.apache.ignite.internal.processors.cache.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.IoStatisticsType;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.metric.IoStatisticsCacheSelfTest.logicalReads;
import static org.apache.ignite.internal.metric.IoStatisticsMetricsLocalMXBeanImplSelfTest.resetAllIoMetrics;
import static org.apache.ignite.internal.metric.IoStatisticsType.CACHE_GROUP;
import static org.apache.ignite.internal.metric.IoStatisticsType.HASH_INDEX;
import static org.apache.ignite.internal.metric.IoStatisticsType.SORTED_INDEX;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.queryProcessor;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * A set of basic tests for caches with indexes.
 */
public class IoStatisticsBasicIndexSelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final int NUMBER_OF_PK_SORTED_INDEXES = 1;

    /** */
    private static final Set<String> PK_HASH_INDEXES = Sets.newHashSet(DEFAULT_CACHE_NAME);

    /** */
    private Collection<QueryIndex> indexes;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        assertNotNull(indexes);

        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("keyStr", String.class.getName());
        fields.put("keyLong", Long.class.getName());
        fields.put("keyPojo", Pojo.class.getName());
        fields.put("valStr", String.class.getName());
        fields.put("valLong", Long.class.getName());
        fields.put("valPojo", Pojo.class.getName());

        Set<String> keyFields = new HashSet<>();
        keyFields.add("keyStr");
        keyFields.add("keyLong");
        keyFields.add("keyPojo");

        CacheConfiguration<Key, Val> ccfg = new CacheConfiguration<Key, Val>(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singleton(
                new QueryEntity()
                    .setKeyType(Key.class.getName())
                    .setValueType(Val.class.getName())
                    .setKeyFields(keyFields)
                    .setFields(fields)
                    .setIndexes(indexes)
            ));

        igniteCfg.setCacheConfiguration(ccfg);

        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )
        );

        return igniteCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        indexes = null;

        super.afterTest();
    }

    /**
     * @throws Exception In case of failure.
     */
    @Test
    public void testNoIndexes() throws Exception {
        indexes = Collections.emptyList();

        startGrid();

        grid().cluster().active(true);

        populateCache();

        checkStat();

        checkAll();

        checkStat();
    }

    /**
     * @throws Exception In case of failure.
     */
    @Test
    public void testAllIndexes() throws Exception {
        indexes = Arrays.asList(
            new QueryIndex("keyStr"),
            new QueryIndex("keyLong"),
            new QueryIndex("keyPojo"),
            new QueryIndex("valStr"),
            new QueryIndex("valLong"),
            new QueryIndex("valPojo")
        );

        startGrid();

        grid().cluster().active(true);

        populateCache();

        checkStat();

        checkAll();

        checkStat();
    }

    /** Checks that {@link MetricRegistry} with the sorted index IO statistics removed on index drop. */
    @Test
    public void testMetricRegistryRemovedOnIndexDrop() throws Exception {
        indexes = Collections.emptyList();

        startGrid();

        grid().cluster().active(true);

        execute(grid(), "CREATE TABLE t(id int, name varchar, primary key (id))");

        execute(grid(), "CREATE INDEX MY_IDX ON t(name)");

        MetricRegistry mreg =
            grid().context().metric().registry(metricName(SORTED_INDEX.metricGroupName(), "SQL_PUBLIC_T", "MY_IDX"));

        assertTrue(mreg.iterator().hasNext());
        assertNotNull(mreg.findMetric("name"));

        execute(grid(), "DROP INDEX MY_IDX");

        mreg = grid().context().metric().registry(metricName(SORTED_INDEX.metricGroupName(), "SQL_PUBLIC_T", "MY_IDX"));

        assertFalse(mreg.iterator().hasNext());
    }

    /** */
    private void checkStat() throws Exception {
        GridMetricManager mmgr = grid().context().metric();

        Set<String> hashIndexes = deriveStatisticNames(grid(), HASH_INDEX);

        Assert.assertEquals(PK_HASH_INDEXES, hashIndexes);

        Set<String> sortedIndexCaches = deriveStatisticNames(grid(), SORTED_INDEX);

        Assert.assertEquals(1, sortedIndexCaches.size());

        Set<String> sortedIdxNames = deriveStatisticSubNames(grid(), SORTED_INDEX);

        Assert.assertEquals(sortedIndexCaches.toString(), indexes.size() + NUMBER_OF_PK_SORTED_INDEXES,
            sortedIdxNames.size());

        for (String idxName : sortedIdxNames) {
            Long logicalReads = logicalReads(mmgr, SORTED_INDEX, metricName(DEFAULT_CACHE_NAME, idxName));

            Assert.assertNotNull(idxName, logicalReads);

            Assert.assertTrue(logicalReads > 0);
        }

        resetAllIoMetrics(grid());
    }

    /** */
    private void checkAll() {
        IgniteCache<Key, Val> cache = grid().cache(DEFAULT_CACHE_NAME);

        checkRemovePut(cache);

        checkSelectAll(cache);

        checkSelectStringEqual(cache);

        checkSelectLongEqual(cache);

        checkSelectStringRange(cache);

        checkSelectLongRange(cache);
    }

    /** */
    private void populateCache() {
        IgniteCache<Key, Val> cache = grid().cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(key(i), val(i));
    }

    /**
     * @param cache Cache.
     */
    private void checkRemovePut(IgniteCache<Key, Val> cache) {
        final int INT = 24;

        assertEquals(val(INT), cache.get(key(INT)));

        cache.remove(key(INT));

        assertNull(cache.get(key(INT)));

        cache.put(key(INT), val(INT));

        assertEquals(val(INT), cache.get(key(INT)));
    }

    /**
     * @param cache Cache.
     */
    private void checkSelectAll(IgniteCache<Key, Val> cache) {
        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val")).getAll();

        assertEquals(100, data.size());

        for (List<?> row : data) {
            Key key = (Key)row.get(0);

            Val val = (Val)row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);
        }
    }

    /**
     * @param cache Cache.
     */
    private void checkSelectStringEqual(IgniteCache<Key, Val> cache) {
        final String STR = "foo011";

        final long LONG = 11;

        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val where keyStr = ?")
            .setArgs(STR))
            .getAll();

        assertEquals(1, data.size());

        List<?> row = data.get(0);

        assertEquals(key(LONG), row.get(0));

        assertEquals(val(LONG), row.get(1));
    }

    /**
     * @param cache Cache.
     */
    private void checkSelectLongEqual(IgniteCache<Key, Val> cache) {
        final long LONG = 42;

        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val where valLong = ?")
            .setArgs(LONG))
            .getAll();

        assertEquals(1, data.size());

        List<?> row = data.get(0);

        assertEquals(key(LONG), row.get(0));

        assertEquals(val(LONG), row.get(1));
    }

    /**
     * @param cache Cache.
     */
    private void checkSelectStringRange(IgniteCache<Key, Val> cache) {
        final String PREFIX = "foo06";

        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val where keyStr like ?")
            .setArgs(PREFIX + "%"))
            .getAll();

        assertEquals(10, data.size());

        for (List<?> row : data) {
            Key key = (Key)row.get(0);

            Val val = (Val)row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);

            assertTrue(key.keyStr.startsWith(PREFIX));
        }
    }

    /**
     * @param cache Cache.
     */
    private void checkSelectLongRange(IgniteCache<Key, Val> cache) {
        final long RANGE_START = 70;

        final long RANGE_END = 80;

        List<List<?>> data = cache.query(
            new SqlFieldsQuery("select _key, _val from Val where valLong >= ? and valLong < ?")
                .setArgs(RANGE_START, RANGE_END))
            .getAll();

        assertEquals(10, data.size());

        for (List<?> row : data) {
            Key key = (Key)row.get(0);

            Val val = (Val)row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);

            assertTrue(i >= RANGE_START && i < RANGE_END);
        }
    }

    /**
     * Extract all tracked subNames for given statistics type .
     *
     * @param statType Type of statistics which tracked names need to extract.
     * @return Set of present names for given statType
     */
    public Set<String> deriveStatisticSubNames(IgniteEx ignite, IoStatisticsType statType) {
        assert statType != null;

        return ioStats(ignite, statType).map(m -> {
            if (m.name().startsWith(CACHE_GROUP.metricGroupName()))
                return m.findMetric("grpId").getAsString();

            if (m.name().startsWith(HASH_INDEX.metricGroupName()) ||
                m.name().startsWith(SORTED_INDEX.metricGroupName()))
                return m.findMetric("indexName").getAsString();

            throw new IgniteException("Wrong metric registry " + m.name());
        }).collect(Collectors.toSet());
    }

    /**
     * Extract all tracked names for given statistics type.
     *
     * @param ignite Node instance.
     * @param statType Type of statistics which tracked names need to extract.
     * @return Set of present names for given statType
     */
    public Set<String> deriveStatisticNames(IgniteEx ignite, IoStatisticsType statType) {
        assert statType != null;

        Stream<ReadOnlyMetricRegistry> grpsStream = ioStats(ignite, statType);

        return grpsStream.flatMap(grp -> StreamSupport.stream(grp.spliterator(), false))
            .filter(m -> m.name().endsWith("name"))
            .map(Metric::getAsString)
            .collect(Collectors.toSet());
    }

    /** @return Stream of MetricGroup for specified {@link statType}. */
    private Stream<ReadOnlyMetricRegistry> ioStats(IgniteEx ignite, IoStatisticsType statType) {
        GridMetricManager mmgr = ignite.context().metric();

        return StreamSupport.stream(mmgr.spliterator(), false)
            .filter(grp -> grp.name().startsWith(statType.metricGroupName()));
    }

    /**
     * @param i Index to generate key.
     * @return generated key.
     */
    private static Key key(long i) {
        return new Key(String.format("foo%03d", i), i, new Pojo(i));
    }

    /**
     * @param i Index to generate key.
     * @return generated key.
     */
    private static Val val(long i) {
        return new Val(String.format("bar%03d", i), i, new Pojo(i));
    }

    /** */
    private static class Key {
        /** */
        private String keyStr;

        /** */
        private long keyLong;

        /** */
        private Pojo keyPojo;

        /**
         * @param str String.
         * @param aLong Long.
         * @param pojo Pojo.
         */
        private Key(String str, long aLong, Pojo pojo) {
            keyStr = str;
            keyLong = aLong;
            keyPojo = pojo;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return keyLong == key.keyLong &&
                Objects.equals(keyStr, key.keyStr) &&
                Objects.equals(keyPojo, key.keyPojo);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(keyStr, keyLong, keyPojo);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Key.class, this);
        }
    }

    /** */
    private static class Val {
        /** */
        private String valStr;

        /** */
        private long valLong;

        /** */
        private Pojo valPojo;

        /**
         * @param str String.
         * @param aLong Long.
         * @param pojo Pojo.
         */
        private Val(String str, long aLong, Pojo pojo) {
            valStr = str;
            valLong = aLong;
            valPojo = pojo;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Val val = (Val)o;

            return valLong == val.valLong &&
                Objects.equals(valStr, val.valStr) &&
                Objects.equals(valPojo, val.valPojo);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(valStr, valLong, valPojo);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Val.class, this);
        }
    }

    /** */
    private static class Pojo {
        /** */
        private long pojoLong;

        /**
         * @param pojoLong Long.
         */
        private Pojo(long pojoLong) {
            this.pojoLong = pojoLong;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Pojo pojo = (Pojo)o;

            return pojoLong == pojo.pojoLong;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(pojoLong);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Pojo.class, this);
        }
    }

    /**
     * Execute query on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    private List<List<?>> execute(Ignite node, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args)
            .setSchema("PUBLIC");

        return queryProcessor(node).querySqlFields(qry, true).getAll();
    }
}
