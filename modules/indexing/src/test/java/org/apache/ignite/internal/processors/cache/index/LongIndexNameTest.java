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

package org.apache.ignite.internal.processors.cache.index;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Regression test for the long index name.
 */
public class LongIndexNameTest extends AbstractIndexingCommonTest {
    /**
     * Create configuration with persistence disabled.
     */
    protected IgniteConfiguration createConfiguration(boolean enablePersistence) throws Exception {
        IgniteConfiguration cfg = getConfiguration("ignite-inmemory");

        if (enablePersistence) {
            DataStorageConfiguration dataStorage = new DataStorageConfiguration();

            dataStorage.setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setName("default").setPersistenceEnabled(true));

            cfg.setDataStorageConfiguration(dataStorage);
        }

        return cfg;
    }

    /**
     * Creates cache configuration with test table in it. If specified index name is not {@code null}, then index on
     * "age" field with such index name will be added.
     *
     * @param ageIdxName name of the index on the "age" field. If null, then no index will be added.
     */
    protected CacheConfiguration cacheConfiguration(@Nullable String ageIdxName) {
        QueryEntity tab = new QueryEntity(String.class.getName(), Person.class.getName());

        LinkedHashMap<String, String> fieldsMap = new LinkedHashMap<>();

        fieldsMap.put("name", String.class.getName());
        fieldsMap.put("age", Integer.class.getName());
        fieldsMap.put("ageDyn", Integer.class.getName());

        tab.setFields(fieldsMap);

        if (ageIdxName != null) {
            ArrayList<QueryIndex> indices = new ArrayList<>();

            QueryIndex index = new QueryIndex("name", true, ageIdxName);

            QueryIndex index2 = new QueryIndex("age", true, "AGE_IDX");

            indices.add(index);
            indices.add(index2);

            tab.setIndexes(indices);
        }

        return new CacheConfiguration("cache").setQueryEntities(Collections.singleton(tab));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    @Test
    public void testStartupIndexLongNameWithPersistence() throws Exception {
        int segmentsCnt = 12;

        int maxAllowedIdxName = maxIdxNameLength(segmentsCnt);

        Callable<IgniteConfiguration> withLongOKLen = () -> createConfiguration(true)
            .setCacheConfiguration(cacheConfiguration(generateName(maxAllowedIdxName))
                .setQueryParallelism(segmentsCnt));

        // Insert data and check idx vs scan:
        try (Ignite ignite = startGrid(withLongOKLen.call())) {
            ignite.cluster().active(true);

            insertSomeData(ignite);

            compareIndexVsScan(ignite);
        }

        // Read from disk and verify again:
        try (Ignite ignite = startGrid(withLongOKLen.call())){
            ignite.cluster().active(true);

            compareIndexVsScan(ignite);
        }
    }

    @Test
    public void testNegativeStartupIndexLongNameWithPersistence () throws Exception {
        int segmentsCnt = 12;

        int maxAllowedIdxName = maxIdxNameLength(segmentsCnt);

        IgniteConfiguration withTooLongIdxName = createConfiguration(true)
            .setCacheConfiguration(cacheConfiguration(generateName(maxAllowedIdxName + 1))
                .setQueryParallelism(12));

        GridTestUtils.assertThrows(log(), () -> {
            try (IgniteEx ign = startGrid(withTooLongIdxName)) {
                // No-op. Just try to start the grid
                return null;
            }
        }, IgniteCheckedException.class, "Index name is too long in UTF-8 encoding [maxAllowed=211");
    }

    @Test
    public void testDynamicIndexLongNameWithPersistence() throws Exception {
        int segmentsCnt = 12;

        int maxAllowedIdxName = maxIdxNameLength(segmentsCnt);

        Callable <IgniteConfiguration> withLongOKLen = () -> createConfiguration(true)
            .setCacheConfiguration(cacheConfiguration(null)
                .setQueryParallelism(segmentsCnt));

        // Insert data and check idx vs scan:
        try (Ignite ignite = startGrid(withLongOKLen.call())) {
            ignite.cluster().active(true);

            ignite.cache("cache").query(new SqlFieldsQuery(
                "CREATE INDEX " + generateName(maxAllowedIdxName) + " ON Person(age)"));

            insertSomeData(ignite);

            compareIndexVsScan(ignite);
        }

        // Read from disk and verify again:
        try (Ignite ignite = startGrid(withLongOKLen.call())){
            ignite.cluster().active(true);

            compareIndexVsScan(ignite);
        }
    }

    @Test
    public void testNegativeDynamicIndexLongNameWithPersistence() throws Exception {
        int segmentsCnt = 12;

        int minDisallowedIdxLen = maxIdxNameLength(segmentsCnt) + 1;

        IgniteConfiguration withNoIdx = createConfiguration(true)
            .setCacheConfiguration(cacheConfiguration(null)
                .setQueryParallelism(segmentsCnt));


        GridTestUtils.assertThrows(log(), () -> {
            try (IgniteEx ignite = startGrid(withNoIdx)) {
                ignite.cluster().active(true);

                ignite.cache("cache").query(new SqlFieldsQuery(
                    "CREATE INDEX " + generateName(minDisallowedIdxLen) + " ON Person(age)"));

                return null;
            }
        }, IgniteSQLException.class, "Index name is too long in UTF-8 encoding [maxAllowed=211");
    }

    /**
     * Generates name that has specified number of bytes in the utf-8 encoding. Since name contains only ASCII symbols
     * this is the same as just length of the string.
     *
     * @param utf8Len Utf 8 length.
     */
    private static String generateName(int utf8Len) {
        StringBuilder sb = new StringBuilder(utf8Len);

        for (int i = 0; i < utf8Len; i++)
            sb.append('a');

        String res = sb.toString();

        assert res.getBytes(StandardCharsets.UTF_8).length == utf8Len;

        return res;
    }

    /**
     * Computes fine maximum allowed index length.
     *
     * @param segmentsCnt Segments count.
     */
    private int maxIdxNameLength(int segmentsCnt) {
        return H2TreeIndex.MAX_PDS_UNMASKED_LEN;
    }

    // 11 + 1 + 2 + 6 + 1 + 11 + 11 + 1

    /**
     * Checks that inmemory mode 1) allows index names longer than in persistence mode 2) using such indexes doesn't
     * corrupt the results.
     */
    @Test
    public void testLongStartupIndexNameInmemory() throws Exception {
        int segmentsCnt = 12;

        int maxAllowedPdsIdxName = maxIdxNameLength(segmentsCnt);

        IgniteConfiguration withIdxCfg = createConfiguration(false)
            .setCacheConfiguration(cacheConfiguration(generateName(maxAllowedPdsIdxName + 1))
            .setQueryParallelism(segmentsCnt));

        try (Ignite ignite = startGrid(withIdxCfg)) {
            insertSomeData(ignite);

            compareIndexVsScan(ignite);
        }
    }

    /**
     * Same as {@link #testLongStartupIndexNameInmemory()}, but creates indexes dynamically using CREATE INDEX.
     */
    @Test
    public void testLongDynamicIndexNameInMemory() throws Exception {
        IgniteConfiguration noidxCfg = createConfiguration(false)
            .setCacheConfiguration(cacheConfiguration(null));

        try (Ignite ignite = startGrid(noidxCfg)) {
            insertSomeData(ignite);

            ignite.cache("cache").query(new SqlFieldsQuery("CREATE INDEX AGE_IDX ON Person(age)"));

            compareIndexVsScan(ignite);
        }
    }

    @Deprecated
    private void checkIndexResultsCorrect(IgniteConfiguration cfg) throws Exception {
        try (Ignite ignite = startGrid(cfg)) {
            ignite.cluster().active(true);

            IgniteCache cache = insertSomeData(ignite);

            QueryCursor cursor1 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name like '%Name 0'"));
            QueryCursor cursor1Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name = 'Name 0'"));

            QueryCursor cursor2 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age like '%0'"));
            QueryCursor cursor2Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age = 0"));

            assertEquals(cursor1.getAll().size(), cursor1Idx.getAll().size());
            assertEquals(cursor2.getAll().size(), cursor2Idx.getAll().size());
        }

        Thread.sleep(2_000);

        try (Ignite ignite = startGrid(cfg)) {
            ignite.cluster().active(true);

            IgniteCache cache = insertSomeData(ignite);

            QueryCursor cursor1 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name like '%Name 0'"));
            QueryCursor cursor1Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name = 'Name 0'"));

            QueryCursor cursor2 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age like '%0'"));
            QueryCursor cursor2Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age = 0"));

            assertEquals(cursor1.getAll().size(), cursor1Idx.getAll().size());
            assertEquals(cursor2.getAll().size(), cursor2Idx.getAll().size());
        }
    }

    private void compareIndexVsScan(Ignite ignite) {
        IgniteCache<String, Person> cache = ignite.cache("cache");

        QueryCursor cursor1 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name like '%Name 0'"));
        QueryCursor cursor1Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name = 'Name 0'"));

        QueryCursor cursor2 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age like '%0'"));
        QueryCursor cursor2Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age = 0"));

        assertEquals(cursor1.getAll().size(), cursor1Idx.getAll().size());
        assertEquals(cursor2.getAll().size(), cursor2Idx.getAll().size());
    }

    /**
     *
     */
    @NotNull private IgniteCache insertSomeData(Ignite ignite) {
        if (!ignite.active())
            ignite.active(true);

        IgniteCache<String, Person> cache = ignite.cache("cache");

        for (int i = 0; i < 10; i++)
            cache.put(String.valueOf(System.currentTimeMillis()), new Person("Name " + i, i));

        return cache;
    }

    /**
     * Cache value type.
     */
    private static class Person {
        /** */
        private String name;

        /** */
        private int age;

        /**
         *
         */
        public Person() {
            // No-op.
        }

        /**
         * @param name Name.
         * @param age Age.
         */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * @return Age.
         */
        public int getAge() {
            return age;
        }

        /**
         * @param age Age.
         */
        public void setAge(int age) {
            this.age = age;
        }
    }
}
