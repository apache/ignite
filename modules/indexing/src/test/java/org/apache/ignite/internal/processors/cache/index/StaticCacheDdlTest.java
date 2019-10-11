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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class StaticCacheDdlTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSISTENT_CACHE_NAME = "PERSISTENTCACHE";

    /** */
    private static final String MEMORY_CACHE_NAME = "MEMORYCACHE";

    /** */
    private static final String TABLE_NAME = "PERSONS";

    /** */
    public static final String PERSISTENT_REGION_NAME = "PERSISTENT_REGION_NAME";

    /** */
    public static final String MEMORY_REGION_NAME = "MEMORY_REGION_NAME";

    /**
     * @throws Exception If failed.
     */
    @Before
    public void clearPersistence() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @After
    public void cleanup() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @return {@code true} if static cache config is ignored by Ignite.
     */
    protected boolean ignoreStaticConfig() {
        return !IgniteSystemProperties.getBoolean(
            IgniteSystemProperties.IGNITE_KEEP_STATIC_CACHE_CONFIGURATION);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAddColumn() throws Exception {
        String fieldName = "new_field";

        int size = 10;

        try (Ignite ignite = startGrid(0)) {
            ignite.cluster().active(true);

            insertData(ignite, PERSISTENT_CACHE_NAME, size);
            insertData(ignite, MEMORY_CACHE_NAME, size);

            checkField(ignite, PERSISTENT_CACHE_NAME, fieldName, false);
            checkField(ignite, MEMORY_CACHE_NAME, fieldName, false);

            addColumn(ignite, PERSISTENT_CACHE_NAME, fieldName);
            addColumn(ignite, MEMORY_CACHE_NAME, fieldName);

            checkTableSize(ignite, PERSISTENT_CACHE_NAME, size);
            checkTableSize(ignite, MEMORY_CACHE_NAME, size);

            checkField(ignite, PERSISTENT_CACHE_NAME, fieldName, ignoreStaticConfig());
            checkField(ignite, MEMORY_CACHE_NAME, fieldName, true);
        }

        // Check the column after restart the node.
        try (Ignite ignite = startGrid(0)) {
            checkTableSize(ignite, PERSISTENT_CACHE_NAME, size);

            checkField(ignite, PERSISTENT_CACHE_NAME, fieldName, ignoreStaticConfig());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testDropColumn() throws Exception {
        String fieldName = "field_to_drop";

        int size = 10;

        try (Ignite ignite = startGrid(0)) {
            ignite.cluster().active(true);

            insertData(ignite, PERSISTENT_CACHE_NAME, size);
            insertData(ignite, MEMORY_CACHE_NAME, size);

            checkField(ignite, PERSISTENT_CACHE_NAME, fieldName, true);
            checkField(ignite, MEMORY_CACHE_NAME, fieldName, true);

            dropColumn(ignite, PERSISTENT_CACHE_NAME, fieldName);
            dropColumn(ignite, MEMORY_CACHE_NAME, fieldName);

            checkField(ignite, PERSISTENT_CACHE_NAME, fieldName, !ignoreStaticConfig());
            checkField(ignite, MEMORY_CACHE_NAME, fieldName, false);
        }

        // Check the column after restart the node.
        try (Ignite ignite = startGrid(0)) {
            checkField(ignite, PERSISTENT_CACHE_NAME, fieldName, !ignoreStaticConfig());
        }
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAddIndex() throws Exception {
        String fieldName = "some_field";

        String idxName = TABLE_NAME + "_" + fieldName.toUpperCase() + "_IDX";

        int size = 10;

        try (Ignite ignite = startGrid(0)) {
            ignite.cluster().active(true);

            insertData(ignite, PERSISTENT_CACHE_NAME, size);
            insertData(ignite, MEMORY_CACHE_NAME, size);

            checkIndex(ignite, PERSISTENT_CACHE_NAME, idxName, fieldName, false);
            checkIndex(ignite, MEMORY_CACHE_NAME, idxName, fieldName, false);

            addIndex(ignite, PERSISTENT_CACHE_NAME, idxName, fieldName);
            addIndex(ignite, MEMORY_CACHE_NAME, idxName, fieldName);

            checkIndex(ignite, PERSISTENT_CACHE_NAME, idxName, fieldName, ignoreStaticConfig());
            checkIndex(ignite, MEMORY_CACHE_NAME, idxName, fieldName, true);
        }

        // Check the column after restart the node.
        try (Ignite ignite = startGrid(0)) {
            checkIndex(ignite, PERSISTENT_CACHE_NAME, idxName, fieldName, ignoreStaticConfig());
        }
    }

    /**
     * This method checks if an index with the given name exists.
     *
     * @param ignite Ignite instance to check on.
     * @param idxName Index name.
     * @param fieldName Field name to check index on.
     * @param shouldExist Should exist flag.
     */
    private void checkIndex(Ignite ignite, String cacheName, String idxName, String fieldName, boolean shouldExist) {
        SqlFieldsQuery q = new SqlFieldsQuery(
            "EXPLAIN SELECT * FROM " + cacheName + "." + TABLE_NAME + " WHERE " + fieldName + " = ?" ).setArgs("");

        boolean exists = false;

        try (FieldsQueryCursor<List<?>> cursor = ignite.cache(cacheName).query(q)) {
            for (List<?> row : cursor) {
                if (row.toString().contains(idxName)) {
                    exists = true;

                    break;
                }
            }
        }

        Assert.assertEquals("Check index (" + idxName + ") exists", shouldExist, exists);
    }

    /**
     * @param ignite Ignite instance.
     * @param fieldName Field name to check.
     * @param shouldExist Should exist flag.
     */
    private void checkField(Ignite ignite, String cacheName, String fieldName, boolean shouldExist) {
        SqlFieldsQuery q = new SqlFieldsQuery(
            "SELECT * FROM " + cacheName + "." + TABLE_NAME + " LIMIT 1 OFFSET 0" );

        boolean exists = false;

        try (FieldsQueryCursor<List<?>> cursor = ignite.cache(cacheName).query(q)) {
            consume(cursor);

            for (int i = 0, cols = cursor.getColumnsCount(); i < cols; i++) {
                if (cursor.getFieldName(i).equals(fieldName.toUpperCase())) {
                    exists = true;

                    break;
                }
            }
        }

        Assert.assertEquals("Check field (" + fieldName + ") exists", shouldExist, exists);
    }

    /**
     * @param ignite Ignite instance.
     * @param expSize Expected size.
     */
    private void checkTableSize(Ignite ignite, String cacheName, int expSize) {
        SqlFieldsQuery q = new SqlFieldsQuery(
            "SELECT * FROM " + cacheName + "." + TABLE_NAME );

        try (QueryCursor<List<?>> cursor = ignite.cache(cacheName).query(q)) {
            int actualSize = 0;

            for (List<?> ignore : cursor)
                actualSize++;

            Assert.assertEquals("Check result set size", expSize, actualSize);
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param size Number of rows to add.
     */
    private void insertData(Ignite ignite, String cacheName, int size) {
        int rows = 0;

        for (int i = 0; i < size; i++) {
            SqlFieldsQuery q = new SqlFieldsQuery(
                "INSERT INTO " + cacheName + "." + TABLE_NAME + "(id, name) VALUES(?, ?)").
                setArgs(i, UUID.randomUUID().toString());

            try (QueryCursor<List<?>> cursor = ignite.cache(cacheName).query(q)) {
                for (List<?> ignore : cursor)
                    rows++;
            }
        }

        info(rows + " rows processed");
    }

    /**
     * @param ignite Ignite instance.
     * @param idxName Index name to add.
     * @param fieldName Field name to add index on.
     */
    private void addIndex(Ignite ignite, String cacheName, String idxName, String fieldName) {
        SqlFieldsQuery q = new SqlFieldsQuery("CREATE INDEX  " + idxName + " ON " + cacheName + "." + TABLE_NAME +
            "(" + fieldName + ")");

        try {
            try (QueryCursor<List<?>> cursor = ignite.cache(cacheName).query(q)) {
                consume(cursor);
            }
        }
        catch (CacheException e) {
            assertTrue("Unexpected exception: " + e.getMessage(),
                cacheName.equalsIgnoreCase(PERSISTENT_CACHE_NAME) && !ignoreStaticConfig());
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param fieldName Field name to add.
     */
    private void addColumn(Ignite ignite, String cacheName, String fieldName) {
        SqlFieldsQuery q = new SqlFieldsQuery("ALTER TABLE " + cacheName + "." + TABLE_NAME + " " +
            "ADD COLUMN " + fieldName + " VARCHAR");

        try {
            try (QueryCursor<List<?>> cursor = ignite.cache(cacheName).query(q)) {
                consume(cursor);
            }
        }
        catch (CacheException e) {
            assertTrue("Unexpected exception: " + e.getMessage(),
                cacheName.equalsIgnoreCase(PERSISTENT_CACHE_NAME) && !ignoreStaticConfig());
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param fieldName Field name to drop.
     */
    private void dropColumn(Ignite ignite, String cacheName, String fieldName) {
        SqlFieldsQuery q = new SqlFieldsQuery("ALTER TABLE " + cacheName + "." + TABLE_NAME + " " +
            "DROP COLUMN " + fieldName);

        try {
            try (QueryCursor<List<?>> cursor = ignite.cache(cacheName).query(q)) {
                consume(cursor);
            }
        }
        catch (CacheException e) {
            assertTrue("Unexpected exception: " + e.getMessage(),
                cacheName.equals(PERSISTENT_CACHE_NAME) && !ignoreStaticConfig());
        }
    }

    /**
     * @param cur Cursor to consume.
     */
    private void consume(QueryCursor<List<?>> cur) {
        int rows = 0;

        for (List<?> ignore : cur)
            rows++;

        info(rows + " rows processed");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration();

        dataStorageConfiguration
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setName(PERSISTENT_REGION_NAME)
                    .setInitialSize(100L * 1024 * 1024)
                    .setMaxSize(1024L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setDataRegionConfigurations(
                new DataRegionConfiguration()
                    .setName(MEMORY_REGION_NAME)
                    .setInitialSize(100L * 1024 * 1024)
                    .setMaxSize(1024L * 1024 * 1024)
                    .setPersistenceEnabled(false));

        dataStorageConfiguration.setCheckpointFrequency(5000);

        return cfg.setCacheConfiguration(
                getCacheConfig(PERSISTENT_CACHE_NAME, PERSISTENT_REGION_NAME),
                getCacheConfig(MEMORY_CACHE_NAME, MEMORY_REGION_NAME))
            .setDataStorageConfiguration(dataStorageConfiguration);
    }

    /**
     * @return Static cache configuration.
     */
    private CacheConfiguration getCacheConfig(String cacheName, String regionName) {
        Set<String> keyFields = new HashSet<>(Collections.singletonList("id"));

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", Integer.class.getName());
        fields.put("name", String.class.getName());
        fields.put("field_to_drop", String.class.getName());
        fields.put("some_field", String.class.getName());

        QueryEntity qryEntity = new QueryEntity()
            .setTableName(TABLE_NAME)
            .setKeyType("CUSTOM_SQL_KEY_TYPE") // Replace by Integer to reproduce "Key is missing from query"
            .setValueType("CUSTOM_SQL_VALUE_TYPE")
            .setKeyFields(keyFields)
            .setFields(fields);

        return new CacheConfiguration(cacheName)
            .setQueryEntities(Collections.singletonList(qryEntity))
            .setDataRegionName(regionName);
    }
}
