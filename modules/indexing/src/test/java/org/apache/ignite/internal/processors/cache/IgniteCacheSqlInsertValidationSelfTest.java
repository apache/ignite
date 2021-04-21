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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlanBuilder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.TransactionDuplicateKeyException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for validation of inserts sql queries.
 */
public class IgniteCacheSqlInsertValidationSelfTest extends AbstractIndexingCommonTest {
    /** Entry point for sql api. Contains table configurations too. */
    private static IgniteCache<Object, Object> cache;

    /** Default value for fk2 field of WITH_KEY_FLDS table. */
    private static final Long DEFAULT_FK2_VAL = 42L;

    /** Default value for fk2 field of WITH_KEY_FLDS table. */
    private static final Long DEFAULT_FK1_VAL = null;

    /** Old allow value. */
    private static boolean oldAllowColumnsVal;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        oldAllowColumnsVal = GridTestUtils.getFieldValue(UpdatePlanBuilder.class, UpdatePlanBuilder.class,
            "ALLOW_KEY_VAL_UPDATES");

        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", true);

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        GridTestUtils.setFieldValue(UpdatePlanBuilder.class, "ALLOW_KEY_VAL_UPDATES", oldAllowColumnsVal);

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        Map<String, Object> defsFK2 = new HashMap<>();
        defsFK2.put("fk2", DEFAULT_FK2_VAL);

        cache = jcache(grid(0), defaultCacheConfiguration()
                .setName("testCache")
                .setQueryEntities(Arrays.asList(
                    new QueryEntity(Key.class.getName(), Val.class.getName())
                        .addQueryField("fk1", "java.lang.Long", null)
                        .addQueryField("fk2", "java.lang.Long", null)
                        .addQueryField("fv1", "java.lang.Long", null)
                        .addQueryField("fv2", "java.lang.Long", null)
                        .setTableName("FORGOTTEN_KEY_FLDS"),
                    new QueryEntity(Key.class.getName(), Integer.class.getName())
                        .addQueryField("fk1", "java.lang.Long", null)
                        .addQueryField("fk2", "java.lang.Long", null)
                        .setDefaultFieldValues(defsFK2)
                        .setKeyFields(new HashSet<>(Arrays.asList("fk1", "fk2")))
                        .setTableName("WITH_KEY_FLDS"),
                    new QueryEntity(Integer.class.getName(), Val2.class.getName())
                        .addQueryField("fv1", "java.lang.Long", null)
                        .addQueryField("fv2", "java.lang.Long", null)
                        .setTableName("INT_KEY_TAB"),
                    new QueryEntity(SuperKey.class, String.class)
                        .setTableName("SUPER_TAB")
                )), "testCache");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (cache != null)
            cache.destroy();
    }

    /**
     * Check that if we cannot insert row using sql due to we don't have keyFields in the configuration, we are still
     * able to put using cache api.
     */
    @Test
    public void testCacheApiIsStillAllowed() {
        cache.put(new Key(1, 2), new Val(3, 4));

        assertNotNull("Expected cache to contain object ", cache.get(new Key(1, 2)));
    }

    /**
     * Check that we are able to perform sql insert using special "_key" field. Even in case of non sql key.
     */
    @Test
    public void testInsertDefaultKeyName() {
        Object cnt = execute("INSERT INTO INT_KEY_TAB (_key, fv1, fv2) VALUES (1 , 2 , 3)").get(0).get(0);

        assertEquals("Expected one row successfully inserted ", 1L, cnt);
    }

    /**
     * Check forgotten key fields. If we've forgotten to specify key fields and we don't specify _key, then default key
     * is inserted.
     */
    @Test
    public void testIncorrectComplex() {
        execute("INSERT INTO FORGOTTEN_KEY_FLDS(FK1, FK2, FV1, FV2) VALUES (2,3,4,5)");

        GridTestUtils.assertThrows(log(),
            () -> execute("INSERT INTO FORGOTTEN_KEY_FLDS(FK1, FK2, FV1, FV2) VALUES (8,9,10,11)"),
            TransactionDuplicateKeyException.class,
            "Duplicate key during INSERT");
    }

    /**
     * Check that we can specify only one pk column (out of two). Second one should be of default value for type;
     */
    @Test
    public void testNotAllKeyColsComplex() {
        execute("INSERT INTO WITH_KEY_FLDS(FK1, _val) VALUES (7, 1)"); // Missing FK2 -> (7, 42, 1)
        execute("INSERT INTO WITH_KEY_FLDS(FK2, _val) VALUES (15, 2)"); // Missing FK1 -> (null, 15, 2)

        Long fk2 = (Long)execute("SELECT FK2 FROM WITH_KEY_FLDS WHERE _val = 1").get(0).get(0);
        Long fk1 = (Long)execute("SELECT FK1 FROM WITH_KEY_FLDS WHERE _val = 2").get(0).get(0);

        assertEquals(DEFAULT_FK2_VAL, fk2);
        assertEquals(DEFAULT_FK1_VAL, fk1);
    }

    /**
     * Check that we can't perform insert without at least one key field specified.
     */
    @Test
    public void testMixedPlaceholderWithOtherKeyFields() {
        GridTestUtils.assertThrows(log(),
            () -> execute("INSERT INTO WITH_KEY_FLDS(_key, FK1, _val) VALUES (?, ?, ?)",
                new Key(1, 2), 42, 43),
            IgniteSQLException.class,
            "Column _KEY refers to entire key cache object.");
    }

    /**
     * Check that key can contain nested field with its own fields. Check that we can insert mixing sql and non sql
     * values.
     */
    @Test
    public void testSuperKey() {
        execute("INSERT INTO SUPER_TAB (SUPERKEYID, NESTEDKEY, _val) VALUES (?, ?, ?)",
            123, new NestedKey("the name "), "the _val value");
    }

    /**
     * Check that key can contain nested field with its own fields. Check that we can insert using _key placeholder.
     */
    @Test
    public void testSuperKeyNative() {
        execute("INSERT INTO SUPER_TAB (_key, _val) VALUES (?, ?)",
            new SuperKey(1, new NestedKey("the name")),
            "_val value");
    }

    /**
     * Check we can amend fields list part.
     */
    @Test
    public void testInsertImplicitAllFields() {
        execute("CREATE TABLE PUBLIC.IMPLICIT_INS (id1 BIGINT, id2 BIGINT, val BIGINT, PRIMARY KEY(id1, id2))");

        execute("INSERT INTO PUBLIC.IMPLICIT_INS VALUES (1,2,3)");
    }

    /**
     * Execute native sql.
     *
     * @param sql query.
     * @param args arguments of SqlFieldsQuery.
     * @return {@link QueryCursor#getAll()} - result of the query.
     */
    private List<List<?>> execute(String sql, Object... args) {
        return cache.query(new SqlFieldsQuery(sql).setArgs(args)).getAll();
    }

    /**
     * @param qryEntity Query entity.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(QueryEntity qryEntity) {
        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setQueryEntities(Collections.singletonList(qryEntity));

        return cache;
    }

    private static class Key {
        private long fk1;

        private long fk2;

        public Key(long fk1, long fk2) {
            this.fk1 = fk1;
            this.fk2 = fk2;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Key key = (Key)o;
            return fk1 == key.fk1 &&
                fk2 == key.fk2;
        }

        @Override public int hashCode() {
            return Objects.hash(fk1, fk2);
        }
    }

    private static class SuperKey {
        @QuerySqlField
        private long superKeyId;

        @QuerySqlField
        private NestedKey nestedKey;

        public SuperKey(long superKeyId, NestedKey nestedKey) {
            this.superKeyId = superKeyId;
            this.nestedKey = nestedKey;
        }
    }

    private static class NestedKey {
        @QuerySqlField
        private String name;

        public NestedKey(String name) {
            this.name = name;
        }
    }

    private static class Val {
        private long fv1;

        private long fv2;

        public Val(long fv1, long fv2) {
            this.fv1 = fv1;
            this.fv2 = fv2;
        }
    }

    private static class Val2 {
        private long fv1;

        private long fv2;

        public Val2(long fv1, long fv2) {
            this.fv1 = fv1;
            this.fv2 = fv2;
        }
    }
}
