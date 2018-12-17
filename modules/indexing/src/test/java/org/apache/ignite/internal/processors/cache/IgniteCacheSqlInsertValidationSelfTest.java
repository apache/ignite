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
import java.util.List;
import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for validation of inserts sql queries.
 */
public class IgniteCacheSqlInsertValidationSelfTest extends GridCommonAbstractTest {
    /** Entry point for sql api. Contains table configurations too. */
    private static IgniteCache<Object, Object> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (false)
            return;

        cache = jcache(grid(0), defaultCacheConfiguration()
                .setName("cacheComplex")
                .setQueryEntities(Arrays.asList(
                    new QueryEntity(Key.class.getName(), Val.class.getName())
                        .addQueryField("fk1", "java.lang.Long", null)
                        .addQueryField("fk2", "java.lang.Long", null)
                        .addQueryField("fv1", "java.lang.Long", null)
                        .addQueryField("fv2", "java.lang.Long", null)
                        .setTableName("VAL"),
                    new QueryEntity(Integer.class.getName(), Val2.class.getName())
                        .addQueryField("fv1", "java.lang.Long", null)
                        .addQueryField("fv2", "java.lang.Long", null)
                        .setTableName("INT_KEY_TAB")
                ))
            , "cacheComplex");
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
    public void testCacheApiIsStillAllowed() {
        cache.put(new Key(1, 2), new Val(3, 4));

        assertNotNull("Expected cache to contain object ", cache.get(new Key(1, 2)));
    }

    /**
     * Check that we are able to perform sql insert using special "_key" field.
     */
    public void testInsertDefaultKeyName() {
        Object cnt = execute("INSERT INTO INT_KEY_TAB (_key, fv1, fv2) VALUES (1 , 2 , 3)").get(0).get(0);

        assertEquals("Expected one row successfully inserted ", 1L, cnt);
    }

    public void testIncorrectComplex() {
        GridTestUtils.assertThrows(log(),
            () -> execute("INSERT INTO VAL(FK1, FK2, FV1, FV2) VALUES (2,3,4,5)"),
            IgniteSQLException.class,
            "Query is expected to contain all primary key columns");
    }

    private List<List<?>> execute(String sql) {
        return cache.query(new SqlFieldsQuery(sql)).getAll();
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