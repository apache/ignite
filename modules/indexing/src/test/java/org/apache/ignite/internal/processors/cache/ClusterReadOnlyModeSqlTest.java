/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Random;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.checkThatRootCauseIsReadOnly;

/**
 * Tests SQL queries in read-only cluster mode.
 */
public class ClusterReadOnlyModeSqlTest extends ClusterReadOnlyModeAbstractTest {
    /**
     *
     */
    @Test
    public void testSqlReadOnly() {
        assertSqlReadOnlyMode(false);

        changeClusterReadOnlyMode(true);

        assertSqlReadOnlyMode(true);

        changeClusterReadOnlyMode(false);

        assertSqlReadOnlyMode(false);
    }

    /**
     * @param readOnly If {@code true} then data modification SQL queries must fail, else succeed.
     */
    private void assertSqlReadOnlyMode(boolean readOnly) {
        Random rnd = new Random();

        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

                try (FieldsQueryCursor<?> cur = cache.query(new SqlFieldsQuery("SELECT * FROM Integer"))) {
                    cur.getAll();
                }

                Throwable failed = null;

                try (FieldsQueryCursor<?> cur = cache.query(new SqlFieldsQuery("DELETE FROM Integer"))) {
                    cur.getAll();
                }
                catch (CacheException ex) {
                    if (!readOnly)
                        log.error("Failed to delete data", ex);

                    failed = ex;
                }

                if ((failed == null) == readOnly)
                    fail("SQL delete from " + cacheName + " must " + (readOnly ? "fail" : "succeed"));

                checkThatRootCauseIsReadOnly(failed);

                failed = null;

                try (FieldsQueryCursor<?> cur = cache.query(new SqlFieldsQuery(
                    "INSERT INTO Integer(_KEY, _VAL) VALUES (?, ?)").setArgs(rnd.nextInt(1000), rnd.nextInt()))) {
                    cur.getAll();
                }
                catch (CacheException ex) {
                    if (!readOnly)
                        log.error("Failed to insert data", ex);

                    failed = ex;
                }

                if ((failed == null) == readOnly)
                    fail("SQL insert into " + cacheName + " must " + (readOnly ? "fail" : "succeed"));

                checkThatRootCauseIsReadOnly(failed);
            }
        }
    }
}
