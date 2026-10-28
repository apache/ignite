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

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that a SQL error on a map node is visible to the client as {@link IgniteSQLException}.
 */
public class IgniteCacheDistributedSqlQueryErrorSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String OVERFLOW_ERR = "Numeric value out of range";

    /** */
    private static final String MAPPER_FAILED_MSG = "Failed to execute map query on remote node";

    /**
     * Checks that a failure of a remote map node carries the SQL error to the client.
     */
    @Test
    public void testRemoteMapperFailure() throws Exception {
        startGrid(0);
        startClientGrid(1);

        checkOverflowError(grid(1).getOrCreateCache(DEFAULT_CACHE_NAME));
    }

    /**
     * Checks that a failure of the local map node carries the SQL error to the client as well.
     */
    @Test
    public void testLocalMapperFailure() throws Exception {
        startGrid(0);

        checkOverflowError(grid(0).getOrCreateCache(DEFAULT_CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param cache Cache to run the failing query on.
     */
    private void checkOverflowError(IgniteCache<?, ?> cache) {
        execute(cache, "DROP TABLE IF EXISTS test");
        execute(cache, "CREATE TABLE test (id INT PRIMARY KEY, field2 INT)");
        execute(cache, "INSERT INTO test (id, field2) VALUES (1, 2), (2, 3)");

        try {
            // IGNITE-10424 reproducer: int multiplication overflows on the map node.
            execute(cache, "SELECT SUM(field2 * 1000000000) FROM test");

            fail("Exception is expected");
        }
        catch (CacheException e) {
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains(MAPPER_FAILED_MSG));

            IgniteSQLException sqlEx = X.cause(e, IgniteSQLException.class);

            assertNotNull("Failed to find SQL error in cause chain", sqlEx);
            assertTrue("Unexpected SQL error message: " + sqlEx.getMessage(), sqlEx.getMessage().contains(OVERFLOW_ERR));

            // H2 runtime errors are currently wrapped with the generic code; carrying richer codes is future work.
            assertEquals(IgniteQueryErrorCode.UNKNOWN, sqlEx.statusCode());
        }
    }

    /**
     * @param cache Cache to run the query on.
     * @param sql SQL statement.
     */
    private void execute(IgniteCache<?, ?> cache, String sql) {
        cache.query(new SqlFieldsQuery(sql).setSchema("PUBLIC")).getAll();
    }
}
