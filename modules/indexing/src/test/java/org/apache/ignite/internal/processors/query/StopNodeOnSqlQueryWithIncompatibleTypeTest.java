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

package org.apache.ignite.internal.processors.query;

import java.time.Instant;
import java.util.List;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests for exception on incompatible types to compare.
 * Node mustn't be stopped.
 */
public class StopNodeOnSqlQueryWithIncompatibleTypeTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test local query execution.
     */
    public void test() {
        sql("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL_OBJ OTHER)");
        sql("CREATE INDEX TEST_VAL_OBJ ON TEST(VAL_OBJ)");
        sql("INSERT INTO TEST VALUES (0, ?)", Instant.now());

        GridTestUtils.assertThrows(log, () -> {
                sql("SELECT * FROM TEST WHERE VAL_OBJ < CURRENT_TIMESTAMP()").getAll();

                return null;
            }, CacheException.class, null);

        GridTestUtils.assertThrows(log, () -> {
            sql("SELECT * FROM TEST WHERE ID < CURRENT_TIMESTAMP()").getAll();

            return null;
        }, CacheException.class, null);

        assertFalse(grid().context().isStopping());
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object ... args) {
        return sql(grid(), sql, args);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object ... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }
}
