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

package org.apache.ignite.internal.processors.query.calcite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/** */
public class JdbcLocalFlagTest extends AbstractJdbcTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testLocalFlag() throws Exception {
        IgniteEx ignite = startGrids(3);

        int keys = 100;

        sql(ignite, "CREATE TABLE test(id INT PRIMARY KEY, val VARCHAR)");

        for (int i = 0; i < keys; i++)
            sql(ignite, "INSERT INTO test(id, val) VALUES (?, ?)", i, "val" + i);

        try (Connection conn = DriverManager.getConnection(URL)) {
            List<List<Object>> res = executeQuery(conn, "SELECT * FROM test");

            assertEquals(keys, res.size());
        }

        try (Connection conn = DriverManager.getConnection(URL + "?local=true")) {
            List<List<Object>> res = executeQuery(conn, "SELECT * FROM test");

            assertTrue(keys > res.size());
        }
    }

    /** */
    private List<List<?>> sql(IgniteEx ignite, String sql, Object... args) {
        return ignite.context().query().querySqlFields(new SqlFieldsQuery(sql).setArgs(args), false).getAll();
    }
}
