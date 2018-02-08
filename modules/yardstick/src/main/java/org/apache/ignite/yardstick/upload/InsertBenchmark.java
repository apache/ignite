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

package org.apache.ignite.yardstick.upload;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.yardstick.jdbc.AbstractJdbcBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

public class InsertBenchmark extends AbstractJdbcBenchmark {
    /** Rows count to be inserted and deleted during warmup */
    public static final int WARMUP_ROWS_CNT = 10_000;

    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
        warmup();
    }

    /** create empty table */
    // todo: use args.range == 0 ?
    @Override protected void setupData(){
        SqlFieldsQuery qry = new SqlFieldsQuery("CREATE TABLE test_long (id LONG PRIMARY KEY, val LONG);");
        GridQueryProcessor qProc = ((IgniteEx)ignite()).context().query();
        qProc.querySqlFields(qry, false);

    }

    private void warmup() throws SQLException {
        try (PreparedStatement insert = conn.get()
            .prepareStatement("INSERT INTO test_long VALUES (?, ?)")) {
            for (int i = 1; i <= WARMUP_ROWS_CNT ; i++) {
                insert.setLong(1, i);
                insert.setLong(2, i + 1);
                insert.executeUpdate();
            }
        }

        try(PreparedStatement delete = conn.get().prepareStatement("DELETE FROM test_long WHERE id > 0")){
            // todo: Should we perform subsequent insert+delete in warmup?
            delete.executeUpdate();
        }
    }


    /** Sequence of single inserts */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int n = args.sqlRange();

        for (int i = 1; i <= n; i++) {
            try (PreparedStatement insert = conn.get().prepareStatement("INSERT INTO test_long VALUES (?, ?)")) {
                insert.setLong(1, i);
                insert.setLong(2, i + 1);
                insert.executeUpdate();
            }

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        try(PreparedStatement count = conn.get().prepareStatement("SELECT COUNT(id) FROM test_long;")){
            try (ResultSet rs = count.executeQuery()) {
                rs.next();
                long size = rs.getLong(1);

                BenchmarkUtils.println(cfg, "Test table `test_long' contains " + size + " rows");
                // todo assert size == threads * sqlRange
            }
        } catch (Exception ex){
            BenchmarkUtils.println(cfg, "Exception thrown at tear down: " + ex);
        }

        super.tearDown();
    }
}
