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

package org.apache.ignite.yardstick.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

import static java.lang.String.format;

public class JdbcSqlInsertBenchmark extends AbstractJdbcBenchmark {
    public static final String TABLE = "test_long";

    private final ThreadLocal<PreparedStatement> singleInsert = newStatement(format("INSERT INTO %s (id, val) VALUES (?, ?)", TABLE));
    private final ThreadLocal<PreparedStatement> singleDelete = newStatement(format("DELETE FROM %s WHERE id = ?", TABLE));


    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        NativeSqlQueryRangeBenchmark.fillData(cfg, (IgniteEx)ignite(), args.range());

        ignite().close();
    }

    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int expectedResSize = 1;

        long insertKey = rnd.nextLong(args.range()) + 1 + args.range();
        long insertVal = insertKey + 1;

        PreparedStatement insert = singleInsert.get();
        insert.setLong(1, insertKey);
        insert.setLong(2, insertVal);

        PreparedStatement delete = singleDelete.get();
        delete.setLong(1, insertKey);

        try {
            insert.executeUpdate();
            delete.executeUpdate();
        }
        catch (Exception ign){
            // collision occurred, ignoring
            BenchmarkUtils.println(cfg, "Collision occured. We're sorry, really :(");
        }

        return true;
    }

    private ThreadLocal<PreparedStatement> newStatement(final String sql){
        return new ThreadLocal<PreparedStatement>(){
            @Override protected PreparedStatement initialValue() {
                try {
                    return conn.get().prepareStatement(sql);
                }
                catch (SQLException e) {
                    throw new IgniteException(e);
                }
            }
        };
    }
}
