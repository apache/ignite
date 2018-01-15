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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.yardstickframework.BenchmarkConfiguration;

import static java.lang.String.format;

public class JdbcSqlUpdateBenchmark extends AbstractJdbcBenchmark {
    public static final String TABLE = "test_long";

    private final ThreadLocal<PreparedStatement> singleUpdate = newStatement(format("UPDATE %s SET val = (val + 1) WHERE id = ?", TABLE));
    private final ThreadLocal<PreparedStatement> rangeUpdate = newStatement(format("UPDATE %s SET val = (val + 1) WHERE id BETWEEN ? AND ?", TABLE));

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        NativeSqlQueryRangeBenchmark.fillData(cfg, (IgniteEx)ignite(), args.range());

        ignite().close();
    }

    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        PreparedStatement update;
        int expectedResSize;

        if (args.sqlRange() == 1){
            long updateKey = rnd.nextLong(args.range()) + 1;
            update = singleUpdate.get();
            update.setLong(1, updateKey);
            expectedResSize = 1;
        }
        else {
            long id = rnd.nextLong(args.range() - args.sqlRange()) + 1;
            long maxId = id + args.sqlRange() - 1;

            update = rangeUpdate.get();
            update.setLong(1, id);
            update.setLong(2, maxId);

            expectedResSize = args.sqlRange();
        }

        int actualResSize = update.executeUpdate();

        if (actualResSize != expectedResSize)
            throw new Exception("Invalid result set size [expected=" + actualResSize + ", expected=" + expectedResSize + ']');

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
