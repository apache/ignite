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

package org.apache.ignite.internal.benchmarks.jmh.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import org.openjdk.jmh.annotations.Benchmark;

/** SQL queries and DML through the JDBC thin driver, with statements prepared in setup. */
public class JmhJdbcThinAllocationBenchmark extends JmhThinClientAllocationAbstractBenchmark {
    /** Rows of a range select. */
    private static final int RANGE = 10;

    /** JDBC connection. */
    private Connection conn;

    /** Select by key. */
    private PreparedStatement select;

    /** Select of {@link #RANGE} rows by the indexed field. */
    private PreparedStatement rangeSelect;

    /** Update of the indexed field. */
    private PreparedStatement update;

    /** Insert of a row. */
    private PreparedStatement insert;

    /** Delete by key. */
    private PreparedStatement delete;

    /** Arguments of {@link #insert} for each key the setup doesn't write. */
    private Object[][] insertArgs;

    /** Index of the next row in {@link #insertArgs}. */
    private int newKeyIdx;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        String table = valType.valueClass().getSimpleName();

        select = conn.prepareStatement("SELECT * FROM " + table + " WHERE _key = ?");
        rangeSelect = conn.prepareStatement("SELECT * FROM " + table + " WHERE id BETWEEN ? AND ?");
        update = conn.prepareStatement("UPDATE " + table + " SET id = ? WHERE _key = ?");
        insert = conn.prepareStatement(valType.insertSql());
        delete = conn.prepareStatement("DELETE FROM " + table + " WHERE _key = ?");

        insertArgs = new Object[CNT][];

        for (int i = 0; i < CNT; i++)
            insertArgs[i] = valType.insertArgs(CNT + i);
    }

    /** {@inheritDoc} */
    @Override protected AutoCloseable connect() throws SQLException {
        conn = DriverManager.getConnection("jdbc:ignite:thin://" + String.join(",", addresses()) +
            "?partitionAwareness=" + partitionAwareness);

        return conn;
    }

    /** @return Number of rows a select by key reads: one. */
    @Benchmark
    public int selectByKey() throws SQLException {
        select.setObject(1, keys[ThreadLocalRandom.current().nextInt(CNT)]);

        return read(select);
    }

    /** @return Number of rows a select by a range of the indexed field reads: {@link #RANGE}. */
    @Benchmark
    public int selectRange() throws SQLException {
        int first = ThreadLocalRandom.current().nextInt(CNT - RANGE + 1);

        rangeSelect.setObject(1, keys[first]);
        rangeSelect.setObject(2, keys[first + RANGE - 1]);

        return read(rangeSelect);
    }

    /**
     * Sets the indexed field of a key to the value it has.
     *
     * @return Number of rows updated: one.
     */
    @Benchmark
    public int update() throws SQLException {
        Integer key = keys[ThreadLocalRandom.current().nextInt(CNT)];

        update.setObject(1, key);
        update.setObject(2, key);

        return update.executeUpdate();
    }

    /** Inserts a row of a key the setup didn't write and deletes it, so the table keeps its size. */
    @Benchmark
    public void insertDelete() throws SQLException {
        Object[] args = insertArgs[newKeyIdx++ % CNT];

        for (int i = 0; i < args.length; i++)
            insert.setObject(i + 1, args[i]);

        insert.executeUpdate();

        delete.setObject(1, args[0]);
        delete.executeUpdate();
    }

    /** @return Number of rows the statement returns. */
    private static int read(PreparedStatement stmt) throws SQLException {
        int cnt = 0;

        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next())
                cnt++;
        }

        return cnt;
    }
}
