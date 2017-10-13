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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Simple benchmark for JDBC v2, JDBC thin & native SQL.
 */
public class JdbcSimpleBenchmark {
    /** JDBC connection URL. */
    private static String jdbcUrl;

    /** Ignite config path. */
    private static String cfgPath;

    /** Items count. */
    private static long itemsCnt;

    /** Warmup time. */
    private static int warmup;

    /** Benchmark duration. */
    private static int duration;

    /** Random. */
    private static ThreadLocalRandom rnd = ThreadLocalRandom.current();

    /**
     * @param args Arguments.
     * @throws SQLException On error.
     */
    public static void main(String [] args) throws SQLException {
        cfgPath = System.getProperty("cfg", "/home/tledkov/work/jdbc.bm/default-config.srv.xml");
        jdbcUrl = System.getProperty("jdbcUrl");

        itemsCnt = Long.getLong("items", 1000000);
        warmup = Integer.getInteger("warmup", 30);
        duration = Integer.getInteger("duration", 300);

        if (F.isEmpty(jdbcUrl))
            nativeSqlBenchmark();
        else
            jdbcBenchmark();
    }

    /**
     * @throws SQLException On error.
     */
    private static void jdbcBenchmark() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.setSchema("PUBLIC");

            fillDataJdbc(conn);

            System.out.println("Warmup...");

            jdbcSelect(conn, warmup);

            double res = jdbcSelect(conn, duration);

            System.out.println("Ops/sec: " + res);
        }
    }

    /**
     * @param conn JDBC connection.
     * @param dur Duration.
     * @return Ops/sec.
     * @throws SQLException On error.
     */
    private static double jdbcSelect(Connection conn, int dur) throws SQLException {
        long t0 = System.currentTimeMillis();
        long tEnd = t0 + dur * 1000;
        long id;

        long ops = 0;

        try (PreparedStatement pstmt = conn.prepareStatement("select val from test_long where id = ?")) {
            while (System.currentTimeMillis() < tEnd) {
                id = rnd.nextLong(itemsCnt);

                pstmt.setLong(1, id);

                ResultSet rs = pstmt.executeQuery();

                rs.next();

                if (rs.getLong(1) != id + 1)
                    throw new RuntimeException("Invalid results");

                if (ops % 10000 == 0)
                    System.out.println("Select " + ops);

                ops++;
            }
        }

        return (double)ops / (System.currentTimeMillis() - t0) * 1000;
    }

    /**
     * @param conn JDBC connection.
     * @throws SQLException On error.
     */
    private static void fillDataJdbc(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE test_long (id long primary key, val long)");

            long t0 = System.currentTimeMillis();
            try (PreparedStatement pstmt = conn.prepareStatement("insert into test_long (id, val) values (?, ?)")) {
                for (long l = 0; l < itemsCnt; ++l) {
                    pstmt.setLong(1, l);
                    pstmt.setLong(2, l + 1);

                    pstmt.executeUpdate();

                    if (l % 10000 == 0)
                        System.out.println("Insert " + l);
                }
            }

            System.out.println("Fill data insert/sec: " + ((double)itemsCnt / (System.currentTimeMillis() - t0) * 1000));
        }
    }

    /**
     */
    private static void nativeSqlBenchmark() {
        Ignition.setClientMode(true);

        try (IgniteEx ignCli = (IgniteEx)Ignition.start(cfgPath)) {
            fillDataNativeSql(ignCli);

            System.out.println("Warmup...");

            nativeSqlSelect(ignCli, warmup);

            System.out.println("Benchmark...");
            double res = nativeSqlSelect(ignCli, duration);

            System.out.println("Ops/sec: " + res);
        }
    }

    /**
     * @param ignCli Ignite.
     */
    private static void fillDataNativeSql(IgniteEx ignCli) {
        ignCli.context().query().querySqlFieldsNoCache(
            new SqlFieldsQuery("CREATE TABLE test_long (id long primary key, val long)"), true);

        long t0 = System.currentTimeMillis();

        for (long l = 0; l < itemsCnt; ++l) {
            ignCli.context().query().querySqlFieldsNoCache(
                new SqlFieldsQuery("insert into test_long (id, val) values (?, ?)")
                    .setArgs(l, l + 1), true);

            if (l % 10000 == 0)
                System.out.println("Insert " + l);
        }

        System.out.println("Fill data insert/sec: " + ((double)itemsCnt / (System.currentTimeMillis() - t0) * 1000));
    }

    /**
     * @param ignCli Ignite.
     * @param dur Duration.
     * @return Ops/sec.
     */
    private static double nativeSqlSelect(IgniteEx ignCli, int dur) {
        long t0 = System.currentTimeMillis();
        long tEnd = t0 + dur * 1000;
        long id;

        long ops = 0;

        while (System.currentTimeMillis() < tEnd) {
            id = rnd.nextLong(itemsCnt);

            List<List<?>> res = ignCli.context().query().querySqlFieldsNoCache(
                new SqlFieldsQuery("select val from test_long where id = ?")
                    .setArgs(id), false).getAll();

            if ((Long)(res.get(0).get(0)) != id + 1)
                throw new RuntimeException("Invalid results");

            if (ops % 10000 == 0)
                System.out.println("Select " + ops);

            ops++;
        }

        return (double)ops / (System.currentTimeMillis() - t0) * 1000;
    }
}