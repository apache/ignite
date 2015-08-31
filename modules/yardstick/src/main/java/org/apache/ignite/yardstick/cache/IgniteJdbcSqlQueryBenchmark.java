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

package org.apache.ignite.yardstick.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.yardstick.cache.model.Person;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations.
 */
public class IgniteJdbcSqlQueryBenchmark extends IgniteCacheAbstractBenchmark {
    /** Statements for closing. */
    Set<PreparedStatement> stms = Collections.synchronizedSet(new HashSet<PreparedStatement>());

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(cfg, "Populating query data...");

        long start = System.nanoTime();

        try (IgniteDataStreamer<Integer, Person> dataLdr = ignite().dataStreamer(cache.getName())) {
            for (int i = 0; i < args.range() && !Thread.currentThread().isInterrupted(); i++) {
                dataLdr.addData(i, new Person(i, "firstName" + i, "lastName" + i, i * 1000));

                if (i % 100000 == 0)
                    println(cfg, "Populated persons: " + i);
            }
        }

        println(cfg, "Finished populating query data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        PreparedStatement stm = (PreparedStatement)ctx.get(0);

        if (stm == null) {
            stm = createStatement();

            stms.add(stm);

            ctx.put(0, stm);
        }

        double salary = ThreadLocalRandom.current().nextDouble() * args.range() * 1000;

        double maxSalary = salary + 1000;

        stm.clearParameters();

        stm.setDouble(1, salary);
        stm.setDouble(2, maxSalary);

        ResultSet rs = stm.executeQuery();

        while (rs.next()) {
            double sal = rs.getDouble("salary");

            if (sal < salary || sal > maxSalary)
                throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary + ']');
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        for (PreparedStatement stm : stms) {
            try {
                stm.getConnection().close();

                stm.close();
            }
            catch (Exception ignore) {
                println("Failed to close connection." + stm);
            }
        }

        super.tearDown();
    }

    /**
     * @return Prepared statement.
     * @throws Exception
     */
    private PreparedStatement createStatement() throws Exception {
        Class.forName("org.apache.ignite.IgniteJdbcDriver");

        Connection conn = null;

        try {
            conn = DriverManager.getConnection(args.jdbcUrl());

            return conn.prepareStatement("select * from Person where salary >= ? and salary <= ?");
        }
        catch (Exception e) {
            if (conn != null)
                conn.close();

            throw new IgniteException("Failed to create prepare statement.", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("query");
    }
}