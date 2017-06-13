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

package org.apache.ignite.jdbc.thin.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Base class for complex SQL tests based on JDBC driver.
 */
public class JdbcThinSqlCreateSelectTest extends JdbcThinAbstractSqlTest {
    /** Cache mode to test with. */
    private final CacheMode cacheMode = CacheMode.PARTITIONED;

    /** Cache atomicity mode to test with. */
    private final CacheAtomicityMode atomicityMode = CacheAtomicityMode.ATOMIC;

    /** Names of companies to use. */
    private static final List<String> COMPANIES = Arrays.asList("ASF", "GNU", "BSD");

    /** Cities to use. */
    private static final List<String> CITIES = Arrays.asList("St. Petersburg", "Boston", "Berkeley", "London");

    /**
     * @throws Exception If failed.
     */
    public void testCreateSelect() throws Exception {
        GridTestUtils.assertThrows(null, new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                sql(new ResultChecker(new Object[][] {}), "SELECT * from Person");

                return null;
            }
        }, SQLException.class, "Failed to parse query: SELECT * from Person");

        sql(new UpdateChecker(0),
            "CREATE TABLE person (id int, name varchar, age int, company varchar, city varchar, " +
                "primary key (id, name, city)) WITH \"template=" + cacheMode.name() + ",atomicity=" + atomicityMode.name()
                + ",affinitykey=city\"");

        sql(new UpdateChecker(0), "CREATE INDEX idx on person (city asc, name asc)");

        sql(new UpdateChecker(0), "CREATE TABLE city (name varchar, population int, primary key (name)) WITH " +
            "\"template=" + cacheMode.name() + ",atomicity=" + atomicityMode.name() + ",affinitykey=name\"");

        sql(new UpdateChecker(3),
            "INSERT INTO city (name, population) values(?, ?), (?, ?), (?, ?)",
            "St. Petersburg", 6000000,
            "Boston", 2000000,
            "London", 8000000
        );

        sql(new ResultColumnChecker("id", "name", "age", "comp"),
            "SELECT id, name, age, company as comp FROM person where id < 50");

        for (int i = 0; i < 100; i++) {
            sql(new UpdateChecker(1),
                "INSERT INTO person (id, name, age, company, city) values (?, ?, ?, ?, ?)",
                i,
                "Person " + i,
                20 + (i % 10),
                COMPANIES.get(i % COMPANIES.size()),
                CITIES.get(i % CITIES.size()));
        }

        final int[] cnt = {0};

        sql(new ResultPredicateChecker(new IgnitePredicate<Object[]>() {
            @Override public boolean apply(Object[] objs) {
                int id = ((Integer)objs[0]);

                if (id >= 50)
                    return false;

                if (20 + (id % 10) != ((Integer)objs[2]))
                    return false;

                if (!("Person " + id).equals(objs[1]))
                    return false;

                ++cnt[0];

                return true;
            }
        }), "SELECT id, name, age FROM person where id < 50");

        assert cnt[0] == 50 : "Invalid rows count";

        // Berkeley is not present in City table, although 25 people have it specified as their city.
        sql(new ResultChecker(new Object[][] {{75L}}),
            "SELECT COUNT(*) from Person p inner join City c on p.city = c.name");

        sql(new UpdateChecker(34),
            "UPDATE Person SET company = 'New Company', age = CASE WHEN MOD(id, 2) <> 0 THEN age + 5 ELSE "
                + "age + 1 END WHERE company = 'ASF'");

        cnt[0] = 0;

        sql(new ResultPredicateChecker(new IgnitePredicate<Object[]>() {
            @Override public boolean apply(Object[] objs) {
                int id = ((Integer)objs[0]);
                int age = ((Integer)objs[2]);

                if (id % 2 == 0) {
                    if (age != 20 + (id % 10) + 1)
                        return false;
                }
                else {
                    if (age != 20 + (id % 10) + 5)
                        return false;
                }

                ++cnt[0];

                return true;
            }
        }), "SELECT * FROM person where company = 'New Company'");

        assert cnt[0] == 34 : "Invalid rows count";

        sql(new UpdateChecker(0), "DROP INDEX idx");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateSelectChar() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5361");

        sql(new UpdateChecker(0),
            "CREATE TABLE str_table (id int, str char, primary key (id)) WITH \"template="
                + cacheMode.name() + ",atomicity=" + atomicityMode.name() + ", affinitykey=id\"");

        String str = "a   ";
        sql(new UpdateChecker(1),
            "INSERT INTO str_table(id, str) values (?, ?)",
            1,
            str);

        sql(new ResultChecker(new Object[][] {{str.trim()}}), "SELECT str FROM str_table");
    }
}