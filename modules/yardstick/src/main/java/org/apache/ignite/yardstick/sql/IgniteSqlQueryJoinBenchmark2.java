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

package org.apache.ignite.yardstick.sql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Second version of Ignite benchmark that performs query operations with joins.
 * It runs queries that aim to check subquery rewriting optimization performance.
 */
public class IgniteSqlQueryJoinBenchmark2 extends IgniteAbstractBenchmark {
    /** Enumeration of subqueries that used int this benchamrk. */
    private enum Subquery {
        /** Query with uncorrelated subselect. */
        SELECT_EXPR_UNCORRELATED(
            "select p.name, p.id, (select name from organization where id = DAY_OF_WEEK(NOW())) as org_name" +
                " from person p where p.id >= ? and p.id < ?"
        ),

        /** Query with correlated subselect. */
        SELECT_EXPR_CORRELATED(
            "select p.name, p.id, (select name from organization where id = org_id) as org_name" +
                " from person p where p.id >= ? and p.id < ?"
        ),

        /** Query with subselect in talbe list. */
        TABLE_LIST(
            "select p.id, p.name, o.name from person p, (select id, name from organization" +
                " where id >= ? and id < ?) o where p.org_id = o.id"
        ),

        /** Query with uncorrelated subselect within IN clause. */
        IN_EXPR_UNCORRELATED(
            "select p.id, p.name from person p where p.org_id in (select id from organization" +
                " where id >= ? and id < ?)"
        ),

        /** Query with correlated subselect within IN clause. */
        IN_EXPR_CORRELATED(
            "select p.id, p.name from person p where p.org_id in (select id from organization" +
                " where id = p.id) and p.id >= ? and p.id < ?"
        ),

        /** Query with subselect within EXISTS clause. */
        EXISTS_EXPR(
            "select p.id, p.name from person p where EXISTS (select 1 from organization" +
                " where id = p.org_id and id >= ? and id < ?)"
        ),

        /** Query with subselect from small table. Should verify table reordering optimization. */
        OUTER_IS_BIG_AND_INNER_IS_SMALL(
            "select p.id, p.name, c.color, ? as p1, ? as p2 from person p, (select id, color from color) c" +
                " where c.id = (MOD(p.id, 7) + 1) limit 10"
        ),

        /** Query with subselect from small table. Should verify single-partition optimization. */
        SINGLE_PARTITION_OPTIMIZATION(
            "select p.id, p.name from person p where EXISTS (select 1 from organization" +
                " where id = ? and id = p.org_id) and ? > -1"
        );

        /** */
        private static final Map<String, Subquery> values = new HashMap<>();

        static {
            for (Subquery s : Subquery.values())
                values.put(s.name(), s);
        }

        /** */
        private final String sql;

        /**
         * @param sql Sql.
         */
        Subquery(String sql) {
            this.sql = sql;
        }

        /**
         * @return String representing SQL query.
         */
        String sql() {
            return sql;
        }

        /**
         * @param name Name.
         * @return Enum value or {@code null} if there is no value with given name.
         */
        static Subquery valueOfSafe(String name) {
            return values.get(name);
        }
    }

    /** */
    private static final long TBL_SIZE = 100_000;

    /** */
    private static final String SUBQUERY_PARAM = "subquery";

    /** */
    private static final String SKIP_INIT_PARAM = "skipInit";

    /** */
    private static final String LAST_PARAM_KEY = "LAST_PARAM_KEY";

    /** */
    private Subquery subqry;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        String subqueryName = args.getStringParameter(SUBQUERY_PARAM, null);

        if (!F.isEmpty(subqueryName))
            subqry = Subquery.valueOfSafe(subqueryName.trim().toUpperCase());

        if (subqry == null)
            throw new IllegalArgumentException("The \"" + SUBQUERY_PARAM + "\" is mandatory" +
                " and should be one of the " + Arrays.toString(Subquery.values()));

        if (args.getBooleanParameter(SKIP_INIT_PARAM, false))
            return;

        IgniteSemaphore sem = ignite().semaphore("setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create tables...");

                init();
            }
            else {
                println(cfg, "Waits for setup...");

                sem.acquire();
            }
        }
        finally {
            sem.release();
        }

        println(cfg, "Done");
    }

    /** */
    private void init() {
        sql("create table color(id long primary key, color varchar) with \"template=replicated\"");

        long id = 0;
        for (String color : Arrays.asList("red", "orange", "yellow", "green", "blue", "indigo", "violet"))
            sql("insert into color(id, color) values(?, ?)", ++id, color);

        sql("create table organization(id long primary key, name varchar)");
        sql("create table person(id long, org_id long, name varchar, primary key (id, org_id))" +
            " with \"affinity_key=org_id\"");

        for (long i = 0; i < TBL_SIZE; i++) {
            sql("insert into organization(id, name) values (?, ?)", i, "org" + i);
            sql("insert into person(id, org_id, name) values (?, ?, ?)", i, i, "person" + i);

            if (i % 1000 == 0 && i != 0)
                println(i + " entries inserted");
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long lowerBound = (long)ctx.getOrDefault(LAST_PARAM_KEY, 0L);

        long upperBound = lowerBound + 10;

        List<List<?>> rows = sql(subqry.sql(), lowerBound, upperBound);

        if (rows.isEmpty())
            throw new Exception("Empty result for query=\"" + subqry.sql() + "\"," +
                " param=[" + lowerBound + ", " + upperBound + "]");

        long newLowerBound = lowerBound + 5;

        if (newLowerBound + 10 > TBL_SIZE)
            newLowerBound = 0;

        ctx.put(LAST_PARAM_KEY, newLowerBound);

        return true;
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private List<List<?>> sql(String sql, Object... args) {
        return ((IgniteEx)ignite()).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setSchema("PUBLIC")
            .setArgs(args), false)
            .getAll();
    }
}
