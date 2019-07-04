/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.jdbc;

import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.yardstickframework.BenchmarkConfiguration;


/**
 * Native sql benchmark that performs select operations with DISTINCT and GROUP BY.
 */
public class NativeSqlQueryDistinctWithGroupByBenchmark extends AbstractNativeBenchmark {
    private final String selectQry = "SELECT DISTINCT val FROM test_long GROUP BY val, id";

    private int expRowsCnt;

    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        expRowsCnt = args.range();
    }

    /**
     * Benchmarked action that performs selects and validates results.
     *
     * {@inheritDoc}
     */

    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery(selectQry);

        try (FieldsQueryCursor<List<?>> res = ((IgniteEx)ignite()).context().query()
            .querySqlFields(qry, false)) {

            if (res.getAll().size() != expRowsCnt)
                throw new Exception("Invalid result size");
        }

        return true;
    }
}
