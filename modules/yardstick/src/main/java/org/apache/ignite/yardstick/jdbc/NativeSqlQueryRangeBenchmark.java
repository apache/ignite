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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Native sql benchmark that performs select operations.
 */
public class NativeSqlQueryRangeBenchmark extends AbstractNativeBenchmark {
    /** Prepares SELECT query and it's parameters. */
    private SelectCommand select;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        select = args.selectCommand();
    }

    /**
     * Benchmarked action that performs selects and validates results.
     *
     * {@inheritDoc}
     */

    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long expRsSize;

        SqlFieldsQuery qry;

        if (args.sqlRange() == 1) {
            qry = new SqlFieldsQuery(select.selectOne());

            long id = ThreadLocalRandom.current().nextLong(args.range()) + 1;

            qry.setArgs(select.fieldByPK(id));

            expRsSize = 1;
        }
        else {
            qry = new SqlFieldsQuery(select.selectRange());

            long id = ThreadLocalRandom.current().nextLong(args.range() - args.sqlRange()) + 1;
            long maxId = id + args.sqlRange() - 1;

            qry.setArgs(select.fieldByPK(id), select.fieldByPK(maxId));

            expRsSize = args.sqlRange();
        }

        long rsSize = 0;

        try (FieldsQueryCursor<List<?>> cursor = ((IgniteEx)ignite()).context().query()
            .querySqlFields(qry, false)) {

            for (List<?> row : cursor) {
                if ((Long)row.get(0) + 1 != (Long)row.get(1))
                    throw new Exception("Invalid result retrieved");

                rsSize++;
            }
        }

        if (rsSize != expRsSize)
            throw new Exception("Invalid result set size [actual=" + rsSize + ", expected=" + expRsSize + ']');

        return true;
    }
}
