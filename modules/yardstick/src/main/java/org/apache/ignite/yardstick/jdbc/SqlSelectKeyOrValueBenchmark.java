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
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

import static org.apache.ignite.yardstick.jdbc.JdbcUtils.fillData;

/**
 * Abstract class for benchmarks that use {@link SqlFieldsQuery}.
 */
public class SqlSelectKeyOrValueBenchmark extends IgniteAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        fillData(cfg, (IgniteEx)ignite(), args.range(), args.atomicMode(),
            "val0 VARCHAR",
            "val1 VARCHAR",
            "val2 VARCHAR",
            "val3 VARCHAR",
            "val4 VARCHAR",
            "val5 VARCHAR",
            "val6 VARCHAR",
            "val7 VARCHAR",
            "val8 VARCHAR",
            "val9 VARCHAR");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        long expRsSize;

        SqlFieldsQuery qry;

        if (args.sqlRange() == 1) {
            qry = new SqlFieldsQuery("SELECT id FROM test_long WHERE id = ?");

            long id = ThreadLocalRandom.current().nextLong(args.range()) + 1;

            qry.setArgs(id);

            expRsSize = 1;
        }
        else if (args.sqlRange() > 0) {
            qry = new SqlFieldsQuery("SELECT id FROM test_long WHERE id BETWEEN ? AND ?");

            long id = ThreadLocalRandom.current().nextLong(args.range() - args.sqlRange()) + 1;
            long maxId = id + args.sqlRange() - 1;

            qry.setArgs(id, maxId);

            expRsSize = args.sqlRange();
        }
        else {
            qry = new SqlFieldsQuery("SELECT id FROM test_long").setLazy(true);

            expRsSize = args.range();
        }

        long rsSize = 0;

        try (FieldsQueryCursor<List<?>> cursor = ((IgniteEx)ignite()).context().query()
            .querySqlFields(qry, false)) {

            for (List<?> row : cursor)
                rsSize++;
        }

        if (rsSize != expRsSize)
            throw new Exception("Invalid result set size [actual=" + rsSize + ", expected=" + expRsSize + ']');

        return true;
    }
}
