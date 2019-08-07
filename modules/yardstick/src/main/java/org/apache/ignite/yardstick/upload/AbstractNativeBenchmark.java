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

package org.apache.ignite.yardstick.upload;

import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.upload.model.QueryFactory;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Base class for benchmarks that perform upload using java api.
 */
public abstract class AbstractNativeBenchmark extends IgniteAbstractBenchmark {
    /** Number of entries to be uploaded during warmup. */
    private long insertRowsCnt;

    /** Name of the cache for test table. */
    protected static final String CACHE_NAME = "SQL_PUBLIC_" + QueryFactory.UPLOAD_TABLE_NAME;

    /** Facade for creating sql queries. */
    protected QueryFactory queries;

    /**
     * Sets up benchmark: performs warmup on one cache and creates another for {@link #test(Map)} method.
     *
     * @param cfg Benchmark configuration.
     * @throws Exception - on error.
     */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        queries = new QueryFactory(args.atomicMode());

        insertRowsCnt = args.upload.uploadRowsCnt();

        dropAndCreateTable();

        // Number of entries to be uploaded during test().
        long warmupRowsCnt = args.upload.warmupRowsCnt();

        // warmup.
        BenchmarkUtils.println(cfg, "Starting custom warmup. Uploading " + warmupRowsCnt + " rows.");

        upload(warmupRowsCnt);

        BenchmarkUtils.println(cfg, "Custom warmup finished.");

        dropAndCreateTable();
    }

    /**
     * Drops test table if exists and creates empty new one.
     */
    private void dropAndCreateTable() {
        executeNativeSql(QueryFactory.DROP_TABLE_IF_EXISTS);

        executeNativeSql(queries.createTable());

        for (int idx = 1; idx <= args.upload.indexesCount(); idx++)
            executeNativeSql(queries.createIndex(idx));
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        try {
            long size = ignite().cache(CACHE_NAME).sizeLong();
            //long size = (Long)executeNativeSql(QueryFactory.COUNT).get(0).get(0);

            if (size != insertRowsCnt) {
                String msg = "Incorrect cache size: [actual=" + size + ", expected=" + insertRowsCnt + "].";

                BenchmarkUtils.println(cfg, "TearDown: " + msg);

                throw new RuntimeException(msg);
            }
        }
        finally {
            super.tearDown();
        }
    }

    /**
     * Executes query using native sql.
     */
    private List<List<?>> executeNativeSql(String qry) {
        return ((IgniteEx)ignite()).context().query().querySqlFields(new SqlFieldsQuery(qry), false).getAll();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        upload(insertRowsCnt);

        return true;
    }

    /** Uploads {@param insertsCnt} to test cache/table using java api. */
    protected abstract void upload(long insertsCnt);
}
