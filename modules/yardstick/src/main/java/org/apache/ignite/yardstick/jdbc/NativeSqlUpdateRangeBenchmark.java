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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Native sql benchmark that performs update operations.
 */
public class NativeSqlUpdateRangeBenchmark extends AbstractNativeBenchmark {
    /** Generates disjoint (among threads) id ranges */
    private DisjointRangeGenerator idGen;

    /** Setup method */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
        idGen = new DisjointRangeGenerator(cfg.threads(), args.range(), args.sqlRange());
    }

    /**
     * Benchmarked action that performs updates (single row or batch).
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long startId = idGen.nextRangeStartId();
        long endId = idGen.endRangeId(startId);

        long expRsSize = idGen.rangeWidth();

        SqlFieldsQuery qry;

        if (idGen.rangeWidth() == 1) {
            qry = new SqlFieldsQuery("UPDATE test_long SET val = (val + 1) WHERE id = ?");

            // startId == endId
            qry.setArgs(startId);
        }
        else {
            qry = new SqlFieldsQuery("UPDATE test_long SET val = (val + 1) WHERE id BETWEEN ? AND ?");

            qry.setArgs(startId, endId);
        }

        try (FieldsQueryCursor<List<?>> cursor = ((IgniteEx)ignite()).context().query()
                .querySqlFields(qry, false)) {
            Iterator<List<?>> it = cursor.iterator();

            List<?> cntRow = it.next();

            long rsSize = (Long)cntRow.get(0);

            if (it.hasNext())
                throw new Exception("Only one row expected on UPDATE query");

            if (rsSize != expRsSize)
                throw new Exception("Invalid result set size [actual=" + rsSize + ", expected=" + expRsSize + ']');
        }

        return true;
    }
}
