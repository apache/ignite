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

import java.sql.PreparedStatement;
import java.util.Map;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * JDBC benchmark that performs update operations.
 */
public class JdbcSqlUpdateBenchmark extends AbstractJdbcBenchmark {
    /** Statement that updates one row. */
    private final ThreadLocal<PreparedStatement> singleUpdate = newStatement(
        "UPDATE test_long SET val = (val + 1) WHERE id = ?");

    /** Statement that updates range of rows. */
    private final ThreadLocal<PreparedStatement> rangeUpdate = newStatement(
        "UPDATE test_long SET val = (val + 1) WHERE id BETWEEN ? AND ?");

    /**Generates disjoint (among threads) id ranges */
    private DisjointRangeGenerator idGen;

    /** Setup method */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
        idGen = new DisjointRangeGenerator(cfg.threads(), args.range(), args.sqlRange());
    }

    /**
     * Benchmarked action that performs updates.
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {

        long expResSize = idGen.rangeWidth();

        long startId = idGen.nextRangeStartId();
        long endId = idGen.endRangeId(startId);

        PreparedStatement update;

        if (idGen.rangeWidth() == 1) {
            update = singleUpdate.get();

            // startId == endId
            update.setLong(1, startId);
        }
        else {
            update = rangeUpdate.get();

            update.setLong(1, startId);
            update.setLong(2, endId);
        }

        int actualResSize = update.executeUpdate();

        if (actualResSize != expResSize) {
            throw new Exception("Invalid result set size [actual=" + actualResSize
                + ", expected=" + expResSize + ']');
        }

        return true;
    }
}
