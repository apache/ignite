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
import java.util.concurrent.ThreadLocalRandom;

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

    /**
     * Benchmarked action that performs updates.
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        PreparedStatement update;

        int expectedResSize;

        if (args.sqlRange() == 1){
            long updateKey = rnd.nextLong(args.range()) + 1;

            update = singleUpdate.get();
            update.setLong(1, updateKey);

            expectedResSize = 1;
        }
        else {
            long blocksCnt = args.range() / args.sqlRange();
            long id = rnd.nextLong(blocksCnt) * args.sqlRange() + 1;

            long maxId = id + args.sqlRange() - 1;

            update = rangeUpdate.get();
            update.setLong(1, id);
            update.setLong(2, maxId);

            expectedResSize = args.sqlRange();
        }

        int actualResSize = update.executeUpdate();

        if (actualResSize != expectedResSize) {
            throw new Exception("Invalid result set size [actual=" + actualResSize
                + ", expected=" + expectedResSize + ']');
        }

        return true;
    }
}
