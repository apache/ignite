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

import java.sql.PreparedStatement;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * JDBC benchmark that performs insert operations.
 */
public class JdbcSqlInsertDeleteBenchmark extends AbstractJdbcBenchmark {
    /** Statement that inserts one row. */
    private final ThreadLocal<PreparedStatement> singleInsert = newStatement(
        "INSERT INTO test_long (id, val) VALUES (?, ?)");

    /** Statement that deletes one row. */
    private final ThreadLocal<PreparedStatement> singleDelete = newStatement(
        "DELETE FROM test_long WHERE id = ?");

    /**
     * Benchmarked action that inserts and immediately deletes single row.
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long insertKey = rnd.nextLong(args.range()) + 1 + args.range();
        long insertVal = insertKey + 1;

        PreparedStatement insert = singleInsert.get();

        insert.setLong(1, insertKey);
        insert.setLong(2, insertVal);

        PreparedStatement delete = singleDelete.get();

        delete.setLong(1, insertKey);

        try {
            insert.executeUpdate();
            delete.executeUpdate();
        }
        catch (Exception ignored){
            // collision occurred, ignoring
        }

        return true;
    }
}
