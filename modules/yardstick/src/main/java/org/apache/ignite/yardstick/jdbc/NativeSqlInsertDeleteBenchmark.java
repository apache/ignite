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
import org.apache.ignite.internal.processors.query.GridQueryProcessor;

/**
 * Native sql benchmark that performs insert operations.
 */
public class NativeSqlInsertDeleteBenchmark extends AbstractNativeBenchmark {
    /**
     * Benchmarked action that inserts and immediately deletes row.
     *
     * {@inheritDoc}
     */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long insertKey = ThreadLocalRandom.current().nextLong(args.range()) + 1 + args.range();
        long insertVal = insertKey + 1;

        SqlFieldsQuery insert = new SqlFieldsQuery("INSERT INTO test_long (id, val) VALUES (?, ?)");

        insert.setArgs(insertKey, insertVal);

        SqlFieldsQuery delete = new SqlFieldsQuery("DELETE FROM test_long WHERE id = ?");

        delete.setArgs(insertKey);

        GridQueryProcessor qryProc = ((IgniteEx)ignite()).context().query();

        try (FieldsQueryCursor<List<?>> insCur = qryProc.querySqlFields(insert, false);
             FieldsQueryCursor<List<?>> delCur = qryProc.querySqlFields(delete, false)) {
            // No-op, there is no result
        }
        catch (Exception ign) {
            // collision occurred, ignoring
        }

        return true;
    }
}
