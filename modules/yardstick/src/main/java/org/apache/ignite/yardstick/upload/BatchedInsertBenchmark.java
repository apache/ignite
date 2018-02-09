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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class BatchedInsertBenchmark extends AbstractUploadBenchmark {
    /** Rows count to be inserted and deleted during warmup */
    public static final int WARMUP_ROWS_CNT = 3000_000;

    /** Number of inserts in batch */
    public static final int BATCH_SIZE = 1000;

    /** {@inheritDoc} */
    @Override public void warmup() throws SQLException {
        try (PreparedStatement insert = conn.get()
                .prepareStatement("INSERT INTO test_long VALUES (?, ?)")) {
            for (int i = 1; i <= WARMUP_ROWS_CNT ; i++) {
                insert.setLong(1, i);
                insert.setLong(2, i + 1);

                insert.addBatch();

                if (i % BATCH_SIZE == 0 || i == WARMUP_ROWS_CNT)
                    insert.executeBatch();

            }
        }

        try(PreparedStatement delete = conn.get().prepareStatement("DELETE FROM test_long WHERE id > 0")){
            // todo: Should we perform subsequent insert+delete in warmup?
            delete.executeUpdate();
        }
    }


    /** Sequence of single inserts */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int n = args.sqlRange();

        try (PreparedStatement insert = conn.get()
                .prepareStatement("INSERT INTO test_long VALUES (?, ?)")) {
            for (int i = 1; i <= n; i++) {
                insert.setLong(1, i);
                insert.setLong(2, i + 1);

                insert.addBatch();

                if (i % BATCH_SIZE == 0 || i == n)
                    insert.executeBatch();
            }
        }

        return true;
    }

}
