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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Measures total time of data upload using batched insert.
 * Supports streaming.
 */
public class BatchedInsertBenchmark extends AbstractUploadBenchmark {
    /** Number of inserts in batch */
    public int BATCH_SIZE;

    /** {@inheritDoc} */
    @Override protected void init() {
        BATCH_SIZE = args.sqlRange();
    }

    /** {@inheritDoc} */
    @Override protected void warmup(Connection warmupConn) throws SQLException {
        performBatchUpdate(warmupConn, WARMUP_ROWS_CNT);

        clearTable();
    }

    /** {@inheritDoc} */
    @Override public void upload(Connection uploadConn) throws Exception {
        performBatchUpdate(uploadConn, INSERT_SIZE);
    }

    /**
     * Actually performs batched inserts, using specified connection.
     *
     * @param uploadConn Special connection for upload purposes.
     * @param rowsCount Number of rows to insert.
     */
    private void performBatchUpdate(Connection uploadConn, long rowsCount) throws SQLException{
        try (PreparedStatement insert = uploadConn.prepareStatement(queries.insert())) {
            for (int id = 1; id <= rowsCount; id++) {
                queries.setRandomInsertArgs(insert, id);

                insert.addBatch();

                if (id % BATCH_SIZE == 0 || id == rowsCount)
                    insert.executeBatch();
            }
        }
    }
}
