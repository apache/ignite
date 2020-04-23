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
    private long batchSize;

    /** {@inheritDoc} */
    @Override protected void init() {
        batchSize = args.upload.jdbcBatchSize();
    }

    /** {@inheritDoc} */
    @Override protected void warmup(Connection warmupConn) throws SQLException {
        performBatchUpdate(warmupConn, warmupRowsCnt);
    }

    /** {@inheritDoc} */
    @Override public void upload(Connection uploadConn) throws Exception {
        performBatchUpdate(uploadConn, insertRowsCnt);
    }

    /**
     * Actually performs batched inserts, using specified connection.
     *
     * @param uploadConn Special connection for upload purposes.
     * @param rowsCnt Number of rows to insert.
     */
    private void performBatchUpdate(Connection uploadConn, long rowsCnt) throws SQLException {
        try (PreparedStatement insert = uploadConn.prepareStatement(queries.insert())) {
            for (long id = 1; id <= rowsCnt; id++) {
                queries.setRandomInsertArgs(insert, id);

                insert.addBatch();

                if (id % batchSize == 0 || id == rowsCnt)
                    insert.executeBatch();
            }
        }
    }
}
