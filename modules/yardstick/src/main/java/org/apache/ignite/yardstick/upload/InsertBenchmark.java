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

package org.apache.ignite.yardstick.upload;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Measures total time of upload data using sequence of single inserts;
 * Supports streaming.
 */
public class InsertBenchmark extends AbstractUploadBenchmark {
    /** {@inheritDoc} */
    @Override public void warmup(Connection warmupConn) throws SQLException {
        try (PreparedStatement insert = warmupConn.prepareStatement(queries.insert())) {
            for (long id = 1; id <= warmupRowsCnt; id++) {
                queries.setRandomInsertArgs(insert, id);

                insert.executeUpdate();
            }
        }
    }

    /**
     * Sequence of single inserts.
     */
    @Override public void upload (Connection uploadConn) throws Exception {
        try (PreparedStatement insert = uploadConn.prepareStatement(queries.insert())) {
            for (long id = 1; id <= insertRowsCnt; id++) {
                queries.setRandomInsertArgs(insert, id);

                insert.executeUpdate();
            }
        }
    }
}
