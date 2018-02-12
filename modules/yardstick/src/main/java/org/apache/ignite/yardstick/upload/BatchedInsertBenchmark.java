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

    /** {@inheritDoc} */
    @Override public void warmup() throws SQLException {
        try (PreparedStatement insert = conn.get().prepareStatement(queries.insert())) {
            for (int id = 1; id <= WARMUP_ROWS_CNT ; id++) {
                queries.setRandomInsertArgs(insert, id);

                insert.addBatch();

                if (id % BATCH_SIZE == 0 || id == WARMUP_ROWS_CNT)
                    insert.executeBatch();

            }
        }

        try(PreparedStatement delete = conn.get().prepareStatement(queries.deleteAll())){
            // todo: Should we perform subsequent insert+delete in warmup?
            delete.executeUpdate();
        }
    }


    /** Sequence of single inserts */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        try (PreparedStatement insert = conn.get().prepareStatement(queries.insert())) {
            for (int id = 1; id <= INSERT_SIZE; id++) {
                queries.setRandomInsertArgs(insert, id);

                insert.addBatch();

                if (id % BATCH_SIZE == 0 || id == INSERT_SIZE)
                    insert.executeBatch();
            }
        }

        return true;
    }

}
