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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.ignite.internal.jdbc2.JdbcStreamingSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Tests for streaming via thin driver.
 */
public class JdbcThinStreamingSelfTest extends JdbcStreamingSelfTest {
    /** {@inheritDoc} */
    @Override protected Connection createStreamedConnection(boolean allowOverwrite, long flushFreq) throws Exception {
        Connection res = JdbcThinAbstractSelfTest.connect(grid(0), "streaming=true&streamingFlushFrequency="
            + flushFreq + "&" + "streamingAllowOverwrite=" + allowOverwrite);

        res.setSchema('"' + DEFAULT_CACHE_NAME + '"');

        return res;
    }

    /** {@inheritDoc} */
    @Override protected Connection createOrdinaryConnection() throws SQLException {
        return JdbcThinAbstractSelfTest.connect(grid(0), null);
    }

    /**
     * @throws Exception if failed.
     */
    public void testStreamedBatchedInsert() throws Exception {
        for (int i = 10; i <= 100; i += 10)
            ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i * 100);

        try (Connection conn = createStreamedConnection(false)) {
            try (PreparedStatement stmt = conn.prepareStatement("insert into Integer(_key, _val) values (?, ?), " +
                "(?, ?)")) {
                for (int i = 1; i <= 100; i+=2) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, i);
                    stmt.setInt(3, i + 1);
                    stmt.setInt(4, i + 1);

                    stmt.addBatch();
                }

                stmt.executeBatch();
            }
        }

        U.sleep(500);

        // Now let's check it's all there.
        for (int i = 1; i <= 100; i++) {
            if (i % 10 != 0)
                assertEquals(i, grid(0).cache(DEFAULT_CACHE_NAME).get(i));
            else // All that divides by 10 evenly should point to numbers 100 times greater - see above
                assertEquals(i * 100, grid(0).cache(DEFAULT_CACHE_NAME).get(i));
        }
    }
}