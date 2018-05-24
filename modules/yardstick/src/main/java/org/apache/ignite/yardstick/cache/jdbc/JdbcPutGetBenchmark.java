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

package org.apache.ignite.yardstick.cache.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import static org.apache.ignite.yardstick.IgniteAbstractBenchmark.nextRandom;
import static org.apache.ignite.yardstick.cache.jdbc.JdbcPutBenchmark.createUpsertStatement;

/** JDBC benchmark that performs raw SQL inserts with subsequent selects of fresh records */
public class JdbcPutGetBenchmark extends JdbcAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int newKey = nextRandom(args.range());
        int newVal = nextRandom(args.range());

        try (PreparedStatement stmt = createUpsertStatement(conn.get(), newKey, newVal)) {
            if (stmt.executeUpdate() <= 0)
                return false;
        }

        try (PreparedStatement stmt = conn.get().prepareStatement("select id, val from SAMPLE where id = ?")) {
            stmt.setInt(1, newKey);

            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                rs.getInt(1);
                rs.getInt(2);

                return true;
            }
            else
                return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (!args.createTempDatabase())
            clearTable("SAMPLE");

        super.tearDown();
    }
}
