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

package org.apache.ignite.internal.ducktest.tests.mex;

import java.sql.Connection;
import java.sql.ResultSet;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/** */
public class MexCntApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) throws Exception {
        final String tableName = jsonNode.get("tableName").asText();
        markInitialized();

        recordResult("tableRowsCnt", printCount(tableName));

        markFinished();
    }

    /** */
    protected int printCount(String tableName) throws Exception {
        try (Connection conn = thinJdbcDataSource.getConnection()) {
            try {
                ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(1) FROM " + tableName);

                rs.next();

                int cnt = rs.getInt(1);

                log.info("TEST | Table '" + tableName + "' contains " + cnt + " records.");

                return cnt;
            }
            catch (Exception t) {
                log.error("Failed to get records count from table '" + tableName + "'. Error: " + t.getMessage(), t);

                markBroken(t);

                throw t;
            }
        }
    }
}
