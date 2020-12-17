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

package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import java.util.List;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

public class SqlCacheApplicationCheck extends IgniteAwareApplication {
    /**
     * {@inheritDoc}
     */
    @Override protected void run(JsonNode jsonNode) {
        String count = null;

        log.info("Open cache...");

        IgniteCache<Long, Account> cache = ignite.cache(jsonNode.get("cacheName").asText());

        SqlFieldsQuery sql = new SqlFieldsQuery(
                "select count(*) from Account");

        log.info("Check cache size");

        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = cache.query(sql)) {
            count = cursor.getAll().get(0).get(0).toString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("SELECT COUNT(*) FROM ACCOUNT return: " + count);

        assert count.equals(jsonNode.get("range").asText());

        sql = new SqlFieldsQuery(
                "explain SELECT * FROM Account WHERE postindex = 100");
        String explain = null;

        log.info("Check SQL Index");
        try (QueryCursor<List<?>> cursor = cache.query(sql)) {
            explain = cursor.getAll().get(0).get(0).toString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("SQL Execution Plan: " + explain);

        assert explain.contains("_IDX");

        log.info("Cache checked");

        markSyncExecutionComplete();
    }
}
